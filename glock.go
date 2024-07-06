/*
Package glock provides a mechanism for distributed locking using a PostgreSQL database.
It allows multiple processes or servers to coordinate access to shared resources by acquiring
a global lock before executing critical sections of code.

The package uses PostgreSQL advisory locks for fast, in-memory locking, combined with a
database table for tracking lock ownership and implementing a heartbeat mechanism to handle
scenarios where a lock holder crashes or becomes unresponsive.

Usage:

To use the glock package, first create a GlobalLock instance with NewGlobalLock,
then call the Run method to execute code within the protected section.

Example:

	db, err := sql.Open("postgres", "postgres://user:password@localhost/dbname")
	if err != nil {
	    log.Fatal(err)
	}
	defer db.Close()

	gl := glock.NewGlobalLock(db, 12345, "instance-1",
	    glock.WithLockTimeout(10*time.Minute),
	    glock.WithHeartbeatInterval(30*time.Second),
	)

	if err := gl.EnsureTable(); err != nil {
	    log.Fatal(err)
	}

	err = gl.Run(func(tx *sql.Tx) error {
	    // Your protected code here
	    return nil
	})
	if err != nil {
	    log.Printf("Error: %v", err)
	}

The package is designed to be resilient to process crashes and network issues, automatically
releasing stale locks and providing a heartbeat mechanism to keep locks alive during long-running operations.
*/
package glock

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

const (
	defaultLockTimeout       = 5 * time.Minute
	defaultHeartbeatInterval = 1 * time.Minute
)

// GlobalLock represents a global locking mechanism using a PostgreSQL database.
type GlobalLock struct {
	db                *sql.DB
	lockID            int64
	instanceID        string
	lockTimeout       time.Duration
	heartbeatInterval time.Duration
}

// NewGlobalLock creates a new GlobalLock instance.
//
// Parameters:
//   - db: A *sql.DB connection to the PostgreSQL database.
//   - lockID: A unique identifier for this particular lock.
//   - instanceID: A unique identifier for the current process or instance.
//   - options: Optional functions to configure the GlobalLock instance.
//
// Example:
//
//	gl := glock.NewGlobalLock(db, 12345, "server-1",
//	    glock.WithLockTimeout(5*time.Minute),
//	    glock.WithHeartbeatInterval(30*time.Second),
//	)
func NewGlobalLock(db *sql.DB, lockID int64, instanceID string, options ...func(*GlobalLock)) *GlobalLock {
	gl := &GlobalLock{
		db:                db,
		lockID:            lockID,
		instanceID:        instanceID,
		lockTimeout:       defaultLockTimeout,
		heartbeatInterval: defaultHeartbeatInterval,
	}

	for _, option := range options {
		option(gl)
	}

	return gl
}

// WithLockTimeout returns an option function that sets the lock timeout for a GlobalLock instance.
//
// The lock timeout determines how long a lock is considered valid before it can be taken over
// by another instance if the heartbeat hasn't been updated.
//
// Example:
//
//	gl := glock.NewGlobalLock(db, 12345, "server-1",
//	    glock.WithLockTimeout(10*time.Minute),
//	)
func WithLockTimeout(timeout time.Duration) func(*GlobalLock) {
	return func(gl *GlobalLock) {
		gl.lockTimeout = timeout
	}
}

// WithHeartbeatInterval returns an option function that sets the heartbeat interval for a GlobalLock instance.
//
// The heartbeat interval determines how often the lock holder updates its heartbeat
// to indicate it's still alive and holding the lock.
//
// Example:
//
//	gl := glock.NewGlobalLock(db, 12345, "server-1",
//	    glock.WithHeartbeatInterval(30*time.Second),
//	)
func WithHeartbeatInterval(interval time.Duration) func(*GlobalLock) {
	return func(gl *GlobalLock) {
		gl.heartbeatInterval = interval
	}
}

// Run executes the given function with a global lock.
//
// This method will attempt to acquire the lock, and if successful, will execute the provided function
// within a database transaction. It also starts a heartbeat goroutine to keep the lock alive
// during execution.
//
// If the lock cannot be acquired immediately, the method returns nil without executing the function.
//
// Parameters:
//   - f: A function that takes a *sql.Tx and returns an error. This function is executed
//     while holding the global lock.
//
// Returns:
//   - error: An error if one occurred during lock acquisition, execution of the provided function,
//     or committing the transaction.
//
// Example:
//
//	err := gl.Run(func(tx *sql.Tx) error {
//	    // Perform some operation that requires global synchronization
//	    _, err := tx.Exec("UPDATE some_table SET status = 'processing' WHERE id = $1", someID)
//	    if err != nil {
//	        return err
//	    }
//	    // Perform the actual processing...
//	    return nil
//	})
//	if err != nil {
//	    log.Printf("Error during locked operation: %v", err)
//	}
func (gl *GlobalLock) Run(f func(*sql.Tx) error) error {
	tx, err := gl.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Try to acquire the advisory lock
	var locked bool
	err = tx.QueryRow("SELECT pg_try_advisory_xact_lock($1)", gl.lockID).Scan(&locked)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %v", err)
	}
	if !locked {
		return nil // Another process holds the lock, we'll try again later
	}

	// Check if there's a stale lock in the locks table
	var lastHeartbeat time.Time
	err = tx.QueryRow("SELECT last_heartbeat FROM locks WHERE id = $1 FOR UPDATE", gl.lockID).Scan(&lastHeartbeat)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check last heartbeat: %v", err)
	}

	if err == sql.ErrNoRows || time.Since(lastHeartbeat) > gl.lockTimeout {
		// The lock is stale or doesn't exist, we can acquire it
		_, err := tx.Exec(`
            INSERT INTO locks (id, locked_by, locked_at, last_heartbeat)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE
            SET locked_by = $2, locked_at = NOW(), last_heartbeat = NOW()
        `, gl.lockID, gl.instanceID)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %v", err)
		}
	} else {
		return nil // The lock is still valid, we'll try again later
	}

	// Start a goroutine for heartbeat
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(gl.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, err := gl.db.Exec("UPDATE locks SET last_heartbeat = NOW() WHERE id = $1 AND locked_by = $2", gl.lockID, gl.instanceID)
				if err != nil {
					log.Printf("Failed to update heartbeat: %v", err)
				}
			case <-done:
				return
			}
		}
	}()

	// Execute the provided function
	err = f(tx)

	// Stop the heartbeat goroutine
	close(done)

	if err != nil {
		return err
	}

	// Release the lock
	_, err = tx.Exec("DELETE FROM locks WHERE id = $1 AND locked_by = $2", gl.lockID, gl.instanceID)
	if err != nil {
		return fmt.Errorf("failed to release lock: %v", err)
	}

	// Commit the transaction
	return tx.Commit()
}

func (gl *GlobalLock) heartbeat(stop <-chan struct{}) {
	ticker := time.NewTicker(gl.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := gl.db.Exec("UPDATE locks SET last_heartbeat = NOW() WHERE id = $1", gl.lockID)
			if err != nil {
				log.Printf("Failed to update heartbeat: %v", err)
			}
		case <-stop:
			return
		}
	}
}

// EnsureTable ensures that the locks table exists in the database.
//
// This method should be called once when setting up the application to make sure
// the required database table exists.
//
// Returns:
//   - error: An error if the table couldn't be created.
//
// Example:
//
//	gl := glock.NewGlobalLock(db, 12345, "server-1")
//	if err := gl.EnsureTable(); err != nil {
//	    log.Fatalf("Failed to create locks table: %v", err)
//	}
func (gl *GlobalLock) EnsureTable() error {
	_, err := gl.db.Exec(`
        CREATE TABLE IF NOT EXISTS locks (
            id BIGINT PRIMARY KEY,
            locked_by VARCHAR(255),
            locked_at TIMESTAMP,
            last_heartbeat TIMESTAMP
        )
    `)
	return err
}
