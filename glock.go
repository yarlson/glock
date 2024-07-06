package globallock

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	defaultLockTimeout       = 5 * time.Minute
	defaultHeartbeatInterval = 1 * time.Minute
)

// GlobalLock represents a global locking mechanism
type GlobalLock struct {
	db                *sql.DB
	lockID            int64
	instanceID        string
	lockTimeout       time.Duration
	heartbeatInterval time.Duration
}

// NewGlobalLock creates a new GlobalLock instance
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

// WithLockTimeout sets the lock timeout
func WithLockTimeout(timeout time.Duration) func(*GlobalLock) {
	return func(gl *GlobalLock) {
		gl.lockTimeout = timeout
	}
}

// WithHeartbeatInterval sets the heartbeat interval
func WithHeartbeatInterval(interval time.Duration) func(*GlobalLock) {
	return func(gl *GlobalLock) {
		gl.heartbeatInterval = interval
	}
}

// Run executes the given function with a global lock
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
		// Check if the existing lock is stale
		var lastHeartbeat time.Time
		err := tx.QueryRow("SELECT last_heartbeat FROM locks WHERE id = $1", gl.lockID).Scan(&lastHeartbeat)
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
				return fmt.Errorf("failed to acquire stale lock: %v", err)
			}
		} else {
			return nil // Another process holds the lock, we'll try again later
		}
	}

	// Start a goroutine for heartbeat
	var wg sync.WaitGroup
	wg.Add(1)
	heartbeatStop := make(chan struct{})
	go func() {
		defer wg.Done()
		gl.heartbeat(heartbeatStop)
	}()
	defer func() {
		close(heartbeatStop)
		wg.Wait()
	}()

	// Execute the provided function
	err = f(tx)
	if err != nil {
		return err
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

// EnsureTable ensures that the locks table exists in the database
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
