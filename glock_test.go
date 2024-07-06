package glock

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GlobalLockTestSuite struct {
	suite.Suite
	db        *sql.DB
	container testcontainers.Container
}

func TestGlobalLockSuite(t *testing.T) {
	suite.Run(t, new(GlobalLockTestSuite))
}

func (s *GlobalLockTestSuite) SetupSuite() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoError(err, "Failed to start container")

	s.container = container

	host, err := container.Host(ctx)
	s.Require().NoError(err, "Failed to get container host")

	port, err := container.MappedPort(ctx, "5432")
	s.Require().NoError(err, "Failed to get container port")

	connStr := fmt.Sprintf("host=%s port=%d user=testuser password=testpass dbname=testdb sslmode=disable", host, port.Int())

	// Try to connect with retries
	var db *sql.DB
	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		s.T().Logf("Failed to connect to database (attempt %d): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	s.Require().NoError(err, "Failed to connect to database after 5 attempts")
	s.db = db

	s.T().Log("Successfully connected to the database")
}

func (s *GlobalLockTestSuite) TearDownSuite() {
	if s.db != nil {
		s.Require().NoError(s.db.Close(), "Failed to close database connection")
	}
	if s.container != nil {
		s.Require().NoError(s.container.Terminate(context.Background()), "Failed to terminate container")
	}
}

func (s *GlobalLockTestSuite) SetupTest() {
	// Ensure the locks table exists and is empty before each test
	gl := NewGlobalLock(s.db, 1, "setup")
	err := gl.EnsureTable()
	s.Require().NoError(err, "Failed to ensure table exists")
	_, err = s.db.Exec("DELETE FROM locks")
	s.Require().NoError(err, "Failed to clear locks table")
}

func (s *GlobalLockTestSuite) TestNewGlobalLock() {
	gl := NewGlobalLock(s.db, 1, "instance1",
		WithLockTimeout(10*time.Second),
		WithHeartbeatInterval(2*time.Second),
	)

	s.Equal(s.db, gl.db)
	s.Equal(int64(1), gl.lockID)
	s.Equal("instance1", gl.instanceID)
	s.Equal(10*time.Second, gl.lockTimeout)
	s.Equal(2*time.Second, gl.heartbeatInterval)
}

func (s *GlobalLockTestSuite) TestEnsureTable() {
	gl := NewGlobalLock(s.db, 1, "instance1")

	err := gl.EnsureTable()
	s.Require().NoError(err)

	// Check if the table exists
	var exists bool
	err = s.db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'locks')").Scan(&exists)
	s.Require().NoError(err)
	s.True(exists)
}

func (s *GlobalLockTestSuite) TestRunWithLock() {
	gl := NewGlobalLock(s.db, 1, "instance1")

	executed := false
	err := gl.Run(func(tx *sql.Tx) error {
		executed = true
		return nil
	})

	s.Require().NoError(err)
	s.True(executed)

	// Check if the lock was released
	var count int
	err = s.db.QueryRow("SELECT COUNT(*) FROM locks WHERE id = 1").Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count)
}

func (s *GlobalLockTestSuite) TestRunWithLockConcurrent() {
	gl1 := NewGlobalLock(s.db, 1, "instance1")
	gl2 := NewGlobalLock(s.db, 1, "instance2")

	executed1 := false
	executed2 := false

	done := make(chan bool, 2)

	go func() {
		err := gl1.Run(func(tx *sql.Tx) error {
			time.Sleep(2 * time.Second)
			executed1 = true
			return nil
		})
		s.Require().NoError(err)
		done <- true
	}()

	go func() {
		time.Sleep(1 * time.Second)
		err := gl2.Run(func(tx *sql.Tx) error {
			executed2 = true
			return nil
		})
		s.Require().NoError(err)
		done <- true
	}()

	<-done
	<-done

	s.True(executed1)
	s.False(executed2)
}

func (s *GlobalLockTestSuite) TestRunWithStaleLock() {
	gl := NewGlobalLock(s.db, 1, "instance1", WithLockTimeout(1*time.Second))

	// Insert a stale lock
	_, err := s.db.Exec("INSERT INTO locks (id, locked_by, locked_at, last_heartbeat) VALUES (1, 'stale', NOW() - INTERVAL '2 seconds', NOW() - INTERVAL '2 seconds')")
	s.Require().NoError(err)

	executed := false
	err = gl.Run(func(tx *sql.Tx) error {
		executed = true
		return nil
	})

	s.Require().NoError(err)
	s.True(executed)
}

func (s *GlobalLockTestSuite) TestHeartbeat() {
	gl := NewGlobalLock(s.db, 1, "instance1", WithHeartbeatInterval(1*time.Second))

	executed := false
	err := gl.Run(func(tx *sql.Tx) error {
		executed = true
		time.Sleep(3 * time.Second)
		return nil
	})

	s.Require().NoError(err)
	s.True(executed)

	var count int
	err = s.db.QueryRow("SELECT COUNT(*) FROM locks WHERE id = 1").Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count, "Lock should be released after Run completes")
}
