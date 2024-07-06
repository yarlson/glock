package main

import (
	"database/sql"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/yarlson/glock"
)

func main() {
	appName := os.Getenv("APP_NAME")
	if appName == "" {
		log.Fatal("APP_NAME environment variable is not set")
	}

	db, err := sql.Open("postgres", "postgres://user:password@localhost:5432/testdb?sslmode=disable")
	if err != nil {
		log.Fatalf("%s: Failed to connect to database: %v", appName, err)
	}
	defer db.Close()

	gl := glock.NewGlobalLock(db, 12345, appName,
		glock.WithLockTimeout(1*time.Minute),
		glock.WithHeartbeatInterval(10*time.Second),
	)

	if err := gl.EnsureTable(); err != nil {
		log.Printf("%s: Failed to ensure locks table: %v", appName, err)
	}

	for {
		log.Printf("%s: Attempting to acquire lock", appName)
		err := gl.Run(func(tx *sql.Tx) error {
			log.Printf("%s: Lock acquired", appName)
			sleepTime := time.Duration(rand.Intn(30)+1) * time.Second
			log.Printf("%s: Sleeping for %v", appName, sleepTime)
			time.Sleep(sleepTime)
			log.Printf("%s: Finished critical section", appName)
			return nil
		})

		if err != nil {
			log.Printf("%s: Error during locked operation: %v", appName, err)
		}

		sleepTime := time.Duration(rand.Intn(5)+1) * time.Second

		time.Sleep(sleepTime) // Wait a bit before trying again
	}
}
