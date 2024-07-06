# glock

[![codecov](https://codecov.io/github/yarlson/glock/graph/badge.svg?token=PqqkygGeks)](https://codecov.io/github/yarlson/glock)

glock is a Go package that provides a distributed locking mechanism using PostgreSQL. It's designed for coordinating access to shared resources across multiple processes or servers.

## Features

- Distributed locking using PostgreSQL advisory locks
- Heartbeat mechanism to handle scenarios where a lock holder becomes unresponsive
- Automatic release of stale locks
- Configurable lock timeout and heartbeat interval

## Installation

To install glock, use `go get`:

```
go get github.com/yarlson/glock
```

## Usage

Here's a basic example of how to use glock:

```go
package main

import (
    "database/sql"
    "log"
    "time"

    "github.com/yarlson/glock"
    _ "github.com/lib/pq"
)

func main() {
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
}
```

## Configuration

glock can be configured with the following options:

- `WithLockTimeout(duration)`: Sets the lock timeout
- `WithHeartbeatInterval(duration)`: Sets the heartbeat interval

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
provides a concise overview of the package, its features, installation instructions, a usage example, configuration options, and standard sections for contributing and licensing. It maintains a professional tone without using promotional language or unnecessary elaboration.