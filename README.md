# go-task

A minimal task queue system for Go using Redis.

## Installation

```bash
go get github.com/a11yster/go-task
```

## Usage

```go
import gotask "github.com/a11yster/go-task"

// Create a server
srv := gotask.NewServer()

// Register a processor
srv.RegisterProcessor("handler-name", func(payload []byte) error {
    // Process the task
    return nil
})

// Start the server
ctx := context.Background()
srv.Start(ctx)
```

## Requirements

- Go 1.25.4+
- Redis server

See `example/` for a complete example.
