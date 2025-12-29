# go-task

A minimal task queue system for Go using Redis.

## Features

- Simple and lightweight task queue implementation
- Redis-backed message broker
- Configurable concurrency
- Extensible broker interface
- Context-aware task processing

## Installation

```bash
go get github.com/a11yster/go-task
```

## Quick Start

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    gotask "github.com/a11yster/go-task"
    "github.com/a11yster/go-task/brokers/redis"
)

func main() {
    ctx := context.Background()

    // Create a Redis broker
    broker := redis.New()

    // Create a server with options
    srv, err := gotask.NewServer(gotask.ServerOpts{
        Broker:      broker,
        Concurrency: 5,  // Number of concurrent workers
        Queue:       "my-queue",  // Queue name (optional, defaults to "gotask:tasks")
    })
    if err != nil {
        log.Fatal(err)
    }

    // Register a processor
    srv.RegisterProcessor("my-handler", func(payload []byte) error {
        // Process the task
        var data map[string]interface{}
        json.Unmarshal(payload, &data)
        // ... your processing logic
        return nil
    })

    // Start the server (blocking)
    srv.Start(ctx)
}
```

## Adding Tasks

```go
// Create a task
payload, _ := json.Marshal(map[string]string{"message": "hello"})
task := gotask.NewTask("my-handler", payload)

// Optionally set a custom queue
task.Queue = "custom-queue"

// Add the task to the queue
err := srv.AddTask(ctx, task)
if err != nil {
    log.Fatal(err)
}
```

## Server Options

- `Broker`: The message broker implementation (required)
- `Concurrency`: Number of concurrent workers (defaults to `runtime.GOMAXPROCS(0)`)
- `Queue`: Queue name (defaults to `"gotask:tasks"`)

## Broker Interface

The library uses a `Broker` interface, allowing you to implement custom brokers:

```go
type Broker interface {
    Enqueue(ctx context.Context, msg []byte, queue string) error
    Consume(ctx context.Context, work chan []byte, queue string)
}
```

A Redis broker implementation is provided in `brokers/redis`.

## Requirements

- Go 1.25.4+
- Redis server

## Example

See `example/` directory for a complete working example.

## License

MIT
