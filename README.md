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
    broker := redis.New()

    srv, err := gotask.NewServer(gotask.ServerOpts{
        Broker:      broker,
        Concurrency: 5,
        Queue:       "my-queue",
    })
    if err != nil {
        log.Fatal(err)
    }

    srv.RegisterProcessor("my-handler", func(payload []byte) error {
        var data map[string]interface{}
        json.Unmarshal(payload, &data)
        // ... your processing logic
        return nil
    })

    srv.Start(ctx)
}
```

## Adding Jobs

```go
payload, _ := json.Marshal(map[string]string{"message": "hello"})
job, _ := gotask.NewJob("my-handler", payload, gotask.JobOpts{
    Queue: "custom-queue",  // Optional
})

jobID, err := srv.Enqueue(ctx, job)
if err != nil {
    log.Fatal(err)
}
```

## Server Options

- `Broker`: The message broker implementation (required)
- `Concurrency`: Number of concurrent workers (defaults to `runtime.GOMAXPROCS(0)`)
- `Queue`: Queue name (defaults to `"gotask:tasks"`)

## Broker Interface

Implement custom brokers using the `Broker` interface:

```go
type Broker interface {
    Enqueue(ctx context.Context, msg []byte, queue string) error
    Consume(ctx context.Context, work chan []byte, queue string)
}
```

A Redis implementation is provided in `brokers/redis`.

## Requirements

- Go 1.25.4+
- Redis server

## Example

See `example/` directory for a complete working example.

## License

MIT
