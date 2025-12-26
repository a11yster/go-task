# Example Usage

This example demonstrates how to use the go-task task queue system.

## Prerequisites

- Go 1.25.4 or later
- Redis server running (default: localhost:6379)

## Running the Example

1. Make sure Redis is running:

   ```bash
   redis-server
   ```

2. Run the example:
   ```bash
   go run main.go
   ```

## What it does

- Registers a processor named "add" that sums two integers
- Enqueues 1000 tasks, each with different arguments
- Processes tasks concurrently as they are consumed from Redis
- Gracefully shuts down on interrupt signal (Ctrl+C)
