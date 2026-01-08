package gotask

import (
	"context"
	"time"
)

type Broker interface {
	// Enqueue pushes a serialized job message onto the queue
	Enqueue(ctx context.Context, msg []byte, queue string) error

	// EnqueueScheduled accepts a task (msg, queue) and also a timestamp
	// The job should be enqueued at the particular timestamp.
	EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error

	// Consume listens on the queue and sends received messages to the work channel.
	Consume(ctx context.Context, work chan []byte, queue string)
}

// Results is the interface for job state persistence.
type Results interface {
	Get(ctx context.Context, id string) ([]byte, error)
	Set(ctx context.Context, id string, b []byte) error
	DeleteJob(ctx context.Context, id string) error
	GetFailed(ctx context.Context) ([]string, error)
	GetSuccess(ctx context.Context) ([]string, error)
	SetFailed(ctx context.Context, id string) error
	SetSuccess(ctx context.Context, id string) error
}
