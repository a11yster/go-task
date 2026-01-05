package gotask

import "context"

type Broker interface {
	// Enqueue pushes a serialized job message onto the queue
	Enqueue(ctx context.Context, msg []byte, queue string) error

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
