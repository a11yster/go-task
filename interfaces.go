package gotask

import "context"

type Broker interface {
	Enqueue(ctx context.Context, msg []byte, queue string) error
	Consume(ctx context.Context, work chan []byte, queue string)
}
