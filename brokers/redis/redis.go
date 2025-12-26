package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const pollPeriod = time.Second

type Broker struct {
	rdb *redis.Client
}

func New() *Broker {
	return &Broker{
		rdb: redis.NewClient(&redis.Options{}),
	}
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := b.rdb.BLPop(ctx, pollPeriod, queue).Result()
			if err != nil {
				continue
			}
			msg, err := blpopResult(res)
			if err != nil {
				continue
			}
			work <- []byte(msg)
		}
	}
}

func (b *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return b.rdb.LPush(ctx, queue, msg).Err()
}

func blpopResult(rs []string) (string, error) {
	if len(rs) != 2 {
		return "", fmt.Errorf("BLPop result should have exactly 2 strings. Got : %v", rs)
	}

	return rs[1], nil
}
