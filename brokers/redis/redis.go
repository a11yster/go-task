package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pollPeriod   = time.Second
	sortedSetKey = "gotask:ss:%s"
)

type Broker struct {
	rdb *redis.Client
}

func New() *Broker {
	return &Broker{
		rdb: redis.NewClient(&redis.Options{}),
	}
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {

	go b.consumeScheduled(ctx, queue)
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

func (b *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	key := fmt.Sprintf(sortedSetKey, queue)
	return b.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(ts.UnixNano()),
		Member: msg,
	}).Err()
}

func (b *Broker) consumeScheduled(ctx context.Context, queue string) {
	ticker := time.NewTicker(pollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.pumpScheduled(ctx, queue)
		}
	}
}

func (b *Broker) pumpScheduled(ctx context.Context, queue string) {
	key := fmt.Sprintf(sortedSetKey, queue)
	now := time.Now().UnixNano()

	err := b.rdb.Watch(ctx, func(tx *redis.Tx) error {
		tasks, err := tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(now, 10),
			Offset: 0,
			Count:  1,
		}).Result()

		if err != nil {
			return err
		}

		if len(tasks) == 0 {
			return nil
		}

		for _, task := range tasks {
			if err := b.rdb.LPush(ctx, queue, []byte(task)).Err(); err != nil {
				return err
			}
			if err := tx.ZRem(ctx, key, task).Err(); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Printf("error pumping scheduled jobs: %v\n", err)
	}
}

func blpopResult(rs []string) (string, error) {
	if len(rs) != 2 {
		return "", fmt.Errorf("BLPop result should have exactly 2 strings. Got : %v", rs)
	}

	return rs[1], nil
}
