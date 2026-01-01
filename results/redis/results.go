package redis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	resultPrefix = "gotask:res:"
	successKey   = "success"
	failedKey    = "failed"
)

type Results struct {
	rdb *redis.Client
}

func New() *Results {
	return &Results{
		rdb: redis.NewClient(&redis.Options{}),
	}
}

func (r *Results) Get(ctx context.Context, id string) ([]byte, error) {
	rs, err := r.rdb.Get(ctx, resultPrefix+id).Bytes()
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (r *Results) Set(ctx context.Context, id string, b []byte) error {
	return r.rdb.Set(ctx, resultPrefix+id, b, 0).Err()
}

func (r *Results) DeleteJob(ctx context.Context, id string) error {
	pipe := r.rdb.Pipeline()

	pipe.ZRem(ctx, resultPrefix+successKey, id)
	pipe.ZRem(ctx, resultPrefix+failedKey, id)
	pipe.Del(ctx, resultPrefix+id)

	_, err := pipe.Exec(ctx)

	return err
}

func (r *Results) SetSuccess(ctx context.Context, id string) error {
	return r.rdb.ZAdd(ctx, resultPrefix+successKey, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *Results) SetFailed(ctx context.Context, id string) error {
	return r.rdb.ZAdd(ctx, resultPrefix+failedKey, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *Results) GetFailed(ctx context.Context) ([]string, error) {
	res, err := r.rdb.ZRevRangeByScore(ctx, resultPrefix+failedKey, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(time.Now().UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (r *Results) GetSuccess(ctx context.Context) ([]string, error) {
	res, err := r.rdb.ZRevRangeByScore(ctx, resultPrefix+successKey, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(time.Now().UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, err
	}
	return res, nil
}
