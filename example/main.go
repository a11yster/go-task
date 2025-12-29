package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	gotask "github.com/a11yster/go-task"
	"github.com/a11yster/go-task/brokers/redis"
)

type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

// SumProcessor prints the sum of two integer arguments.
func SumProcessor(b []byte) error {
	var pl SumPayload
	if err := json.Unmarshal(b, &pl); err != nil {
		return err
	}

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("Sum: %d + %d = %d\n", pl.Arg1, pl.Arg2, pl.Arg1+pl.Arg2)

	return nil
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	broker := redis.New()
	srv, err := gotask.NewServer(gotask.ServerOpts{
		Broker:      broker,
		Concurrency: 5,
		Queue:       "add-queue",
	})
	if err != nil {
		log.Fatal(err)
	}
	srv.RegisterProcessor("add", SumProcessor)

	go func() {
		for i := 0; i < 100; i++ {
			for j := 0; j < 10; j++ {
				b, _ := json.Marshal(SumPayload{Arg1: i*10 + j, Arg2: 4})
				task := gotask.NewTask("add", b)
				task.Queue = "add-queue"
				if err := srv.AddTask(ctx, task); err != nil {
					log.Fatal(err)
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	srv.Start(ctx)

	fmt.Println("exit..")
}
