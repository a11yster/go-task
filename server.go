package gotask

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"sync"

	"github.com/a11yster/go-task/brokers/redis"
)

const defaultConcurrency = 1

type Handler func([]byte) error

type Server struct {
	processors map[string]Handler
	broker     Broker
}

func NewServer() *Server {
	return &Server{
		processors: make(map[string]Handler),
		broker:     redis.New(),
	}
}

func (s *Server) Start(ctx context.Context) {
	log.Println("go-task: server starting")
	var work chan []byte = make(chan []byte)
	var wg sync.WaitGroup
	go s.consume(ctx, work, defaultQueue)
	for range defaultConcurrency {
		wg.Add(1)
		go s.process(ctx, work, &wg)
	}
	wg.Wait()
}

func (s *Server) AddTask(ctx context.Context, task *Task) error {
	var b bytes.Buffer
	var encoder = gob.NewEncoder(&b)
	if err := encoder.Encode(task); err != nil {
		return err
	}
	return s.broker.Enqueue(ctx, b.Bytes(), task.Queue)
}

func (s *Server) RegisterProcessor(name string, handler Handler) {
	s.registerHandler(name, handler)
}

func (s *Server) registerHandler(name string, handler Handler) {
	s.processors[name] = handler
}

func (s *Server) getHandler(name string) Handler {
	return s.processors[name]
}

func (s *Server) consume(ctx context.Context, work chan []byte, queue string) {
	s.broker.Consume(ctx, work, queue)
}

func (s *Server) process(ctx context.Context, work chan []byte, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case work := <-work:
			var task Task
			decoder := gob.NewDecoder(bytes.NewBuffer(work))
			decoder.Decode(&task)
			log.Printf("go-task: processing task [handler=%s]", task.Handler)
			handler := s.getHandler(task.Handler)
			if err := handler(task.Payload); err != nil {
				log.Printf("go-task: task failed [handler=%s, error=%v]", task.Handler, err)
			} else {
				log.Printf("go-task: task completed [handler=%s]", task.Handler)
			}
		}
	}
}
