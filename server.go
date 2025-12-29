package gotask

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"runtime"
	"sync"
)

const defaultConcurrency = 1
const defaultQueue = "gotask:tasks"

type Handler func([]byte) error

type ServerOpts struct {
	Broker      Broker
	Concurrency int
	Queue       string
}

type Server struct {
	processors  map[string]Handler
	broker      Broker
	concurrency int
	queue       string
	mu          sync.RWMutex
}

func NewServer(opts ServerOpts) (*Server, error) {
	if opts.Broker == nil {
		return nil, fmt.Errorf("broker is missing in server opts")
	}

	if opts.Concurrency <= 0 {
		opts.Concurrency = runtime.GOMAXPROCS(0)
	}

	if opts.Queue == "" {
		opts.Queue = defaultQueue
	}
	return &Server{
		processors:  make(map[string]Handler),
		broker:      opts.Broker,
		concurrency: opts.Concurrency,
		queue:       opts.Queue,
	}, nil
}

func (s *Server) Start(ctx context.Context) {
	log.Println("go-task: server starting")
	work := make(chan []byte)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.consume(ctx, work, s.queue)
		close(work)
	}()

	for i := 0; i < defaultConcurrency; i++ {
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
	s.mu.Lock()
	s.processors[name] = handler
	s.mu.Unlock()
}

func (s *Server) getHandler(name string) Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processors[name]
}

func (s *Server) consume(ctx context.Context, work chan []byte, queue string) {
	s.broker.Consume(ctx, work, queue)
}

func (s *Server) process(ctx context.Context, work chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-work:
			if !ok {
				return
			}
			var task Task
			decoder := gob.NewDecoder(bytes.NewBuffer(b))
			if err := decoder.Decode(&task); err != nil {
				log.Printf("go-task: failed to decode task payload: %v", err)
				continue
			}

			log.Printf("go-task: processing task [handler=%s]", task.Handler)
			handler := s.getHandler(task.Handler)
			if handler == nil {
				log.Printf("go-task: no processor registered [handler=%s]", task.Handler)
				continue
			}
			if err := handler(task.Payload); err != nil {
				log.Printf("go-task: task failed [handler=%s, error=%v]", task.Handler, err)
			} else {
				log.Printf("go-task: task completed [handler=%s]", task.Handler)
			}
		}
	}
}
