package gotask

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

const defaultConcurrency = 1

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
		opts.Queue = DefaultQueue
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

	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go s.process(ctx, work, &wg)
	}

	wg.Wait()
}

func (s *Server) Enqueue(ctx context.Context, job Job) (string, error) {
	meta := DefaultMeta(job.Opts)

	msg := job.message(meta)

	b, err := msgpack.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job message: %w", err)
	}

	if err := s.broker.Enqueue(ctx, b, meta.Queue); err != nil {
		return "", fmt.Errorf("error enquing the job to the broker")
	}

	return msg.Id, nil
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
			var message JobMessage
			if err := msgpack.Unmarshal(b, &message); err != nil {
				log.Printf("go-task: failed to decode job message: %v", err)
				continue
			}

			log.Printf("go-task: processing job [id=%s, handler=%s]", message.Id, message.Job.Task)

			handler := s.getHandler(message.Job.Task)
			if handler == nil {
				log.Printf("go-task: no processor registered [id=%s, handler=%s]", message.Id, message.Job.Task)
				continue
			}
			if err := handler(message.Job.Payload); err != nil {
				log.Printf("go-task: job failed [id=%s, handler=%s, error=%v]", message.Id, message.Job.Task, err)
			} else {
				log.Printf("go-task: job completed [id=%s, handler=%s]", message.Id, message.Job.Task)
			}
		}
	}
}
