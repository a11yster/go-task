package gotask

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const defaultConcurrency = 1

type Handler func([]byte, JobCtx) error

type ServerOpts struct {
	Broker      Broker
	Results     Results
	Concurrency int
	Queue       string
}

type Server struct {
	processors  map[string]Handler
	broker      Broker
	results     Results
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

	if opts.Results == nil {
		return nil, fmt.Errorf("results is required in server opts")
	}

	if opts.Queue == "" {
		opts.Queue = DefaultQueue
	}
	return &Server{
		processors:  make(map[string]Handler),
		broker:      opts.Broker,
		results:     opts.Results,
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

	if err := s.statusStarted(ctx, msg); err != nil {
		return "", fmt.Errorf("failed to set job status: %w", err)
	}

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

func (s *Server) getHandler(name string) (Handler, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	handler, ok := s.processors[name]
	if !ok {
		return nil, fmt.Errorf("handler %s not found", name)
	}
	return handler, nil
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

			handler, err := s.getHandler(message.Job.Task)
			if err != nil {
				log.Printf("go-task: %v", err)
				continue
			}

			if err := s.statusProcessing(ctx, message); err != nil {
				log.Printf("go-task: failed to set processing status: %v", err)
				continue
			}

			if err := s.execJob(ctx, message, handler); err != nil {
				log.Printf("go-task: job execution error [id=%s, error=%v]", message.Id, err)
			}
		}
	}
}

func (s *Server) execJob(ctx context.Context, msg JobMessage, handler Handler) error {
	jobCtx := JobCtx{
		Context: ctx,
		Meta:    msg.Meta,
		store:   s.results,
	}

	err := handler(msg.Job.Payload, jobCtx)

	if err != nil {
		msg.PrevErr = err.Error()

		if msg.MaxRetry > msg.Retried {
			return s.retryJob(ctx, msg)
		}

		if statusErr := s.statusFailed(ctx, msg); statusErr != nil {
			return fmt.Errorf("failed to set failed status: %w", statusErr)
		}
		log.Printf("go-task: job failed permanently [id=%s, handler=%s, error=%v]",
			msg.Id, msg.Job.Task, err)
		return nil
	}

	if err := s.statusDone(ctx, msg); err != nil {
		return fmt.Errorf("failed to set done status: %w", err)
	}
	log.Printf("go-task: job completed [id=%s, handler=%s]", msg.Id, msg.Job.Task)
	return nil
}

func (s *Server) retryJob(ctx context.Context, msg JobMessage) error {
	msg.Retried++

	if err := s.statusRetrying(ctx, msg); err != nil {
		return fmt.Errorf("failed to set retrying status: %w", err)
	}
	
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshall job: %w", err)
	}

	if err := s.broker.Enqueue(ctx, b, msg.Queue); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	log.Printf("go-task: job retrying [id=%s, attempt=%d/%d]",
		msg.Id, msg.Retried, msg.MaxRetry)

	return nil
}

func (s *Server) setJobMessage(ctx context.Context, msg JobMessage) error {
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal job message: %w", err)
	}
	if err := s.results.Set(ctx, jobPrefix+msg.Id, b); err != nil {
		return fmt.Errorf("failed to set job message: %w", err)
	}
	return nil
}

func (s *Server) statusStarted(ctx context.Context, msg JobMessage) error {
	msg.ProcessedAt = time.Now()
	msg.Status = StatusStarted
	return s.setJobMessage(ctx, msg)
}

func (s *Server) statusProcessing(ctx context.Context, msg JobMessage) error {
	msg.ProcessedAt = time.Now()
	msg.Status = StatusProcessing
	return s.setJobMessage(ctx, msg)
}

func (s *Server) statusDone(ctx context.Context, msg JobMessage) error {
	msg.ProcessedAt = time.Now()
	msg.Status = StatusDone

	if err := s.results.SetSuccess(ctx, msg.Id); err != nil {
		return err
	}
	return s.setJobMessage(ctx, msg)
}

func (s *Server) statusFailed(ctx context.Context, msg JobMessage) error {
	msg.ProcessedAt = time.Now()
	msg.Status = StatusFailed

	if err := s.results.SetFailed(ctx, msg.Id); err != nil {
		return err
	}
	return s.setJobMessage(ctx, msg)
}

func (s *Server) statusRetrying(ctx context.Context, msg JobMessage) error {
	msg.ProcessedAt = time.Now()
	msg.Status = StatusRetrying
	return s.setJobMessage(ctx, msg)
}
