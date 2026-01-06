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

type TaskOpts struct {
	Concurrency  uint32
	Queue        string
	SuccessCB    func(JobCtx)
	ProcessingCB func(JobCtx)
	RetryingCB   func(JobCtx, error)
	FailedCB     func(JobCtx, error)
}

type Task struct {
	name    string
	handler Handler
	opts    TaskOpts
}

type TaskInfo struct {
	Name        string `json:"name" msgpack:"name"`
	Queue       string `json:"queue" msgpack:"queue"`
	Concurrency uint32 `json:"concurrency" msgpack:"concurrency"`
}

type Handler func([]byte, JobCtx) error

type ServerOpts struct {
	Broker      Broker
	Results     Results
	Concurrency int
	Queue       string
}

type Server struct {
	tasks       map[string]Task
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
		tasks:       make(map[string]Task),
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

func (s *Server) RegisterTask(name string, handler Handler, opts TaskOpts) error {
	if opts.Queue == "" {
		opts.Queue = s.queue
	}

	if opts.Concurrency <= 0 {
		opts.Concurrency = uint32(s.concurrency)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[name]; exists {
		return fmt.Errorf("task %s is already registered", name)
	}

	s.tasks[name] = Task{
		name:    name,
		handler: handler,
		opts:    opts,
	}
	log.Printf("go-task: registered task [name=%s, queue=%s, concurrency=%d]",
		name, opts.Queue, opts.Concurrency)

	return nil
}

func (s *Server) RegisterProcessor(name string, handler Handler) {
	s.RegisterTask(name, handler, TaskOpts{})
}

func (s *Server) getTask(name string) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.tasks[name]
	if !ok {
		return Task{}, fmt.Errorf("task %s not found", name)
	}

	return task, nil
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

			task, err := s.getTask(message.Job.Task)
			if err != nil {
				log.Printf("go-task: %v", err)
				continue
			}

			if err := s.statusProcessing(ctx, message); err != nil {
				log.Printf("go-task: failed to set processing status: %v", err)
				continue
			}

			if err := s.execJob(ctx, message, task); err != nil {
				log.Printf("go-task: job execution error [id=%s, error=%v]", message.Id, err)
			}
		}
	}
}

func (s *Server) execJob(ctx context.Context, msg JobMessage, task Task) error {
	jobCtx := JobCtx{
		Meta:  msg.Meta,
		store: s.results,
	}

	var (
		errChan          = make(chan error, 1)
		err              error
		jctx, cancelFunc = context.WithCancel(ctx)
	)
	defer cancelFunc()

	if msg.Job.Opts.Timeout > 0 {
		jctx, cancelFunc = context.WithDeadline(ctx, time.Now().Add(msg.Job.Opts.Timeout))
	}

	jobCtx.Context = jctx

	if task.opts.ProcessingCB != nil {
		task.opts.ProcessingCB(jobCtx)
	}

	go func() {
		errChan <- task.handler(msg.Job.Payload, jobCtx)
		close(errChan)
	}()

	select {
	case <-jctx.Done():
		cancelFunc()
		err = jctx.Err()
		if err == context.Canceled {
			err = nil
		}

	case handlerErr := <-errChan:
		cancelFunc()
		err = handlerErr
		if err == context.Canceled {
			err = nil
		}
	}

	if err != nil {
		msg.PrevErr = err.Error()

		if msg.MaxRetry > msg.Retried {
			if task.opts.RetryingCB != nil {
				task.opts.RetryingCB(jobCtx, err)
			}
			return s.retryJob(ctx, msg)
		}

		if task.opts.FailedCB != nil {
			task.opts.FailedCB(jobCtx, err)
		}

		if statusErr := s.statusFailed(ctx, msg); statusErr != nil {
			return fmt.Errorf("failed to set failed status: %w", statusErr)
		}

		log.Printf("go-task: job failed permanently [id=%s, task=%s, error=%v]",
			msg.Id, msg.Job.Task, err)
		return nil
	}

	if task.opts.SuccessCB != nil {
		task.opts.SuccessCB(jobCtx)
	}

	if err := s.statusDone(ctx, msg); err != nil {
		return fmt.Errorf("failed to set done status: %w", err)
	}

	log.Printf("go-task: job completed [id=%s, task=%s]", msg.Id, msg.Job.Task)
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

func (s *Server) GetJob(ctx context.Context, id string) (JobMessage, error) {
	b, err := s.GetResult(ctx, jobPrefix+id)
	if err != nil {
		return JobMessage{}, err
	}

	var msg JobMessage
	if err := msgpack.Unmarshal(b, &msg); err != nil {
		return JobMessage{}, fmt.Errorf("failed to unmarshal job message: %w", err)
	}
	return msg, nil
}

func (s *Server) GetResult(ctx context.Context, id string) ([]byte, error) {
	b, err := s.results.Get(ctx, id)
	if err == nil {
		return b, nil
	}
	return nil, err
}

func (s *Server) DeleteJob(ctx context.Context, id string) error {
	return s.results.DeleteJob(ctx, id)
}

func (s *Server) GetFailed(ctx context.Context) ([]string, error) {
	return s.results.GetFailed(ctx)
}

func (s *Server) GetSuccess(ctx context.Context) ([]string, error) {
	return s.results.GetSuccess(ctx)
}
