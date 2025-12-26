package gotask

const defaultQueue = "gotask:tasks"

type Task struct {
	Queue   string
	Handler string
	Payload []byte
}

type TaskMessage struct {
	UUID   string
	Status string
	Task   *Task
}

func NewTask(handler string, payload []byte) *Task {
	return &Task{
		Queue:   defaultQueue,
		Handler: handler,
		Payload: payload,
	}
}


