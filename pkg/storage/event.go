package storage

const (
	// StartingInstanceEvent  load a new instance event
	StartingInstanceEvent = iota
	// RunningInstanceEvent instance started event
	RunningInstanceEvent
	// StoppingInstanceEvent instance stopping event
	StoppingInstanceEvent
	// StoppedInstanceEvent instance stopped event
	StoppedInstanceEvent
	// RunningInstanceWorkerEvent load a new instance state event
	RunningInstanceWorkerEvent
	// RemoveInstanceWorkerEvent instance state removed to another node event
	RemoveInstanceWorkerEvent
)

// Event the event
type Event struct {
	EventType int
	Data      interface{}
}
