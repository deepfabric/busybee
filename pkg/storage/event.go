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
	// InstanceWorkerCreatedEvent create a new instance state event
	InstanceWorkerCreatedEvent
	// InstanceWorkerDestoriedEvent destoried instance state event
	InstanceWorkerDestoriedEvent
	// StartRunnerEvent start runner
	StartRunnerEvent
	// StopRunnerEvent stop runner
	StopRunnerEvent
)

// Event the event
type Event struct {
	EventType int
	Data      interface{}
}
