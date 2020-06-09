package queue

import (
	"github.com/fagongzi/log"
)

var (
	logger log.Logger
)

func init() {
	logger = log.NewLoggerWithPrefix("[queue]")
}

// Consumer a simple queue consumer
type Consumer interface {
	// Start start the consumer
	Start(batch uint64, cb func(uint32, uint64, ...[]byte) (uint64, error))
	// Stop stop consumer
	Stop()
}
