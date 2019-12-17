package core

import (
	"errors"
)

var (
	// ErrTimeout timeout error
	ErrTimeout = errors.New("Timeout")

	// ErrWorkerNotFound The state worker not in the node
	ErrWorkerNotFound = errors.New("The state worker not in the node")
)
