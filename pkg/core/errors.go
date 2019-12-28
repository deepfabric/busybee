package core

import (
	"errors"
)

var (
	errTimeout        = errors.New("Timeout")
	errWorkerNotFound = errors.New("The state worker not in the node")
)
