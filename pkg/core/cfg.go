package core

import (
	"time"
)

type options struct {
	maxCrowdShardSize uint64
	retryInterval     time.Duration
}
