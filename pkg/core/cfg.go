package core

import (
	"time"
)

type options struct {
	retryInterval       time.Duration
	bitmapWorkers       int
	bitmapMaxCacheBytes uint64
	shardBitmapBytes    uint64
	clickhouseAddress   string
	clickhouseUserName  string
	clickhousePassword  string
	tempKeyTTL          uint32
	snapshotTTL         uint32
	workerCreateCount   uint64
}

func (opts *options) adjust() {
	if opts.retryInterval == 0 {
		opts.retryInterval = time.Second * 3
	}

	if opts.bitmapWorkers == 0 {
		opts.bitmapWorkers = 32
	}

	if opts.bitmapMaxCacheBytes == 0 {
		opts.bitmapMaxCacheBytes = 1024 * 1024 * 512 // 1GB
	}

	if opts.shardBitmapBytes == 0 {
		opts.shardBitmapBytes = 1024 * 256 // 256KB
	}

	if opts.tempKeyTTL == 0 {
		opts.tempKeyTTL = 3600
	}

	if opts.snapshotTTL == 0 {
		opts.snapshotTTL = 3600 * 24 * 7 // 7 day
	}

	if opts.workerCreateCount == 0 {
		opts.workerCreateCount = 8
	}
}

// Option engine option
type Option func(*options)

// WithRetryOption set retry option
func WithRetryOption(value time.Duration) Option {
	return func(opts *options) {
		opts.retryInterval = value
	}
}

// WithBitmapWorkers set bitmap workers option
func WithBitmapWorkers(value int) Option {
	return func(opts *options) {
		opts.bitmapWorkers = value
	}
}

// WithBitmapMaxCacheBytes set bitmap max cache bytes option
func WithBitmapMaxCacheBytes(value uint64) Option {
	return func(opts *options) {
		opts.bitmapMaxCacheBytes = value
	}
}

// WithClickhouse set clickhouse option
func WithClickhouse(address, name, password string) Option {
	return func(opts *options) {
		opts.clickhouseAddress = address
		opts.clickhouseUserName = name
		opts.clickhousePassword = password
	}
}

// WithShardBitmapBytes set bytes of shard bitmap option
func WithShardBitmapBytes(value uint64) Option {
	return func(opts *options) {
		opts.shardBitmapBytes = value
	}
}

// WithTempKeyTTL set temp ttl option
func WithTempKeyTTL(value uint32) Option {
	return func(opts *options) {
		opts.tempKeyTTL = value
	}
}

// WithSnapshotTTL set snapshot ttl option
func WithSnapshotTTL(value uint32) Option {
	return func(opts *options) {
		opts.snapshotTTL = value
	}
}

// WithWorkerCreateCount set worker create count option
func WithWorkerCreateCount(value uint64) Option {
	return func(opts *options) {
		opts.workerCreateCount = value
	}
}
