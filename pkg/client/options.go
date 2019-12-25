package client

import (
	"time"
)

// Option option
type Option func(*options)

type options struct {
	timeout time.Duration
}

func (opts *options) adjust() {
	if opts.timeout == 0 {
		opts.timeout = time.Second * 30
	}
}

// WithTimeout with client timeout
func WithTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.timeout = timeout
	}
}
