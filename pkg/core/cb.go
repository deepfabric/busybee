package core

import (
	"sync"
)

var (
	cbPool sync.Pool
)

func acquireCB() *stepCB {
	value := cbPool.Get()
	if value == nil {
		return &stepCB{
			c: make(chan error),
		}
	}

	cb := value.(*stepCB)
	cb.c = make(chan error)
	return cb
}

func releaseCB(value *stepCB) {
	value.reset()
	cbPool.Put(value)
}

type stepCB struct {
	c chan error
}

func (cb *stepCB) reset() {
	if cb != nil &&
		cb.c != nil {
		close(cb.c)
	}
}

func (cb *stepCB) wait() error {
	return <-cb.c
}

func (cb *stepCB) complete(err error) {
	if cb != nil &&
		cb.c != nil {
		cb.c <- err
	}
}
