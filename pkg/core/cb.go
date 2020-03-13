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
	return value.(*stepCB)
}

func releaseCB(value *stepCB) {
	value.reset()
	cbPool.Put(value)
}

type stepCB struct {
	c chan error
}

func (cb *stepCB) reset() {
	if cb.c != nil {
		close(cb.c)
	}
	cb.c = make(chan error)
}

func (cb *stepCB) wait() error {
	return <-cb.c
}

func (cb *stepCB) complete(err error) {
	cb.c <- err
}
