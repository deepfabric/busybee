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
		return &stepCB{}
	}
	return value.(*stepCB)
}

func releaseCB(value *stepCB) {
	if value == nil {
		return
	}

	if value.c != nil {
		close(value.c)
	}
	cbPool.Put(value)
}

type stepCB struct {
	c chan error
}

func (cb *stepCB) reset() {
	cb.c = make(chan error)
}

func (cb *stepCB) wait() error {
	return <-cb.c
}

func (cb *stepCB) complete(err error) {
	cb.c <- err
}
