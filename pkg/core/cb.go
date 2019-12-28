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
	if value.c != nil {
		close(value.c)
	}
	cbPool.Put(value)
}

type stepCB struct {
	c chan struct{}
}

func (cb *stepCB) wait() {
	<-cb.c
}
