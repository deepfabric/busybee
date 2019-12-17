package core

import (
	"context"
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
	value.ctx = nil
	if value.c != nil {
		close(value.c)
	}
	cbPool.Put(value)
}

type stepCB struct {
	ctx context.Context
	c   chan error
}

func (cb *stepCB) wait() error {
	return <-cb.c
}
