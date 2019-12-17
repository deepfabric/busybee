package storage

import (
	"sync"

	"github.com/fagongzi/goetty"
)

var (
	batchPool sync.Pool
	bufPool   sync.Pool
)

func acquireBatch() *batch {
	v := batchPool.Get()
	if v == nil {
		return &batch{}
	}

	return v.(*batch)
}

func releaseBatch(value *batch) {
	value.Reset()
	batchPool.Put(value)
}

func acquireBuf() *goetty.ByteBuf {
	value := bufPool.Get()
	if value == nil {
		return goetty.NewByteBuf(512)
	}

	buf := value.(*goetty.ByteBuf)
	buf.Resume(512)
	return buf
}

func releaseBuf(value *goetty.ByteBuf) {
	value.Clear()
	value.Release()
	bufPool.Put(value)
}
