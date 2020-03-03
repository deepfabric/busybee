package util

import (
	"bytes"
	"sync"
)

var (
	bufPool sync.Pool
)

// AcquireBuf get a bm from pool
func AcquireBuf() *bytes.Buffer {
	v := bufPool.Get()
	if v == nil {
		return bytes.NewBuffer(nil)
	}

	return v.(*bytes.Buffer)
}

// ReleaseBuf release a bitmap
func ReleaseBuf(value *bytes.Buffer) {
	value.Reset()
	bufPool.Put(value)
}
