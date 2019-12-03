package storage

import (
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/pilosa/pilosa/roaring"
)

var (
	batchPool  sync.Pool
	bitmapPool sync.Pool
	bufPool    sync.Pool
)

func acquireBitmap() *roaring.Bitmap {
	v := bitmapPool.Get()
	if v == nil {
		return roaring.NewBTreeBitmap()
	}

	return v.(*roaring.Bitmap)
}

func releaseBitmap(value *roaring.Bitmap) {
	value.Containers.Reset()
	bitmapPool.Put(value)
}

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
