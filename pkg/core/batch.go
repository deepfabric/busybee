package core

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
)

var (
	pool = sync.Pool{
		New: func() interface{} {
			return util.AcquireBitmap()
		},
	}
)

func acquireBM() *roaring.Bitmap {
	return pool.Get().(*roaring.Bitmap)
}

func releaseBM(value *roaring.Bitmap) {
	value.Clear()
	pool.Put(value)
}

type executionbatch struct {
	changes []changedCtx
	cbs     []*stepCB
}

func newExecutionbatch() *executionbatch {
	return &executionbatch{}
}

func (b *executionbatch) addChanged(changed changedCtx) {
	for idx := range b.changes {
		if b.changes[idx].from == changed.from &&
			b.changes[idx].to == changed.to {
			b.changes[idx].add(changed)
			return
		}
	}

	ctx := changedCtx{changed.from, changed.to, who{0, acquireBM()}, changed.ttl}
	ctx.add(changed)
	b.changes = append(b.changes, ctx)
}

func (b *executionbatch) reset() {
	for _, changed := range b.changes {
		releaseBM(changed.who.users)
	}

	b.changes = b.changes[:0]
	b.cbs = b.cbs[:0]
}
