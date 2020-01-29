package core

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/apipb"
	"github.com/deepfabric/busybee/pkg/util"
)

type executionbatch struct {
	event    apipb.Event
	from     string
	to       string
	crowd    *roaring.Bitmap
	notifies []apipb.Notify
	cbs      []*stepCB
}

func newExecutionbatch() *executionbatch {
	return &executionbatch{}
}

func (b *executionbatch) notify() {
	value := apipb.Notify{
		UserID:     b.event.UserID,
		TenantID:   b.event.TenantID,
		WorkflowID: b.event.WorkflowID,
		FromStep:   b.from,
		ToStep:     b.to,
	}
	if b.crowd != nil {
		value.Crowd = util.MustMarshalBM(b.crowd)
	}
	b.notifies = append(b.notifies, value)
}

func (b *executionbatch) next(notify bool) {
	if notify {
		b.notify()
	}

	b.event = apipb.Event{}
	b.from = ""
	b.to = ""
	b.crowd = nil
}

func (b *executionbatch) reset() {
	b.event = apipb.Event{}
	b.from = ""
	b.to = ""
	b.crowd = nil
	b.notifies = b.notifies[:0]
	b.cbs = b.cbs[:0]
}
