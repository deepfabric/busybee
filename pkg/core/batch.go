package core

import (
	"bytes"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/pilosa/pilosa/roaring"
)

type executionbatch struct {
	event metapb.Event
	from  string
	to    string
	crowd *roaring.Bitmap
	buf   *bytes.Buffer

	notifies []metapb.Notify
	cbs      []*stepCB
}

func newExecutionbatch() *executionbatch {
	return &executionbatch{
		buf: util.AcquireBuf(),
	}
}

func (b *executionbatch) next() {
	value := metapb.Notify{
		UserID:     b.event.UserID,
		TenantID:   b.event.TenantID,
		WorkflowID: b.event.WorkflowID,
		InstanceID: b.event.InstanceID,
		Step:       b.from,
	}
	if b.crowd != nil {
		b.buf.Reset()
		util.MustWriteTo(b.crowd, b.buf)
		value.Crowd = b.buf.Bytes()
	}
	b.notifies = append(b.notifies, value)

	b.event = metapb.Event{}
	b.from = ""
	b.to = ""
	b.crowd = nil
}

func (b *executionbatch) reset() {
	b.event = metapb.Event{}
	b.from = ""
	b.to = ""
	b.crowd = nil
	b.notifies = b.notifies[:0]
	b.cbs = b.cbs[:0]
}
