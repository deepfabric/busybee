package core

import (
	"sync"
	"testing"
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
)

type testWorker struct {
	wr   *workerRunner
	from uint32
	to   uint32
}

func (w *testWorker) onEvent(p uint32, offset uint64, event *metapb.Event) (bool, error) {
	return true, nil
}

func (w *testWorker) beloneTo(event *metapb.Event) bool {
	return event.User.UserID >= w.from && event.User.UserID < w.to
}

func (w *testWorker) commit(p uint32, value uint64) {
	w.wr.completed(p, value)
}

func (w *testWorker) stop()                   {}
func (w *testWorker) close()                  {}
func (w *testWorker) workflowID() uint64      { return 0 }
func (w *testWorker) isStopped() bool         { return false }
func (w *testWorker) init()                   {}
func (w *testWorker) cachedEventSize() uint64 { return 0 }
func (w *testWorker) handleEvent(cb func(p uint32, offset uint64)) bool {
	time.Sleep(time.Millisecond * 10)
	return true
}

type testConsumer struct {
	sync.RWMutex

	p         uint32
	committed uint64
}

func (c *testConsumer) Start(cb func(uint32, uint64, [][]byte)) {}
func (c *testConsumer) Commit(offsets map[uint32]uint64, cb func(error)) {
	c.Lock()
	defer c.Unlock()

	for p, offset := range offsets {
		c.p = p
		if c.committed < offset {
			c.committed = offset
		}
	}
}
func (c *testConsumer) CommitPartition(p uint32, offset uint64, cb func(error)) {
	c.Lock()
	defer c.Unlock()

	c.p = p
	if c.committed < offset {
		c.committed = offset
	}
}
func (c *testConsumer) Stop() {}

func (c *testConsumer) committedValue(p uint32) uint64 {
	c.RLock()
	defer c.RUnlock()

	return c.committed
}

func TestCommitOffset(t *testing.T) {
	c := &testConsumer{}

	wr := newWorkerRunner(1, 0, "wr-0-1", nil)
	wr.offsets = make(map[uint32]*offsets)
	wr.disableInitOffsets = true
	wr.disableLoadShardState = true
	wr.disableStartConsumer = true
	wr.disableLock = true
	wr.initOffsetsWithN(1)
	wr.consumer = c
	wr.start()

	w1 := &testWorker{from: 1, to: 10, wr: wr}
	w2 := &testWorker{from: 10, to: 20, wr: wr}
	wr.addWorker("w1", w1)
	wr.addWorker("w2", w2)

	wr.onEvent(0, 1, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 1}})})
	wr.onEvent(0, 2, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 10}})})

	assert.Equal(t, uint64(0), c.committedValue(0), "TestCommitOffset failed")

	w1.commit(0, 1)

	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, uint64(1), c.committedValue(0), "TestCommitOffset failed")

	// wait 2
	wr.onEvent(0, 3, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 20}})})
	assert.Equal(t, uint64(1), c.committedValue(0), "TestCommitOffset failed")

	w2.commit(0, 2)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, uint64(3), c.committedValue(0), "TestCommitOffset failed")

	wr.onEvent(0, 4, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 20}})})
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, uint64(4), c.committedValue(0), "TestCommitOffset failed")
}

func TestCommitOffsetWithNoWorker(t *testing.T) {
	c := &testConsumer{}

	wr := newWorkerRunner(1, 0, "wr-0-1", nil)
	wr.offsets = make(map[uint32]*offsets)
	wr.disableInitOffsets = true
	wr.disableLoadShardState = true
	wr.disableStartConsumer = true
	wr.disableLock = true
	wr.initOffsetsWithN(1)
	wr.consumer = c
	wr.start()

	wr.onEvent(0, 1, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 1}})})
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, uint64(1), c.committedValue(0), "TestCommitOffsetWithNoWorker failed")

	wr.onEvent(0, 2, [][]byte{protoc.MustMarshal(&metapb.Event{User: &metapb.UserEvent{UserID: 10}})})
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, uint64(2), c.committedValue(0), "TestCommitOffsetWithNoWorker failed")
}

func TestOffsets(t *testing.T) {
	offsets := &offsets{max: 2}

	assert.True(t, offsets.add(1, 1), "TestOffsets failed")
	assert.True(t, offsets.add(2, 1), "TestOffsets failed")
	assert.False(t, offsets.add(3, 1), "TestOffsets failed")
}

func TestOffsetsWithWait(t *testing.T) {
	offsets := &offsets{max: 3}
	assert.True(t, offsets.add(1, 1), "TestOffsetsWithWait failed")

	assert.True(t, offsets.add(2, 0), "TestOffsetsWithWait failed")
	assert.True(t, offsets.add(3, 0), "TestOffsetsWithWait failed")
	assert.True(t, offsets.add(4, 0), "TestOffsetsWithWait failed")

	assert.True(t, offsets.add(5, 1), "TestOffsetsWithWait failed")
	assert.False(t, offsets.add(6, 1), "TestOffsetsWithWait failed")
	assert.False(t, offsets.add(7, 0), "TestOffsetsWithWait failed")
}
