package storage

import (
	"testing"
	"time"

	"github.com/deepfabric/beehive/storage/mem"
	bhutil "github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/stretchr/testify/assert"
)

func TestPreAlloc(t *testing.T) {
	buf := goetty.NewByteBuf(256)
	// [0,10)
	preAllocRange(0, 10, 2, 0, buf)
	data := buf.RawBuf()[16:buf.GetWriteIndex()]
	assert.Equal(t, 2, len(data)/rangeLength, "TestPreAlloc failed")
	assert.Equal(t, uint64(0), goetty.Byte2UInt64(data[rangeStartOffset:]))
	assert.Equal(t, uint64(5), goetty.Byte2UInt64(data[rangeEndOffset:]))
	assert.Equal(t, uint64(5), goetty.Byte2UInt64(data[rangeStartOffset+25:]))
	assert.Equal(t, uint64(10), goetty.Byte2UInt64(data[rangeEndOffset+25:]))

	buf.Clear()
	preAllocRange(0, 11, 2, 0, buf)
	data = buf.RawBuf()[16:buf.GetWriteIndex()]
	assert.Equal(t, 2, len(data)/rangeLength, "TestPreAlloc failed")
	assert.Equal(t, uint64(0), goetty.Byte2UInt64(data[rangeStartOffset:]))
	assert.Equal(t, uint64(6), goetty.Byte2UInt64(data[rangeEndOffset:]))
	assert.Equal(t, uint64(6), goetty.Byte2UInt64(data[rangeStartOffset+25:]))
	assert.Equal(t, uint64(11), goetty.Byte2UInt64(data[rangeEndOffset+25:]))

	buf.Clear()
	preAllocRange(1, 2, 1, 0, buf)
	data = buf.RawBuf()[16:buf.GetWriteIndex()]
	assert.Equal(t, 1, len(data)/rangeLength, "TestPreAlloc failed")
	assert.Equal(t, uint64(1), goetty.Byte2UInt64(data[rangeStartOffset:]))
	assert.Equal(t, uint64(2), goetty.Byte2UInt64(data[rangeEndOffset:]))
}

func TestAllocRange(t *testing.T) {
	store := mem.NewStorage()

	key := []byte("key1")
	// offset(0~7) + last ts(8~15) + min(16~23) + max(24~31)
	value := make([]byte, rangeLength*2+preAllocLen, rangeLength*2+preAllocLen)
	goetty.Uint64ToBytesTo(1, value[minPreFieldOffset:])
	goetty.Uint64ToBytesTo(20, value[maxPreFieldOffset:])
	store.Set(key, value)

	now := time.Now().Unix()
	data := value[preAllocLen:]
	// range1 [1,10)
	range1 := data[0:rangeLength]
	range1[0] = availableState
	goetty.Int64ToBytesTo(now, range1[rangeStateOffset:])
	goetty.Uint64ToBytesTo(1, range1[rangeStartOffset:])
	goetty.Uint64ToBytesTo(10, range1[rangeEndOffset:])

	// range2 [10,20)
	range2 := data[rangeLength : rangeLength*2]
	range2[0] = availableState
	goetty.Int64ToBytesTo(now, range2[rangeStateOffset:])
	goetty.Uint64ToBytesTo(10, range2[rangeStartOffset:])
	goetty.Uint64ToBytesTo(20, range2[rangeEndOffset:])

	buf := goetty.NewByteBuf(256)
	req := rpcpb.AcquireQueueFetchRequest()
	req.Concurrency = 2
	req.Consumer = []byte("c1")
	req.Count = 10
	req.CompletedOffset = 0

	alloc, start, end := allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(availableState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(1), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(10), end, "TestPreAlloc failed")

	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(10), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(20), end, "TestPreAlloc failed")

	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(0), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(0), end, "TestPreAlloc failed")

	// part completed
	range1[rangeStateOffset] = availableState
	range2[rangeStateOffset] = availableState
	allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	req.CompletedOffset = 5
	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(availableState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(6), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(10), end, "TestPreAlloc failed")

	req.CompletedOffset = 0
	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(10), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(20), end, "TestPreAlloc failed")

	req.CompletedOffset = 0
	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(0), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(0), end, "TestPreAlloc failed")

	ts := time.Now().Unix() - completedTimeout - 1
	goetty.Int64ToBytesTo(ts, range1[rangeAllocTSOffset:])
	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(6), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(10), end, "TestPreAlloc failed")

	ts = time.Now().Unix() - completedTimeout - 1
	goetty.Int64ToBytesTo(ts, range2[rangeAllocTSOffset:])
	alloc, start, end = allocRange(key, store, bhutil.NewWriteBatch(), *req, buf)
	assert.False(t, alloc, "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range1[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, byte(processingState), range2[rangeStateOffset], "TestPreAlloc failed")
	assert.Equal(t, uint64(10), start, "TestPreAlloc failed")
	assert.Equal(t, uint64(20), end, "TestPreAlloc failed")
}
