package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	availableState  byte = 0
	processingState byte = 1
	completedState  byte = 2

	completedTimeout = int64(60) // seconds

	completedFieldOffset = 0
	lastFetchFieldOffset = 8
	minPreFieldOffset    = 16
	maxPreFieldOffset    = 24
	rangeStateOffset     = 0
	rangeAllocTSOffset   = 1
	rangeStartOffset     = 9
	rangeEndOffset       = 17

	noPreAllocLen = 16
	preAllocLen   = 32
	rangeLength   = 25
)

func (h *beeStorage) queueFetch(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueFetch := rpcpb.QueueFetchRequest{}
	protoc.MustUnmarshal(&queueFetch, req.Cmd)

	key := committedOffsetKey(req.Key, queueFetch.Consumer, buf)
	store := h.getStore(shard.ID)
	wb := store.NewWriteBatch()
	allocNewRanges, start, end := allocRange(key, store, wb, queueFetch, buf)
	if start == end {
		resp.Value = rpcpb.EmptyBytesSliceBytes
		return 0, 0, resp
	}

	startKey := itemKey(req.Key, start, buf)
	endKey := itemKey(req.Key, end, buf)

	end = start
	var items [][]byte
	err := h.getStore(shard.ID).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		items = append(items, value)
		end++
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	if start == end {
		resp.Value = rpcpb.EmptyBytesSliceBytes
		return 0, 0, resp
	}

	customResp := rpcpb.AcquireBytesSliceResponse()

	if allocNewRanges {
		now := time.Now().Unix()
		idx := buf.GetWriteIndex()
		buf.WriteUint64(start - 1)
		buf.WriteInt64(now)
		end = preAllocRange(start, end, queueFetch.Concurrency, now, buf)
		wb.Set(key, buf.RawBuf()[idx:buf.GetWriteIndex()])
		items = items[:int(end-start)]
	}

	err = store.Write(wb, false)
	if err != nil {
		log.Fatalf("set consumer committed offset failed with %+v", err)
	}

	customResp.Values = items
	customResp.LastValue = end - 1
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesSliceResponse(customResp)
	return 0, 0, resp
}

func allocRange(key []byte, store storage.DataStorage, wb util.WriteBatch,
	req rpcpb.QueueFetchRequest, buf *goetty.ByteBuf) (bool, uint64, uint64) {
	completed := req.CompletedOffset
	// committedOffsetKey -> offset(0~7) + last ts(8~15) + min(16~23) + max(24~31)
	value, err := store.Get(key)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	if len(value) == 0 {
		return true, completed + 1, completed + 1 + req.Count
	}

	alreadyCompleted := goetty.Byte2UInt64(value[completedFieldOffset:])
	if completed < alreadyCompleted {
		completed = alreadyCompleted
	}

	// has no pre alloc range
	if len(value) <= minPreFieldOffset {
		return true, completed + 1, completed + 1 + req.Count
	}

	now := time.Now().Unix()
	if (len(value)-preAllocLen)%25 != 0 {
		log.Fatalf("BUG: pre alloc range must pow of 25 bytes(state + ts + start + end")
	}

	min := goetty.Byte2UInt64(value[minPreFieldOffset:])
	max := goetty.Byte2UInt64(value[maxPreFieldOffset:])

	if completed >= max {
		log.Fatalf("BUG: completed not alloc items, already alloc max offset %d, giving %d",
			max, completed)
	}

	n := (len(value) - preAllocLen) / rangeLength
	offset := preAllocLen
	if completed >= min && completed < max {
		for i := 0; i < n; i++ {
			offset += rangeLength * i
			start := goetty.Byte2UInt64(value[offset+rangeStartOffset:])
			end := goetty.Byte2UInt64(value[offset+rangeEndOffset:])

			if completed >= start && completed < end {
				state := value[offset+rangeStateOffset]
				switch state {
				case availableState:
					log.Fatalf("BUG: can not complete a available state range, %d", completed)
				case processingState:
					start := goetty.Byte2UInt64(value[offset+rangeStartOffset:])
					end := goetty.Byte2UInt64(value[offset+rangeEndOffset:])

					if completed == end-1 {
						// range completed by consumer
						// e.g. range is [1, 10), completed is 9
						// become -> completed, old ts, [1, 9)
						// we don't need put value to writebatch, because
						// we will try alloc the next available or timeout range or
						// fetch some new ranges from store
						value[offset+rangeStateOffset] = completedState
					} else if completed >= start {
						// range partially completed by consumer
						// e.g. range is [1, 10), completed is 1
						// become -> processing, new ts, [2, 10)
						goetty.Int64ToBytesTo(now, value[offset+rangeAllocTSOffset:])
						goetty.Uint64ToBytesTo(completed+1, value[offset+rangeStartOffset:])
						wb.Set(key, value)
						return false, completed + 1, end
					} else {
						log.Fatalf("BUG: can not at here, %d, [%d,%d)",
							completed,
							start,
							end)
					}
				}

				break
			}
		}
	}

	// try find available range or timeout processing range
	offset = preAllocLen
	completedCount := 0
	processingCount := 0
	for i := 0; i < n; i++ {
		offset = preAllocLen + rangeLength*i
		start := goetty.Byte2UInt64(value[offset+rangeStartOffset:])
		end := goetty.Byte2UInt64(value[offset+rangeEndOffset:])
		switch value[offset+rangeStateOffset] {
		case availableState:
			value[offset+rangeStateOffset] = processingState
			goetty.Int64ToBytesTo(now, value[offset+rangeAllocTSOffset:])
			wb.Set(key, value)
			return false, start, end
		case processingState:
			// processing timeout, re-allocate to the consumer
			ts := goetty.Byte2Int64(value[offset+rangeAllocTSOffset:])
			if now-ts > completedTimeout {
				goetty.Int64ToBytesTo(now, value[offset+rangeAllocTSOffset:])
				wb.Set(key, value)
				return false, start, end
			}

			processingCount++
		case completedState:
			completedCount++
		}
	}

	if completedCount != n && processingCount != n {
		log.Fatalf("BUG: must all range in completed or processing state, but %d completed and %d processing",
			completedCount, processingCount)
	}

	// all in processing
	if processingCount == n {
		return false, 0, 0
	}

	// all completed, fetch new ranges from store later
	return true, completed + 1, completed + 1 + req.Count
}

func preAllocRange(start, end, concurrency uint64, now int64, buf *goetty.ByteBuf) uint64 {
	if concurrency == 0 {
		return 0
	}

	buf.WriteUint64(start)
	buf.WriteUint64(end)

	step := (end - start) / concurrency
	if step == 0 {
		step = 1
	} else if (end-start)%concurrency != 0 {
		step++
	}

	first := uint64(0)
	for start < end {
		if first == 0 {
			buf.WriteByte(processingState)
		} else {
			buf.WriteByte(availableState)
		}

		buf.WriteInt64(now)
		buf.WriteUInt64(start)
		start += step
		if start >= end {
			start = end
		}
		buf.WriteUInt64(start)

		if first == 0 {
			first = start
		}
	}

	return first
}
