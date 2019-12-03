package storage

import (
	"bytes"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/pilosa/pilosa/roaring"
)

const (
	opAdd = iota
	opRemove
	opClear
	opDel
	opSet
)

type batch struct {
	shard        uint64
	writtenBytes uint64
	changedBytes int64
	h            *beeStorage
	kv           kvBatch
	bitmap       bitmapBatch
}

func (b *batch) Add(shard uint64, req *raftcmdpb.Request) (bool, *raftcmdpb.Response, error) {
	if b.shard != 0 && b.shard != shard {
		log.Fatalf("BUG: diffent shard opts in a batch, %d, %d",
			b.shard,
			shard)
	}

	b.shard = shard
	resp := pb.AcquireResponse()

	switch rpcpb.Type(req.CustemType) {
	case rpcpb.Set:
		msg := rpcpb.AcquireSetRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key

		b.kv.set(msg)

		b.writtenBytes += uint64(len(msg.Key) + len(msg.Value))
		b.changedBytes += int64(len(msg.Key) + len(msg.Value))

		value := rpcpb.AcquireSetResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseSetRequest(msg)
		rpcpb.ReleaseSetResponse(value)
	case rpcpb.Delete:
		msg := rpcpb.AcquireDeleteRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.kv.delete(msg)

		b.writtenBytes += uint64(len(msg.Key))
		b.changedBytes -= int64(len(msg.Key))

		value := rpcpb.AcquireDeleteResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseDeleteRequest(msg)
		rpcpb.ReleaseDeleteResponse(value)
	case rpcpb.BMCreate:
		msg := rpcpb.AcquireBMCreateRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.bitmap.add(msg.Key, msg.Value...)

		value := rpcpb.AcquireBMCreateResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseBMCreateRequest(msg)
		rpcpb.ReleaseBMCreateResponse(value)
	case rpcpb.BMAdd:
		msg := rpcpb.AcquireBMAddRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.bitmap.add(msg.Key, msg.Value...)

		value := rpcpb.AcquireBMAddResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseBMAddRequest(msg)
		rpcpb.ReleaseBMAddResponse(value)
	case rpcpb.BMRemove:
		msg := rpcpb.AcquireBMRemoveRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.bitmap.remove(msg.Key, msg.Value...)

		value := rpcpb.AcquireBMRemoveResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseBMRemoveRequest(msg)
		rpcpb.ReleaseBMRemoveResponse(value)
	case rpcpb.BMClear:
		msg := rpcpb.AcquireBMClearRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.bitmap.clear(msg.Key)

		value := rpcpb.AcquireBMClearResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseBMClearRequest(msg)
		rpcpb.ReleaseBMClearResponse(value)
	case rpcpb.BMDel:
		msg := rpcpb.AcquireBMDelRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		b.bitmap.del(msg.Key)

		value := rpcpb.AcquireBMDelResponse()
		value.ID = msg.ID
		resp.Value = protoc.MustMarshal(value)

		rpcpb.ReleaseBMDelRequest(msg)
		rpcpb.ReleaseBMDelResponse(value)
	default:
		return false, nil, nil
	}

	return true, resp, nil
}

func (b *batch) Execute() (uint64, int64, error) {
	s := b.h.getStore(b.shard)
	wb := s.NewWriteBatch()

	if len(b.kv.ops) > 0 {
		idx := 0
		for _, op := range b.kv.ops {
			switch op {
			case opSet:
				wb.Set(b.kv.pairs[idx], b.kv.pairs[idx+1])
				idx += 2
			case opDel:
				wb.Delete(b.kv.pairs[idx])
				idx++
			}
		}
	}

	if len(b.bitmap.ops) > 0 {
		buf := bytes.NewBuffer(nil)
		bm := roaring.NewBTreeBitmap()
		for idx, ops := range b.bitmap.ops {
			key := b.bitmap.bitmaps[idx]
			if ops[len(ops)-1] == opDel {
				wb.Delete(key)
				b.changedBytes -= int64(len(key))
				continue
			}

			value, err := s.Get(key)
			if err != nil {
				return 0, 0, err
			}

			bm.Containers.Reset()
			if len(value) > 0 {
				_, _, err = bm.ImportRoaringBits(value[1:], false, false, 0)
				if err != nil {
					return 0, 0, err
				}
			}

			for _, op := range ops {
				switch op {
				case opAdd:
					bm = bm.Union(b.bitmap.bitmapAdds[idx])
					break
				case opRemove:
					bm = bm.Xor(b.bitmap.bitmapRemoves[idx])
					break
				case opClear:
					bm.Containers.Reset()
					break
				case opDel:
					bm.Containers.Reset()
					break
				}
			}

			buf.Reset()
			_, err = bm.WriteTo(buf)
			if err != nil {
				return 0, 0, err
			}

			wb.Set(key, appendPrefix(buf.Bytes(), kvType))

			b.writtenBytes += uint64(len(buf.Bytes()) - len(value))
			b.changedBytes += int64(len(buf.Bytes()) - len(value))
		}
	}

	b.Reset()
	err := s.Write(wb, false)
	return b.writtenBytes, b.changedBytes, err
}

func (b *batch) Reset() {
	b.shard = 0
	b.kv.reset()
	b.bitmap.reset()
}

type kvBatch struct {
	pairs [][]byte
	ops   []int
}

func (kv *kvBatch) reset() {
	kv.pairs = kv.pairs[:0]
	kv.ops = kv.ops[:0]
}

func (kv *kvBatch) set(req *rpcpb.SetRequest) {
	kv.pairs = append(kv.pairs, req.Key, appendPrefix(req.Value, kvType))
	kv.ops = append(kv.ops, opSet)
}

func (kv *kvBatch) delete(req *rpcpb.DeleteRequest) {
	kv.pairs = append(kv.pairs, req.Key)
	kv.ops = append(kv.ops, opDel)
}

type bitmapBatch struct {
	bitmaps       [][]byte
	bitmapAdds    []*roaring.Bitmap
	bitmapRemoves []*roaring.Bitmap
	ops           [][]int
}

func (rb *bitmapBatch) add(bm []byte, values ...uint64) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], opAdd)
			rb.appendAdds(idx, values...)
			return
		}
	}

	value := acquireBitmap()
	value.Add(values...)

	rb.ops = append(rb.ops, []int{opAdd})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, value)
	rb.bitmapRemoves = append(rb.bitmapRemoves, nil)
}

func (rb *bitmapBatch) remove(bm []byte, values ...uint64) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], opRemove)
			rb.appendRemoves(idx, values...)
			return
		}
	}

	value := acquireBitmap()
	value.Add(values...)

	rb.ops = append(rb.ops, []int{opRemove})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, nil)
	rb.bitmapRemoves = append(rb.bitmapRemoves, value)
}

func (rb *bitmapBatch) clear(bm []byte) {
	rb.clean(bm, opClear)
}

func (rb *bitmapBatch) del(bm []byte) {
	rb.clean(bm, opDel)
}

func (rb *bitmapBatch) appendAdds(idx int, values ...uint64) {
	if rb.bitmapAdds[idx] == nil {
		rb.bitmapAdds[idx] = acquireBitmap()
	}

	rb.bitmapAdds[idx].Add(values...)
}

func (rb *bitmapBatch) appendRemoves(idx int, values ...uint64) {
	if rb.bitmapRemoves[idx] == nil {
		rb.bitmapRemoves[idx] = acquireBitmap()
	}

	rb.bitmapRemoves[idx].Add(values...)
}

func (rb *bitmapBatch) clean(bm []byte, op int) {
	for idx, key := range rb.bitmaps {
		if bytes.Compare(key, bm) == 0 {
			rb.ops[idx] = append(rb.ops[idx], op)

			if rb.bitmapAdds[idx] != nil {
				releaseBitmap(rb.bitmapAdds[idx])
				rb.bitmapAdds[idx] = nil
			}

			if rb.bitmapRemoves[idx] != nil {
				releaseBitmap(rb.bitmapRemoves[idx])
				rb.bitmapRemoves[idx] = nil
			}
			return
		}
	}

	rb.ops = append(rb.ops, []int{op})
	rb.bitmaps = append(rb.bitmaps, bm)
	rb.bitmapAdds = append(rb.bitmapAdds, nil)
	rb.bitmapRemoves = append(rb.bitmapRemoves, nil)
}

func (rb *bitmapBatch) reset() {
	for _, bm := range rb.bitmapAdds {
		if bm != nil {
			releaseBitmap(bm)
		}
	}

	for _, bm := range rb.bitmapRemoves {
		if bm != nil {
			releaseBitmap(bm)
		}
	}

	rb.ops = rb.ops[:0]
	rb.bitmaps = rb.bitmaps[:0]
	rb.bitmapAdds = rb.bitmapAdds[:0]
	rb.bitmapRemoves = rb.bitmapRemoves[:0]
}
