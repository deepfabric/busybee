package storage

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	bhstorage "github.com/deepfabric/beehive/storage"
	bhutil "github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

type kvBatch struct {
	pairs [][]byte
	ops   []int
	ttl   []int64
}

func newKVBatch() batchType {
	return &kvBatch{}
}

func (kv *kvBatch) support() []rpcpb.Type {
	return []rpcpb.Type{rpcpb.Set, rpcpb.Delete}
}

func (kv *kvBatch) addReq(req *raftcmdpb.Request, resp *raftcmdpb.Response, b *batch, buf *goetty.ByteBuf) {
	switch rpcpb.Type(req.CustemType) {
	case rpcpb.Set:
		msg := rpcpb.AcquireSetRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key

		kv.set(msg, buf)

		b.writtenBytes += uint64(len(msg.Key) + len(msg.Value))
		b.changedBytes += int64(len(msg.Key) + len(msg.Value))

		resp.Value = rpcpb.EmptyRespBytes
		rpcpb.ReleaseSetRequest(msg)
	case rpcpb.Delete:
		msg := rpcpb.AcquireDeleteRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		kv.delete(msg)

		b.writtenBytes += uint64(len(msg.Key))
		b.changedBytes -= int64(len(msg.Key))

		resp.Value = rpcpb.EmptyRespBytes
		rpcpb.ReleaseDeleteRequest(msg)
	default:
		log.Fatalf("BUG: not supoprt rpctype: %d", rpcpb.Type(req.CustemType))
	}
}

func (kv *kvBatch) exec(s bhstorage.DataStorage, wb bhutil.WriteBatch, b *batch) error {
	if len(kv.ops) > 0 {
		idx := 0
		for i, op := range kv.ops {
			switch op {
			case opSet:
				wb.SetWithTTL(kv.pairs[idx], kv.pairs[idx+1], kv.ttl[i])
				idx += 2

			case opDel:
				wb.Delete(kv.pairs[idx])
				idx++
			}
		}
	}

	return nil
}

func (kv *kvBatch) reset() {
	kv.pairs = kv.pairs[:0]
	kv.ops = kv.ops[:0]
	kv.ttl = kv.ttl[:0]
}

func (kv *kvBatch) set(req *rpcpb.SetRequest, buf *goetty.ByteBuf) {
	kv.pairs = append(kv.pairs, req.Key, appendValuePrefix(buf, req.Value, kvType))
	kv.ops = append(kv.ops, opSet)
	kv.ttl = append(kv.ttl, req.TTL)
}

func (kv *kvBatch) delete(req *rpcpb.DeleteRequest) {
	kv.pairs = append(kv.pairs, req.Key)
	kv.ops = append(kv.ops, opDel)
	kv.ttl = append(kv.ttl, 0)
}
