package storage

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

type kvBatch struct {
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

		kv.set(msg, buf, b.wb)

		b.writtenBytes += uint64(len(msg.Key) + len(msg.Value))
		b.changedBytes += int64(len(msg.Key) + len(msg.Value))

		resp.Value = rpcpb.EmptyRespBytes
		rpcpb.ReleaseSetRequest(msg)
	case rpcpb.Delete:
		msg := rpcpb.AcquireDeleteRequest()
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key
		kv.delete(msg, b.wb)

		b.writtenBytes += uint64(len(msg.Key))
		b.changedBytes -= int64(len(msg.Key))

		resp.Value = rpcpb.EmptyRespBytes
		rpcpb.ReleaseDeleteRequest(msg)
	default:
		log.Fatalf("BUG: not supoprt rpctype: %d", rpcpb.Type(req.CustemType))
	}
}

func (kv *kvBatch) exec(s bhstorage.DataStorage, b *batch) error {
	return nil
}

func (kv *kvBatch) reset() {
}

func (kv *kvBatch) set(req *rpcpb.SetRequest, buf *goetty.ByteBuf, wb *util.WriteBatch) {
	wb.SetWithTTL(req.Key, appendValuePrefix(buf, req.Value, kvType), int32(req.TTL))
}

func (kv *kvBatch) delete(req *rpcpb.DeleteRequest, wb *util.WriteBatch) {
	wb.Delete(req.Key)
}
