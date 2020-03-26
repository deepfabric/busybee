package storage

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
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

func (kv *kvBatch) addReq(req *raftcmdpb.Request, resp *raftcmdpb.Response, b *batch, attrs map[string]interface{}) {
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)

	switch rpcpb.Type(req.CustemType) {
	case rpcpb.Set:
		msg := getSetRequest(attrs)
		protoc.MustUnmarshal(msg, req.Cmd)
		msg.Key = req.Key

		kv.set(msg, buf, b.wb)

		b.writtenBytes += uint64(len(msg.Key) + len(msg.Value))
		b.changedBytes += int64(len(msg.Key) + len(msg.Value))

		resp.Value = rpcpb.EmptyRespBytes
	case rpcpb.Delete:
		msg := getDeleteRequest(attrs)
		msg.Key = req.Key
		kv.delete(msg, b.wb)

		b.writtenBytes += uint64(len(msg.Key))
		b.changedBytes -= int64(len(msg.Key))

		resp.Value = rpcpb.EmptyRespBytes
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
	wb.SetWithTTL(req.Key, req.Value, int32(req.TTL))
}

func (kv *kvBatch) delete(req *rpcpb.DeleteRequest, wb *util.WriteBatch) {
	wb.Delete(req.Key)
}
