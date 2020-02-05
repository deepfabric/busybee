package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) queueFetch(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueFetch := rpcpb.QueueFetchRequest{}
	protoc.MustUnmarshal(&queueFetch, req.Cmd)

	customResp := rpcpb.AcquireBytesSliceResponse()
	offset := queueFetch.AfterOffset
	if offset == 0 {
		value, err := h.getStore(shard.ID).Get(committedOffsetKey(req.Key, queueFetch.Consumer))
		if err != nil {
			log.Fatalf("get consumer committed offset failed with %+v", err)
		}
		if len(value) > 0 {
			offset = goetty.Byte2UInt64(value)
		}
	}

	from := itemKey(req.Key, offset+1)
	to := itemKey(req.Key, offset+1+uint64(queueFetch.Count))

	idx := buf.GetWriteIndex()
	buf.WriteUInt64(offset)
	buf.WriteInt64(time.Now().Unix())
	err := h.getStore(shard.ID).Set(committedOffsetKey(req.Key, queueFetch.Consumer),
		buf.RawBuf()[idx:buf.GetWriteIndex()])
	if err != nil {
		log.Fatalf("set consumer committed offset failed with %+v", err)
	}

	err = h.getStore(shard.ID).Scan(from, to, func(key, value []byte) (bool, error) {
		offset++
		customResp.Values = append(customResp.Values, value)
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	customResp.LastValue = offset
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesSliceResponse(customResp)
	return 0, 0, resp
}
