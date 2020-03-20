package storage

import (
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

	key := committedOffsetKey(req.Key, queueFetch.Consumer, buf)
	store := h.getStore(shard.ID)

	completed := queueFetch.CompletedOffset
	value, err := store.Get(key)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	saveCompleted := queueFetch.CompletedOffset > 0
	if len(value) > 0 {
		alreadyCompleted := goetty.Byte2UInt64(value)
		if completed <= alreadyCompleted {
			completed = alreadyCompleted
			saveCompleted = false
		}
	}

	if saveCompleted {
		buf.MarkWrite()
		buf.WriteUint64(queueFetch.CompletedOffset)
		err = store.Set(key, buf.WrittenDataAfterMark())
		if err != nil {
			log.Fatalf("set consumer committed offset failed with %+v", err)
		}
	}

	startKey := itemKey(req.Key, completed+1, buf)
	endKey := itemKey(req.Key, completed+1+queueFetch.Count, buf)

	start := completed + 1
	end := start
	var items [][]byte
	err = h.getStore(shard.ID).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
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
	customResp.Values = items
	customResp.LastValue = end - 1
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesSliceResponse(customResp)
	return 0, 0, resp
}
