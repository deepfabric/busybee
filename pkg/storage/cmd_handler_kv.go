package storage

import (
	"bytes"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) allocID(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := rpcpb.AllocIDRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("alloc id %+v failed with %+v", req.Key, err)
	}

	id := uint32(0)
	if len(value) > 0 {
		id, err = format.BytesToUint32(value)
		if err != nil {
			log.Fatalf("alloc id %+v failed with %+v", req.Key, err)
		}
	}

	start := id + 1
	end := id + uint32(customReq.Batch)

	err = h.getStore(shard.ID).Set(req.Key, format.Uint32ToBytes(end))
	if err != nil {
		log.Fatalf("alloc id %+v failed with %+v", req.Key, err)
	}

	customResp := rpcpb.AcquireUint32RangeResponse()
	customResp.From = start
	customResp.To = end
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseUint32RangeResponse(customResp)

	written := uint64(len(req.Key)) + 4
	return written, int64(written), resp
}

func (h *beeStorage) resetID(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := rpcpb.ResetIDRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value := 0 + customReq.StartWith
	err := h.getStore(shard.ID).Set(req.Key, format.Uint32ToBytes(uint32(value)))
	if err != nil {
		log.Fatalf("alloc id %+v failed with %+v", req.Key, err)
	}

	resp.Value = rpcpb.EmptyRespBytes

	written := uint64(len(req.Key)) + 4
	return written, int64(written), resp
}

func (h *beeStorage) get(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.GetRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	customResp := rpcpb.AcquireBytesResponse()
	customResp.Value = value
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesResponse(customResp)
	return resp
}

func (h *beeStorage) scan(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.ScanRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	prefix := req.Key[0 : len(req.Key)-len(customReq.End)]
	buf.MarkWrite()
	if len(prefix) > 0 {
		buf.Write(prefix)
		buf.Write(customReq.End)
	}
	end := buf.WrittenDataAfterMark()

	if len(shard.End) > 0 {
		buf.MarkWrite()
		if len(prefix) > 0 {
			buf.Write(prefix)
			buf.Write(shard.End)
		}
		max := buf.WrittenDataAfterMark()

		if bytes.Compare(end, max) > 0 {
			end = max
		}
	}

	customResp := rpcpb.AcquireBytesSliceResponse()
	err := h.getStore(shard.ID).Scan(req.Key, end, func(key, value []byte) (bool, error) {
		customResp.Values = append(customResp.Values, value)
		if uint64(len(customResp.Values)) >= customReq.Limit {
			return false, nil
		}
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("scan %+v failed with %+v", req.Key, err)
	}

	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesSliceResponse(customResp)
	return resp
}

func (h *beeStorage) bmcontains(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMContainsRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	contains := false
	if len(value) > 0 {
		bm := util.MustParseBM(value)
		bm2 := util.AcquireBitmap()
		bm2.AddMany(customReq.Value)
		contains = util.BMAnd(bm, bm2).GetCardinality() == uint64(len(customReq.Value))
	}

	customResp := rpcpb.AcquireBoolResponse()
	customResp.Value = contains
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBoolResponse(customResp)
	return resp
}

func (h *beeStorage) bmcount(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMCountRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	count := uint64(0)
	if len(value) > 0 {
		count = util.MustParseBM(value).GetCardinality()
	}

	customResp := rpcpb.AcquireUint64Response()
	customResp.Value = count
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseUint64Response(customResp)
	return resp
}

func (h *beeStorage) bmrange(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMRangeRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	var values []uint32
	if len(value) > 0 {
		bm := util.MustParseBM(value)
		if bm.GetCardinality() > 0 {
			count := uint64(0)
			itr := bm.Iterator()
			itr.AdvanceIfNeeded(customReq.Start)
			for {
				if !itr.HasNext() {
					break
				}

				values = append(values, itr.Next())
				count++

				if count >= customReq.Limit {
					break
				}
			}
		}
	}

	customResp := rpcpb.AcquireUint32SliceResponse()
	customResp.Values = values
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseUint32SliceResponse(customResp)
	return resp
}

func (h *beeStorage) updateMapping(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := rpcpb.UpdateMappingRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	set := customReq.Set
	data, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("update mapping failed with %+v", err)
	}

	written := uint64(0)
	if len(data) > 0 {
		set.Values = nil
		protoc.MustUnmarshal(&set, data)

		for i := range customReq.Set.Values {
			found := false
			for j := range set.Values {
				if customReq.Set.Values[i].Type == set.Values[j].Type {
					set.Values[j].Value = customReq.Set.Values[i].Value
					found = true
					break
				}
			}

			if !found {
				set.Values = append(set.Values, customReq.Set.Values[i])
			}
		}
	}

	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(&set))
	if err != nil {
		log.Fatalf("set mapping id failed with %+v", err)
	}

	written += uint64(len(req.Key)) + uint64(set.Size())
	resp.Value = protoc.MustMarshal(&set)
	return written, int64(written), resp
}
