package storage

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) get(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.GetRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	customResp := rpcpb.AcquireBytesResponse()
	customResp.Value = value
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBytesResponse(customResp)
	return resp
}

func (h *beeStorage) bmcontains(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMContainsRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
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

func (h *beeStorage) bmcount(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMCountRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
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

func (h *beeStorage) bmrange(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMRangeRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	var values []uint32
	if len(value) > 0 {
		bm := util.MustParseBM(value)
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

	customResp := rpcpb.AcquireUint32SliceResponse()
	customResp.Values = values
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseUint32SliceResponse(customResp)
	return resp
}
