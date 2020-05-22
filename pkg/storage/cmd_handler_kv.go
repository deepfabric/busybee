package storage

import (
	"bytes"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) setIf(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := getSetIfRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("set id %+v failed with %+v", req.Key, err)
	}

	if !matchConditionGroups(value, customReq.Conditions) {
		resp.Value = rpcpb.FalseRespBytes
		return 0, 0, resp
	}

	err = h.getStore(shard.ID).SetWithTTL(req.Key, customReq.Value,
		int32(customReq.TTL))
	if err != nil {
		log.Fatalf("set id %+v failed with %+v", req.Key, err)
	}

	resp.Value = rpcpb.TrueRespBytes
	written := uint64(0)
	if len(value) > 0 {
		written += uint64(len(req.Key) + len(customReq.Value))
	}
	return written, int64(written), resp
}

func (h *beeStorage) deleteIf(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := getDeleteIfRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("delete if %+v failed with %+v", req.Key, err)
	}

	if !matchConditionGroups(value, customReq.Conditions) {
		resp.Value = rpcpb.FalseRespBytes
		return 0, 0, resp
	}

	err = h.getStore(shard.ID).Delete(req.Key)
	if err != nil {
		log.Fatalf("delete id %+v failed with %+v", req.Key, err)
	}

	resp.Value = rpcpb.TrueRespBytes
	written := uint64(0)
	if len(value) > 0 {
		written += uint64(len(req.Key) + len(value))
	}
	return written, -int64(written), resp
}

func (h *beeStorage) allocID(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := getAllocIDRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

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

	customResp := getUint32RangeResponse(attrs)
	customResp.From = start
	customResp.To = end
	resp.Value = protoc.MustMarshal(customResp)

	written := uint64(len(req.Key)) + 4
	return written, int64(written), resp
}

func (h *beeStorage) resetID(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := getResetIDRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	value := 0 + customReq.StartWith
	err := h.getStore(shard.ID).Set(req.Key, format.Uint32ToBytes(uint32(value)))
	if err != nil {
		log.Fatalf("alloc id %+v failed with %+v", req.Key, err)
	}

	resp.Value = rpcpb.EmptyRespBytes

	written := uint64(len(req.Key)) + 4
	return written, int64(written), resp
}

func (h *beeStorage) scan(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := &raftcmdpb.Response{}
	customReq := getScanRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	buf.MarkWrite()
	buf.Write(req.Key[0:9])
	buf.Write(customReq.End)
	end := buf.WrittenDataAfterMark()

	if len(shard.End) > 0 {
		buf.MarkWrite()
		buf.Write(req.Key[0:9])
		buf.Write(shard.End)
		max := buf.WrittenDataAfterMark()

		if bytes.Compare(end.Data(), max.Data()) > 0 {
			end = max
		}
	}

	var keys []goetty.Slice
	var values []goetty.Slice
	customResp := getBytesSliceResponse(attrs)
	err := h.getStore(shard.ID).Scan(req.Key, end.Data(), func(key, value []byte) (bool, error) {
		if !allowWriteToBuf(buf, len(value)) {
			log.Warningf("scan skipped, buf cap %d, write at %d, value %d",
				buf.Capacity(),
				buf.GetWriteIndex(),
				len(value))
			return false, nil
		}

		if uint64(len(keys)) >= customReq.Limit {
			return false, nil
		}

		buf.MarkWrite()
		buf.Write(raftstore.DecodeDataKey(key))
		keys = append(keys, buf.WrittenDataAfterMark())

		buf.MarkWrite()
		buf.Write(value)
		values = append(values, buf.WrittenDataAfterMark())
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("scan %+v failed with %+v", req.Key, err)
	}

	if len(keys) > 0 {
		for idx := range keys {
			customResp.Keys = append(customResp.Keys, keys[idx].Data())
			customResp.Values = append(customResp.Values, values[idx].Data())
		}
	}

	resp.Value = protoc.MustMarshal(customResp)
	return resp
}

func (h *beeStorage) bmcontains(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := getBMContainsRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	contains := false
	if len(value) > 0 {
		bm := util.MustParseBM(value)
		bm2 := getTempBM(attrs)
		bm2.AddMany(customReq.Value)
		bm.And(bm2)
		contains = bm.GetCardinality() == uint64(len(customReq.Value))
	}

	customResp := getBoolResponse(attrs)
	customResp.Value = contains
	resp.Value = protoc.MustMarshal(customResp)
	return resp
}

func (h *beeStorage) bmcount(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := getBMCountRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	count := uint64(0)
	if len(value) > 0 {
		count = util.MustParseBM(value).GetCardinality()
	}

	customResp := getUint64Response(attrs)
	customResp.Value = count
	resp.Value = protoc.MustMarshal(customResp)
	return resp
}

func (h *beeStorage) bmrange(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := getBMRangeRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

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

	customResp := getUint32SliceResponse(attrs)
	customResp.Values = values
	resp.Value = protoc.MustMarshal(customResp)
	return resp
}

func (h *beeStorage) updateMapping(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := getUpdateMappingRequest(attrs)
	protoc.MustUnmarshal(customReq, req.Cmd)

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
