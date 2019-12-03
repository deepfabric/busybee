package storage

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/pilosa/pilosa/roaring"
)

func (h *beeStorage) init() {
	h.AddReadFunc("get", uint64(rpcpb.Get), h.get)
	h.AddReadFunc("bmcontains", uint64(rpcpb.BMContains), h.bmcontains)
	h.AddReadFunc("bmcount", uint64(rpcpb.BMCount), h.bmcount)
	h.AddReadFunc("bmrange", uint64(rpcpb.BMRange), h.bmrange)

	h.AddWriteFunc("startwf", uint64(rpcpb.StartWF), h.startWF)
	h.AddWriteFunc("removewf", uint64(rpcpb.RemoveWF), h.removeWF)
	h.AddWriteFunc("createstate", uint64(rpcpb.CreateState), h.createState)
	h.AddWriteFunc("removestate", uint64(rpcpb.RemoveState), h.removeState)

	h.runner.RunCancelableTask(h.handleShardCycle)
}

func (h *beeStorage) BuildRequest(req *raftcmdpb.Request, cmd interface{}) error {
	switch cmd.(type) {
	case *rpcpb.SetRequest:
		msg := cmd.(*rpcpb.SetRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Set)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseSetRequest(msg)
	case *rpcpb.GetRequest:
		msg := cmd.(*rpcpb.GetRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Get)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseGetRequest(msg)
	case *rpcpb.DeleteRequest:
		msg := cmd.(*rpcpb.DeleteRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Delete)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseDeleteRequest(msg)
	case *rpcpb.BMCreateRequest:
		msg := cmd.(*rpcpb.BMCreateRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMCreate)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMCreateRequest(msg)
	case *rpcpb.BMAddRequest:
		msg := cmd.(*rpcpb.BMAddRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMAdd)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMAddRequest(msg)
	case *rpcpb.BMRemoveRequest:
		msg := cmd.(*rpcpb.BMRemoveRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMRemove)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMRemoveRequest(msg)
	case *rpcpb.BMClearRequest:
		msg := cmd.(*rpcpb.BMClearRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMClear)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMClearRequest(msg)
	case *rpcpb.BMContainsRequest:
		msg := cmd.(*rpcpb.BMContainsRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMContains)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMContainsRequest(msg)
	case *rpcpb.BMDelRequest:
		msg := cmd.(*rpcpb.BMDelRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMDel)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMDelRequest(msg)
	case *rpcpb.BMCountRequest:
		msg := cmd.(*rpcpb.BMCountRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMCount)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMCountRequest(msg)
	case *rpcpb.BMRangeRequest:
		msg := cmd.(*rpcpb.BMRangeRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMRange)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMRangeRequest(msg)
	case *rpcpb.StartWFRequest:
		msg := cmd.(*rpcpb.StartWFRequest)
		msg.ID = req.ID
		req.Key = goetty.Uint64ToBytes(msg.Instance.ID)
		req.CustemType = uint64(rpcpb.StartWF)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStartWFRequest(msg)
	case *rpcpb.RemoveWFRequest:
		msg := cmd.(*rpcpb.RemoveWFRequest)
		msg.ID = req.ID
		req.Key = goetty.Uint64ToBytes(msg.InstanceID)
		req.CustemType = uint64(rpcpb.StartWF)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseRemoveWFRequest(msg)
	}

	return nil
}

func (h *beeStorage) Codec() (goetty.Decoder, goetty.Encoder) {
	return decoder, encoder
}

func (h *beeStorage) AddReadFunc(cmd string, cmdType uint64, cb raftstore.ReadCommandFunc) {
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *beeStorage) AddWriteFunc(cmd string, cmdType uint64, cb raftstore.WriteCommandFunc) {
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *beeStorage) WriteBatch() raftstore.CommandWriteBatch {
	b := acquireBatch()
	b.Reset()
	b.h = h
	return b
}

func (h *beeStorage) get(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.GetRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	customResp := rpcpb.AcquireGetResponse()
	customResp.ID = customReq.ID
	customResp.Value = value
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseGetResponse(customResp)
	return resp
}

func (h *beeStorage) bmcontains(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMContainsRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	contains := false
	if len(value) > 0 {
		contains = true
		bm := mustParseBitmap(value)
		for _, id := range customReq.Value {
			if !bm.Contains(id) {
				contains = false
				break
			}
		}
		releaseBitmap(bm)
	}

	customResp := rpcpb.AcquireBMContainsResponse()
	customResp.ID = customReq.ID
	customResp.Contains = contains
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBMContainsResponse(customResp)
	return resp
}

func (h *beeStorage) bmcount(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMCountRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	count := uint64(0)
	if len(value) > 0 {
		bm := mustParseBitmap(value)
		count = bm.Count()
		releaseBitmap(bm)
	}

	customResp := rpcpb.AcquireBMCountResponse()
	customResp.ID = customReq.ID
	customResp.Count = count
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBMCountResponse(customResp)
	return resp
}

func (h *beeStorage) bmrange(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	customReq := rpcpb.BMRangeRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("get %+v failed with %+v", req.Key, err)
	}

	var values []uint64
	if len(value) > 0 {
		bm := mustParseBitmap(value)
		count := uint64(0)
		itr := bm.Iterator()
		itr.Seek(customReq.Start)
		for {
			value, eof := itr.Next()
			if eof {
				break
			}

			values = append(values, value)
			count++

			if count >= customReq.Limit {
				break
			}
		}
		releaseBitmap(bm)
	}

	customResp := rpcpb.AcquireBMRangeResponse()
	customResp.ID = customReq.ID
	customResp.Values = values
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseBMRangeResponse(customResp)
	return resp
}

func (h *beeStorage) startWF(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	startWF := rpcpb.StartWFRequest{}
	protoc.MustUnmarshal(&startWF, req.Cmd)

	customResp := rpcpb.AcquireStartWFResponse()
	customResp.ID = startWF.ID
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseStartWFResponse(customResp)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", startWF, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	value = protoc.MustMarshal(&startWF.Instance)
	err = h.getStore(shard).Set(req.Key, appendPrefix(value, instanceType))
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", startWF, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceLoadedEvent,
			Data:      startWF.Instance,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) removeWF(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	removeWF := rpcpb.RemoveWFRequest{}
	protoc.MustUnmarshal(&removeWF, req.Cmd)

	customResp := rpcpb.AcquireRemoveWFResponse()
	customResp.ID = removeWF.ID
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseRemoveWFResponse(customResp)

	err := h.getStore(shard).Delete(req.Key)
	if err != nil {
		log.Fatalf("remove workflow instance %d failed with %+v", removeWF.ID, err)
	}

	return uint64(len(req.Key)), -int64(len(req.Key)), resp
}

func (h *beeStorage) createState(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	createState := rpcpb.CreateStateRequest{}
	protoc.MustUnmarshal(&createState, req.Cmd)

	customResp := rpcpb.AcquireCreateStateResponse()
	customResp.ID = createState.ID
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseCreateStateResponse(customResp)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("create workflow instance state %+v failed with %+v", createState, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	value = protoc.MustMarshal(&createState.State)
	err = h.getStore(shard).Set(req.Key, appendPrefix(req.Cmd, stateType))
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", createState, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStateLoadedEvent,
			Data:      createState.State,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) removeState(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	removeState := rpcpb.RemoveStateRequest{}
	protoc.MustUnmarshal(&removeState, req.Cmd)

	customResp := rpcpb.AcquireRemoveStateResponse()
	customResp.ID = removeState.ID
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseRemoveStateResponse(customResp)

	err := h.getStore(shard).Delete(req.Key)
	if err != nil {
		log.Fatalf("remove workflow instance state %d failed with %+v", removeState.ID, err)
	}

	return uint64(len(req.Key)), -int64(len(req.Key)), resp
}

func (h *beeStorage) getValue(shard uint64, key []byte) ([]byte, error) {
	value, err := h.getStore(shard).Get(key)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}
	return value[1:], nil
}

func mustParseBitmap(value []byte) *roaring.Bitmap {
	bm := acquireBitmap()
	bm.Containers.Reset()
	_, _, err := bm.ImportRoaringBits(value, false, false, 0)
	if err != nil {
		log.Fatalf("BUG: parse bitmap failed with %+v", err)
	}
	return bm
}
