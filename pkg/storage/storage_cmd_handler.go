package storage

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) init() {
	h.AddReadFunc("get", uint64(rpcpb.Get), h.get)
	h.AddReadFunc("bmcontains", uint64(rpcpb.BMContains), h.bmcontains)
	h.AddReadFunc("bmcount", uint64(rpcpb.BMCount), h.bmcount)
	h.AddReadFunc("bmrange", uint64(rpcpb.BMRange), h.bmrange)

	h.AddWriteFunc("startwf", uint64(rpcpb.StartWF), h.startWF)
	h.AddWriteFunc("removewf", uint64(rpcpb.RemoveWF), h.removeWF)
	h.AddWriteFunc("createstate", uint64(rpcpb.CreateState), h.createState)
	h.AddWriteFunc("updatestate", uint64(rpcpb.UpdateState), h.updateState)
	h.AddWriteFunc("removestate", uint64(rpcpb.RemoveState), h.removeState)
	h.AddWriteFunc("queueadd", uint64(rpcpb.QueueAdd), h.queueAdd)
	h.AddWriteFunc("queuefetch", uint64(rpcpb.QueueFetch), h.queueFetch)

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
		req.Key = InstanceStartKey(msg.Instance.ID)
		req.CustemType = uint64(rpcpb.StartWF)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStartWFRequest(msg)
	case *rpcpb.RemoveWFRequest:
		msg := cmd.(*rpcpb.RemoveWFRequest)
		msg.ID = req.ID
		req.Key = InstanceStartKey(msg.InstanceID)
		req.CustemType = uint64(rpcpb.RemoveWF)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseRemoveWFRequest(msg)
	case *rpcpb.CreateStateRequest:
		msg := cmd.(*rpcpb.CreateStateRequest)
		msg.ID = req.ID
		req.Key = InstanceStateKey(msg.State.InstanceID, msg.State.Start, msg.State.End)
		req.CustemType = uint64(rpcpb.CreateState)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseCreateStateRequest(msg)
	case *rpcpb.UpdateStateRequest:
		msg := cmd.(*rpcpb.UpdateStateRequest)
		msg.ID = req.ID
		req.Key = InstanceStateKey(msg.State.InstanceID, msg.State.Start, msg.State.End)
		req.CustemType = uint64(rpcpb.CreateState)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseUpdateStateRequest(msg)
	case *rpcpb.RemoveStateRequest:
		msg := cmd.(*rpcpb.RemoveStateRequest)
		msg.ID = req.ID
		req.Key = InstanceStateKey(msg.InstanceID, msg.Start, msg.End)
		req.CustemType = uint64(rpcpb.CreateState)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseRemoveStateRequest(msg)
	case *rpcpb.QueueAddRequest:
		msg := cmd.(*rpcpb.QueueAddRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.QueueAdd)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseQueueAddRequest(msg)
	case *rpcpb.QueueFetchRequest:
		msg := cmd.(*rpcpb.QueueFetchRequest)
		msg.ID = req.ID
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.QueueFetch)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseQueueFetchRequest(msg)
	default:
		log.Fatalf("not support request %+v(%+T)", cmd, cmd)
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
		bm := util.MustParseBM(value)
		bm2 := util.AcquireBitmap()
		bm2.AddMany(customReq.Value)
		contains = util.BMAnd(bm, bm2).GetCardinality() == uint64(len(customReq.Value))
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
		count = util.MustParseBM(value).GetCardinality()
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
	err = h.getStore(shard).Set(req.Key, appendPrefix(value, stateType))
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

func (h *beeStorage) updateState(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	updateState := rpcpb.UpdateStateRequest{}
	protoc.MustUnmarshal(&updateState, req.Cmd)

	customResp := rpcpb.AcquireUpdateStateResponse()
	customResp.ID = updateState.ID
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseUpdateStateResponse(customResp)

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", updateState, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	oldState := metapb.WorkflowInstanceState{}
	protoc.MustUnmarshal(&oldState, value)
	if oldState.Version >= updateState.State.Version {
		return 0, 0, resp
	}

	value = protoc.MustMarshal(&updateState.State)
	err = h.getStore(shard).Set(req.Key, appendPrefix(value, stateType))
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", updateState, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStateUpdatedEvent,
			Data:      updateState.State,
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

func (h *beeStorage) queueAdd(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueAdd := rpcpb.QueueAddRequest{}
	protoc.MustUnmarshal(&queueAdd, req.Cmd)

	customResp := rpcpb.AcquireQueueAddResponse()
	customResp.ID = queueAdd.ID

	var lastOffset uint64
	offsetKey := queueLastOffsetKey(req.Key)
	data, err := h.getStore(shard).Get(offsetKey)
	if err != nil {
		log.Fatalf("loading last offset failed with %+v", err)
	}
	if len(data) > 0 {
		lastOffset = goetty.Byte2UInt64(data)
	}

	var writtenBytes uint64
	driver := h.getStore(shard)
	wb := driver.NewWriteBatch()
	for _, item := range queueAdd.Items {
		lastOffset++
		key := queueItemKey(req.Key, lastOffset)
		wb.Set(key, item)
		writtenBytes += uint64(len(key) + len(item))
	}
	wb.Set(offsetKey, goetty.Uint64ToBytes(lastOffset))

	err = driver.Write(wb, false)
	if err != nil {
		log.Fatalf("queue add failed with %+v", err)
	}

	customResp.LastOffset = lastOffset
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseQueueAddResponse(customResp)
	return writtenBytes, int64(writtenBytes), resp
}

func (h *beeStorage) queueFetch(shard uint64, req *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueFetch := rpcpb.QueueFetchRequest{}
	protoc.MustUnmarshal(&queueFetch, req.Cmd)

	customResp := rpcpb.AcquireQueueFetchResponse()
	customResp.ID = queueFetch.ID

	offset := queueFetch.AfterOffset
	from := queueItemKey(req.Key, offset+1)
	to := queueItemKey(req.Key, offset+1+uint64(queueFetch.Count))

	err := h.getStore(shard).RangeDelete(queueItemKey(req.Key, 0), from)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	err = h.getStore(shard).Scan(from, to, func(key, value []byte) (bool, error) {
		offset++
		customResp.Items = append(customResp.Items, value)
		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("fetch queue failed with %+v", err)
	}

	customResp.LastOffset = offset
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseQueueFetchResponse(customResp)
	return 0, 0, resp
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
