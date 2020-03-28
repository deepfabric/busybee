package storage

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) init() {
	h.AddWriteFunc("setif", uint64(rpcpb.SetIf), h.setIf)
	h.AddWriteFunc("deleteif", uint64(rpcpb.DeleteIf), h.deleteIf)
	h.AddReadFunc("get", uint64(rpcpb.Get), h.get)
	h.AddReadFunc("scan", uint64(rpcpb.Scan), h.scan)
	h.AddWriteFunc("allocid", uint64(rpcpb.AllocID), h.allocID)
	h.AddWriteFunc("resetid", uint64(rpcpb.ResetID), h.resetID)
	h.AddWriteFunc("update-mapping", uint64(rpcpb.UpdateMapping), h.updateMapping)
	h.AddReadFunc("bm-contains", uint64(rpcpb.BMContains), h.bmcontains)
	h.AddReadFunc("bm-count", uint64(rpcpb.BMCount), h.bmcount)
	h.AddReadFunc("bm-range", uint64(rpcpb.BMRange), h.bmrange)

	h.AddWriteFunc("starting-instance", uint64(rpcpb.StartingInstance), h.startingWorkflowInstance)
	h.AddWriteFunc("update-instance", uint64(rpcpb.UpdateWorkflow), h.updateWorkflowDefinition)
	h.AddWriteFunc("started-instance", uint64(rpcpb.StartedInstance), h.workflowInstanceStarted)
	h.AddWriteFunc("stopping-instance", uint64(rpcpb.StopInstance), h.stopWorkflowInstance)
	h.AddWriteFunc("stopped-instance", uint64(rpcpb.StoppedInstance), h.workflowInstanceStopped)
	h.AddWriteFunc("create-state", uint64(rpcpb.CreateInstanceStateShard), h.createInstanceWorker)
	h.AddWriteFunc("update-state", uint64(rpcpb.UpdateInstanceStateShard), h.updateInstanceWorkerState)
	h.AddWriteFunc("remove-state", uint64(rpcpb.RemoveInstanceStateShard), h.removeInstanceWorker)
	h.AddWriteFunc("queue-join", uint64(rpcpb.QueueJoin), h.queueJoinGroup)
	h.AddWriteFunc("queue-concurrency-fetch", uint64(rpcpb.QueueFetch), h.queueFetch)

	h.runner.RunCancelableTask(h.handleShardCycle)
}

func (h *beeStorage) BuildRequest(req *raftcmdpb.Request, cmd interface{}) error {
	switch cmd.(type) {
	case *rpcpb.SetRequest:
		msg := cmd.(*rpcpb.SetRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Set)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseSetRequest(msg)
	case *rpcpb.SetIfRequest:
		msg := cmd.(*rpcpb.SetIfRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.SetIf)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseSetIfRequest(msg)
	case *rpcpb.GetRequest:
		msg := cmd.(*rpcpb.GetRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Get)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseGetRequest(msg)
	case *rpcpb.DeleteRequest:
		msg := cmd.(*rpcpb.DeleteRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.Delete)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseDeleteRequest(msg)
	case *rpcpb.DeleteIfRequest:
		msg := cmd.(*rpcpb.DeleteIfRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.DeleteIf)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseDeleteIfRequest(msg)
	case *rpcpb.ScanRequest:
		msg := cmd.(*rpcpb.ScanRequest)
		req.Key = msg.Start
		req.CustemType = uint64(rpcpb.Scan)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseScanRequest(msg)
	case *rpcpb.AllocIDRequest:
		msg := cmd.(*rpcpb.AllocIDRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.AllocID)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseAllocIDRequest(msg)
	case *rpcpb.ResetIDRequest:
		msg := cmd.(*rpcpb.ResetIDRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.ResetID)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseResetIDRequest(msg)
	case *rpcpb.BMCreateRequest:
		msg := cmd.(*rpcpb.BMCreateRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMCreate)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMCreateRequest(msg)
	case *rpcpb.BMAddRequest:
		msg := cmd.(*rpcpb.BMAddRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMAdd)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMAddRequest(msg)
	case *rpcpb.BMRemoveRequest:
		msg := cmd.(*rpcpb.BMRemoveRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMRemove)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMRemoveRequest(msg)
	case *rpcpb.BMClearRequest:
		msg := cmd.(*rpcpb.BMClearRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMClear)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMClearRequest(msg)
	case *rpcpb.BMContainsRequest:
		msg := cmd.(*rpcpb.BMContainsRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMContains)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMContainsRequest(msg)
	case *rpcpb.BMCountRequest:
		msg := cmd.(*rpcpb.BMCountRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMCount)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMCountRequest(msg)
	case *rpcpb.BMRangeRequest:
		msg := cmd.(*rpcpb.BMRangeRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.BMRange)
		req.Type = raftcmdpb.Read
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseBMRangeRequest(msg)
	case *rpcpb.StartingInstanceRequest:
		msg := cmd.(*rpcpb.StartingInstanceRequest)
		req.Key = WorkflowCurrentInstanceKey(msg.Instance.Snapshot.ID)
		req.CustemType = uint64(rpcpb.StartingInstance)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStartingInstanceRequest(msg)
	case *rpcpb.UpdateWorkflowRequest:
		msg := cmd.(*rpcpb.UpdateWorkflowRequest)
		req.Key = WorkflowCurrentInstanceKey(msg.Workflow.ID)
		req.CustemType = uint64(rpcpb.UpdateWorkflow)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
	case *rpcpb.StartedInstanceRequest:
		msg := cmd.(*rpcpb.StartedInstanceRequest)
		req.Key = WorkflowCurrentInstanceKey(msg.WorkflowID)
		req.CustemType = uint64(rpcpb.StartedInstance)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStartedInstanceRequest(msg)
	case *rpcpb.StopInstanceRequest:
		msg := cmd.(*rpcpb.StopInstanceRequest)
		req.Key = WorkflowCurrentInstanceKey(msg.WorkflowID)
		req.CustemType = uint64(rpcpb.StopInstance)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStopInstanceRequest(msg)
	case *rpcpb.StoppedInstanceRequest:
		msg := cmd.(*rpcpb.StoppedInstanceRequest)
		req.Key = WorkflowCurrentInstanceKey(msg.WorkflowID)
		req.CustemType = uint64(rpcpb.StoppedInstance)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseStoppedInstanceRequest(msg)
	case *rpcpb.CreateInstanceStateShardRequest:
		msg := cmd.(*rpcpb.CreateInstanceStateShardRequest)
		req.Key = InstanceShardKey(msg.State.WorkflowID, msg.State.Index)
		req.CustemType = uint64(rpcpb.CreateInstanceStateShard)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseCreateInstanceStateShardRequest(msg)
	case *rpcpb.UpdateInstanceStateShardRequest:
		msg := cmd.(*rpcpb.UpdateInstanceStateShardRequest)
		req.Key = InstanceShardKey(msg.State.WorkflowID, msg.State.Index)
		req.CustemType = uint64(rpcpb.UpdateInstanceStateShard)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseUpdateInstanceStateShardRequest(msg)
	case *rpcpb.RemoveInstanceStateShardRequest:
		msg := cmd.(*rpcpb.RemoveInstanceStateShardRequest)
		req.Key = InstanceShardKey(msg.WorkflowID, msg.Index)
		req.CustemType = uint64(rpcpb.RemoveInstanceStateShard)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseRemoveInstanceStateShardRequest(msg)
	case *rpcpb.QueueAddRequest:
		msg := cmd.(*rpcpb.QueueAddRequest)
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.QueueAdd)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseQueueAddRequest(msg)
	case *rpcpb.QueueFetchRequest:
		msg := cmd.(*rpcpb.QueueFetchRequest)
		if len(msg.Key) == 0 {
			msg.Key = PartitionKey(msg.ID, msg.Partition)
		}
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.QueueFetch)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseQueueFetchRequest(msg)
	case *rpcpb.QueueJoinGroupRequest:
		msg := cmd.(*rpcpb.QueueJoinGroupRequest)
		if len(msg.Key) == 0 {
			msg.Key = PartitionKey(msg.ID, 0)
		}
		req.Key = msg.Key
		req.CustemType = uint64(rpcpb.QueueJoin)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseQueueJoinGroupRequest(msg)
	case *rpcpb.UpdateMappingRequest:
		msg := cmd.(*rpcpb.UpdateMappingRequest)
		req.Key = MappingIDKey(msg.ID, msg.UserID)
		req.CustemType = uint64(rpcpb.UpdateMapping)
		req.Type = raftcmdpb.Write
		req.Cmd = protoc.MustMarshal(msg)
		rpcpb.ReleaseUpdateMappingRequest(msg)
	default:
		log.Fatalf("not support request %+v(%+T)", cmd, cmd)
	}

	return nil
}

func (h *beeStorage) Codec() (goetty.Decoder, goetty.Encoder) {
	return nil, nil
}

func (h *beeStorage) AddReadFunc(cmd string, cmdType uint64, cb raftstore.ReadCommandFunc) {
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *beeStorage) AddWriteFunc(cmd string, cmdType uint64, cb raftstore.WriteCommandFunc) {
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *beeStorage) AddLocalFunc(cmd string, cmdType uint64, cb raftstore.LocalCommandFunc) {
	h.store.RegisterLocalFunc(cmdType, cb)
}

func (h *beeStorage) WriteBatch() raftstore.CommandWriteBatch {
	return newBatch(h, newKVBatch(), newBitmapBatch(), newQueueBatch())
}

func (h *beeStorage) getValue(shard uint64, key []byte) ([]byte, error) {
	value, err := h.getStore(shard).Get(key)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}

	return value, nil
}
