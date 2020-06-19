package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	bhutil "github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) updateTenantInitState(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := &rpcpb.TenantInitStateUpdateRequest{}
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("update tenant init state %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		log.Fatalf("update tenant init state %+v failed with missing tenant metadata", cmd)
	}

	metadata := &metapb.Tenant{}
	protoc.MustUnmarshal(metadata, value)

	switch cmd.Group {
	case metapb.TenantInputGroup:
		metadata.InputsState[cmd.Index] = true
	case metapb.TenantOutputGroup:
		metadata.OutputsState[cmd.Index] = true
	case metapb.TenantRunnerGroup:
		metadata.RunnersState[cmd.Index] = true
	}

	err = h.getStoreByGroup(shard.Group).Set(req.Key, protoc.MustMarshal(metadata))
	if err != nil {
		log.Fatalf("update tenant init state %+v failed with %+v", cmd, err)
	}

	return 0, 0, resp
}

func (h *beeStorage) startingWorkflowInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStartingInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		old := getTempWorkflowInstance(attrs)
		protoc.MustUnmarshal(old, value)

		if old.State != metapb.Stopped {
			return 0, 0, resp
		}
	}

	cmd.Instance.State = metapb.Starting
	value = protoc.MustMarshal(&cmd.Instance)
	err = h.getStoreByGroup(shard.Group).Set(req.Key, value)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		instance := &metapb.WorkflowInstance{}
		protoc.MustUnmarshal(instance, value)

		h.eventC <- Event{
			EventType: StartingInstanceEvent,
			Data:      instance,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) updateWorkflowDefinition(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getUpdateWorkflowRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	old := getTempWorkflowInstance(attrs)
	protoc.MustUnmarshal(old, value)

	if old.State != metapb.Running {
		return 0, 0, resp
	}

	old.Snapshot = cmd.Workflow
	old.Version++
	err = h.getStoreByGroup(shard.Group).Set(req.Key, protoc.MustMarshal(old))
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.Workflow.ID, err)
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) workflowInstanceStarted(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStartedInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("get workflow instance %d failed with %+v",
			cmd.WorkflowID, err)
	}
	if len(value) == 0 {
		log.Fatalf("missing workflow instance %d",
			cmd.WorkflowID)
	}

	old := getTempWorkflowInstance(attrs)
	protoc.MustUnmarshal(old, value)
	if old.State != metapb.Starting {
		return 0, 0, resp
	}

	old.StartedAt = time.Now().Unix()
	old.State = metapb.Running
	value = protoc.MustMarshal(old)
	err = h.getStoreByGroup(shard.Group).Set(req.Key, value)
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		instance := &metapb.WorkflowInstance{}
		protoc.MustUnmarshal(instance, value)

		h.eventC <- Event{
			EventType: RunningInstanceEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) stopWorkflowInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStopInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("get workflow instance %d failed with %+v",
			cmd.WorkflowID, err)
	}
	if len(value) == 0 {
		log.Fatalf("missing workflow instance %d", cmd.WorkflowID)
	}

	old := getTempWorkflowInstance(attrs)
	protoc.MustUnmarshal(old, value)
	if old.State != metapb.Running {
		return 0, 0, resp
	}

	old.State = metapb.Stopping
	value = protoc.MustMarshal(old)
	err = h.getStoreByGroup(shard.Group).Set(req.Key, value)
	if err != nil {
		log.Fatalf("set workflow instance %d stopped failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		instance := &metapb.WorkflowInstance{}
		protoc.MustUnmarshal(instance, value)

		h.eventC <- Event{
			EventType: StoppingInstanceEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) workflowInstanceStopped(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStoppedInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("stopped workflow instance %d failed with %+v",
			cmd.WorkflowID, err)
	}
	if len(value) == 0 {
		log.Fatalf("missing workflow instance %d", cmd.WorkflowID)
	}

	old := getTempWorkflowInstance(attrs)
	protoc.MustUnmarshal(old, value)
	if old.State != metapb.Stopping {
		return 0, 0, resp
	}

	old.State = metapb.Stopped
	old.StoppedAt = time.Now().Unix()
	err = h.getStoreByGroup(shard.Group).Set(req.Key, protoc.MustMarshal(old))
	if err != nil {
		log.Fatalf("set workflow instance %d stopped failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: StoppedInstanceEvent,
			Data:      old.Snapshot.ID,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

// in group TenantRunnerGroup, disable split
func (h *beeStorage) createInstanceWorker(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getCreateInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.TrueRespBytes

	key := runnerKey(cmd.State.TenantID, cmd.State.Runner)
	runner := h.loadWorkerRunnerByGroup(shard.Group, key)
	if runner.State == metapb.WRStopped {
		resp.Value = rpcpb.FalseRespBytes
		return 0, 0, resp
	}

	for _, wr := range runner.Workers {
		if wr.WorkflowID == cmd.State.WorkflowID &&
			wr.InstanceID == cmd.State.InstanceID &&
			wr.Index == cmd.State.Index {
			return 0, 0, resp
		}
	}

	runner.Workers = append(runner.Workers, metapb.WorkflowWorker{
		WorkflowID: cmd.State.WorkflowID,
		InstanceID: cmd.State.InstanceID,
		Index:      cmd.State.Index,
	})

	value := protoc.MustMarshal(&cmd.State)
	wb := bhutil.NewWriteBatch()
	wb.Set(req.Key, value)
	wb.Set(key, protoc.MustMarshal(runner))
	err := h.getStoreByGroup(shard.Group).Write(wb, false)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if h.store.MaybeLeader(shard.ID) {
		state := metapb.WorkflowInstanceWorkerState{}
		protoc.MustUnmarshal(&state, value)

		h.eventC <- Event{
			EventType: InstanceWorkerCreatedEvent,
			Data:      state,
		}
	}

	return 0, 0, resp
}

func (h *beeStorage) updateInstanceWorkerState(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getUpdateInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueByGroup(shard.Group, req.Key)
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	err = h.getStoreByGroup(shard.Group).Set(req.Key, protoc.MustMarshal(&cmd.State))
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}

	return 0, 0, resp
}

func (h *beeStorage) removeInstanceWorker(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getRemoveInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	key := runnerKey(cmd.TenantID, cmd.Runner)
	runner := h.loadWorkerRunnerByGroup(shard.Group, key)
	if runner.State == metapb.WRStopped {
		log.Fatalf("BUG: remove a instance shard from a stopped worker runner")
	}

	var newWorkers []metapb.WorkflowWorker
	for _, wr := range runner.Workers {
		if wr.WorkflowID == cmd.WorkflowID &&
			wr.InstanceID == cmd.InstanceID &&
			wr.Index == cmd.Index {
			continue
		}

		newWorkers = append(newWorkers, wr)
	}

	if len(newWorkers) == len(runner.Workers) {
		return 0, 0, resp
	}

	runner.Workers = newWorkers
	wb := bhutil.NewWriteBatch()
	wb.Delete(req.Key)
	wb.Set(key, protoc.MustMarshal(runner))
	err := h.getStoreByGroup(shard.Group).Write(wb, false)
	if err != nil {
		log.Fatalf("remove workflow instance state %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceWorkerDestoriedEvent,
			Data: metapb.WorkflowInstanceWorkerState{
				TenantID:   cmd.TenantID,
				WorkflowID: cmd.WorkflowID,
				InstanceID: cmd.InstanceID,
				Index:      cmd.Index,
				Runner:     cmd.Runner,
			},
		}
	}
	return 0, 0, resp
}

func (h *beeStorage) loadWorkerRunnerByGroup(group uint64, key []byte) *metapb.WorkerRunner {
	data, err := h.getValueByGroup(group, key)
	if err != nil {
		log.Fatalf("load worker runner %+v failed with %+v",
			key,
			err)
	}

	if len(data) == 0 {
		log.Fatalf("BUG: missing worker runner %+v",
			key)
	}

	value := &metapb.WorkerRunner{}
	protoc.MustUnmarshal(value, data)

	return value
}

func runnerKey(tid uint64, runner uint64) []byte {
	return raftstore.EncodeDataKey(uint64(metapb.TenantRunnerGroup),
		TenantRunnerMetadataKey(tid, runner))
}
