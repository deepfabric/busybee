package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) startingWorkflowInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStartingInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
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
	err = h.getStore(shard.ID).Set(req.Key, value)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: StartingInstanceEvent,
			Data:      &cmd.Instance,
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

	value, err := h.getValue(shard.ID, req.Key)
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
	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(old))
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

	value, err := h.getValue(shard.ID, req.Key)
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
	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(old))
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: RunningInstanceEvent,
			Data:      old,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) stopWorkflowInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStopInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
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
	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(old))
	if err != nil {
		log.Fatalf("set workflow instance %d stopped failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: StoppingInstanceEvent,
			Data:      old,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) workflowInstanceStopped(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getStoppedInstanceRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
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
	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(old))
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

func (h *beeStorage) createInstanceWorker(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getCreateInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("create workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(&cmd.State))
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: RunningInstanceWorkerEvent,
			Data:      cmd.State,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) updateInstanceWorkerState(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getUpdateInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	err = h.getStore(shard.ID).Set(req.Key, protoc.MustMarshal(&cmd.State))
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) removeInstanceWorker(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := getRemoveInstanceStateShardRequest(attrs)
	protoc.MustUnmarshal(cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	err := h.getStore(shard.ID).Delete(req.Key)
	if err != nil {
		log.Fatalf("remove workflow instance state %d/%d failed with %+v",
			cmd.WorkflowID,
			cmd.Index,
			err)
	}
	if err != nil {
		log.Fatalf("remove workflow instance state %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: RemoveInstanceWorkerEvent,
			Data: metapb.WorkflowInstanceWorkerState{
				WorkflowID: cmd.WorkflowID,
				Index:      cmd.Index,
			},
		}
	}
	return uint64(len(req.Key)), -int64(len(req.Key)), resp
}
