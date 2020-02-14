package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) startingInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StartingInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	value = protoc.MustMarshal(&cmd.Instance)
	err = h.getStore(shard.ID).Set(req.Key, appendValuePrefix(buf, value, instanceStartingType))
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceLoadedEvent,
			Data:      cmd.Instance,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) updateInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.UpdateInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	switch value[0] {
	case instanceStoppingType, instanceStoppedType:
		return 0, 0, resp
	}

	old := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&old, value[1:])

	if old.Version >= cmd.Instance.Version {
		return 0, 0, resp
	}

	err = h.getStore(shard.ID).Set(req.Key, appendValuePrefix(buf, protoc.MustMarshal(&cmd.Instance), instanceStartedType))
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.Instance.Snapshot.ID, err)
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) startedInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StartedInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get workflow instance %d failed with %+v",
			cmd.WorkflowID, err)
	}
	if len(value) == 0 {
		log.Fatalf("missing workflow instance %d",
			cmd.WorkflowID)
	}

	switch value[0] {
	case instanceStartedType, instanceStoppingType, instanceStoppedType:
		return 0, 0, resp
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value[1:])
	instance.StartedAt = time.Now().Unix()

	err = h.getStore(shard.ID).Set(req.Key, appendValuePrefix(buf, protoc.MustMarshal(&instance), instanceStartedType))
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceStartedEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) stopInstance(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StopInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("get workflow instance %d failed with %+v",
			cmd.WorkflowID, err)
	}
	if len(value) == 0 {
		log.Fatalf("missing workflow instance %d", cmd.WorkflowID)
	}

	switch value[0] {
	case instanceStoppingType, instanceStoppedType:
		return 0, 0, resp
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value[1:])
	instance.StoppedAt = time.Now().Unix()

	err = h.getStore(shard.ID).Set(req.Key,
		appendValuePrefix(buf, protoc.MustMarshal(&instance), instanceStoppingType))
	if err != nil {
		log.Fatalf("set workflow instance %d stopped failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceStoppingEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) createInstanceStateShard(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.CreateInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("create workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	buf.MarkWrite()
	buf.WriteByte(runningStateType)
	buf.WriteUInt64(cmd.State.Version)
	buf.Write(protoc.MustMarshal(&cmd.State))
	err = h.getStore(shard.ID).Set(req.Key, buf.WrittenDataAfterMark())
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceStateLoadedEvent,
			Data:      cmd.State,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) updateState(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.UpdateInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	switch value[0] {
	case stoppedStateType:
		return 0, 0, resp
	}

	oldVersion := goetty.Byte2UInt64(value[1:])
	if oldVersion >= cmd.State.Version {
		return 0, 0, resp
	}

	buf.MarkWrite()
	buf.WriteByte(runningStateType)
	buf.WriteUInt64(cmd.State.Version)
	buf.Write(protoc.MustMarshal(&cmd.State))
	err = h.getStore(shard.ID).Set(req.Key, buf.WrittenDataAfterMark())
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) removeState(shard bhmetapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.RemoveInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard.ID, req.Key)
	if err != nil {
		log.Fatalf("remove workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) == 0 {
		return 0, 0, resp
	}

	switch value[0] {
	case stoppedStateType:
		return 0, 0, resp
	}

	value[0] = stoppedStateType
	err = h.getStore(shard.ID).Set(req.Key, value)
	if err != nil {
		log.Fatalf("remove workflow instance state %d/%d failed with %+v",
			cmd.WorkflowID,
			cmd.Index,
			err)
	}

	if h.store.MaybeLeader(shard.ID) {
		h.eventC <- Event{
			EventType: InstanceStoppedEvent,
			Data:      cmd.WorkflowID,
		}
	}

	return uint64(len(req.Key)), -int64(len(req.Key)), resp
}
