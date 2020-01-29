package storage

import (
	"time"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

func (h *beeStorage) startingInstance(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StartingInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	value = protoc.MustMarshal(&cmd.Instance)
	err = h.getStore(shard).Set(req.Key, appendValuePrefix(buf, value, instanceStartingType))
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceLoadedEvent,
			Data:      cmd.Instance,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) startedInstance(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StartedInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard, req.Key)
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

	err = h.getStore(shard).Set(req.Key, appendValuePrefix(buf, protoc.MustMarshal(&instance), instanceStartedType))
	if err != nil {
		log.Fatalf("set workflow instance %d started failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStartedEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) stopInstance(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.StopInstanceRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard, req.Key)
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

	err = h.getStore(shard).Set(req.Key,
		appendValuePrefix(buf, protoc.MustMarshal(&instance), instanceStoppingType))
	if err != nil {
		log.Fatalf("set workflow instance %d stopped failed with %+v",
			cmd.WorkflowID, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStoppingEvent,
			Data:      instance,
		}
	}

	return uint64(len(req.Key) + len(value)), 0, resp
}

func (h *beeStorage) createState(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.CreateInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValue(shard, req.Key)
	if err != nil {
		log.Fatalf("create workflow instance state %+v failed with %+v", cmd, err)
	}
	if len(value) > 0 {
		return 0, 0, resp
	}

	idx := buf.GetWriteIndex()
	buf.WriteByte(runingStateType)
	buf.WriteUInt64(cmd.State.Version)
	buf.Write(protoc.MustMarshal(&cmd.State))
	err = h.getStore(shard).Set(req.Key, buf.RawBuf()[idx:buf.GetWriteIndex()])
	if err != nil {
		log.Fatalf("save workflow instance %+v failed with %+v", cmd, err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStateLoadedEvent,
			Data:      cmd.State,
		}
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) updateState(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.UpdateInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard, req.Key)
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

	idx := buf.GetWriteIndex()
	buf.WriteByte(runingStateType)
	buf.WriteUInt64(cmd.State.Version)
	buf.Write(protoc.MustMarshal(&cmd.State))
	err = h.getStore(shard).Set(req.Key, buf.RawBuf()[idx:buf.GetWriteIndex()])
	if err != nil {
		log.Fatalf("update workflow instance state %+v failed with %+v", cmd, err)
	}

	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *beeStorage) removeState(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := rpcpb.RemoveInstanceStateShardRequest{}
	protoc.MustUnmarshal(&cmd, req.Cmd)

	resp.Value = rpcpb.EmptyRespBytes

	value, err := h.getValueWithPrefix(shard, req.Key)
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
	err = h.getStore(shard).Set(req.Key, value)
	if err != nil {
		log.Fatalf("remove workflow instance state %d/[%d,%d) failed with %+v",
			cmd.WorkflowID,
			cmd.Start,
			cmd.End,
			err)
	}

	if h.store.MaybeLeader(shard) {
		h.eventC <- Event{
			EventType: InstanceStoppedEvent,
			Data:      cmd.WorkflowID,
		}
	}

	return uint64(len(req.Key)), -int64(len(req.Key)), resp
}
