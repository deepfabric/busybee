package storage

import (
	"bytes"
	"context"
	"math"

	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

const (
	becomeLeader = iota
	becomeFollower
)

type shardCycle struct {
	shard  bhmetapb.Shard
	action int
}

func (h *beeStorage) addShardCallback(shard bhmetapb.Shard) error {
	if len(shard.Data) == 0 {
		return nil
	}

	action := &metapb.CallbackAction{}
	protoc.MustUnmarshal(action, shard.Data)

	if action.SetKV != nil {
		req := rpcpb.AcquireSetRequest()
		req.Key = action.SetKV.KV.Key
		req.Value = action.SetKV.KV.Value
		_, err := h.ExecCommandWithGroup(req, action.SetKV.Group)
		if err != nil {
			return err
		}

		if shard.Group == uint64(metapb.TenantRunnerGroup) &&
			h.store.MaybeLeader(shard.ID) {
			h.shardC <- shardCycle{
				shard:  shard,
				action: becomeLeader,
			}
		}
	}

	return nil
}

func (h *beeStorage) Created(shard bhmetapb.Shard) {

}

func (h *beeStorage) Splited(shard bhmetapb.Shard) {

}

func (h *beeStorage) Destory(shard bhmetapb.Shard) {

}

func (h *beeStorage) BecomeLeader(shard bhmetapb.Shard) {
	if shard.Group == uint64(metapb.DefaultGroup) ||
		shard.Group == uint64(metapb.TenantRunnerGroup) {
		h.shardC <- shardCycle{
			shard:  shard,
			action: becomeLeader,
		}
	}
}

func (h *beeStorage) BecomeFollower(shard bhmetapb.Shard) {
	if shard.Group == uint64(metapb.DefaultGroup) ||
		shard.Group == uint64(metapb.TenantRunnerGroup) {
		h.shardC <- shardCycle{
			shard:  shard,
			action: becomeFollower,
		}
	}
}

func (h *beeStorage) handleShardCycle(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("handle shard cycle task stopped")
			return
		case shard, ok := <-h.shardC:
			if ok {
				switch shard.action {
				case becomeLeader:
					h.doLoadEvent(shard.shard, true)
					h.doLoadWorkerRunnerEvent(shard.shard, true)
				case becomeFollower:
					h.doLoadEvent(shard.shard, false)
					h.doLoadWorkerRunnerEvent(shard.shard, false)
				}
			}
		}
	}
}

func (h *beeStorage) doLoadWorkerRunnerEvent(shard bhmetapb.Shard, leader bool) {
	if shard.Group != uint64(metapb.TenantRunnerGroup) {
		return
	}

	from := raftstore.EncodeDataKey(shard.Group, TenantRunnerMetadataKey(0, 0))
	end := raftstore.EncodeDataKey(shard.Group, TenantRunnerMetadataKey(math.MaxUint64, math.MaxUint64))

	err := h.getStore(shard.ID).Scan(from, end, func(key, value []byte) (bool, error) {
		decodedKey := raftstore.DecodeDataKey(key)

		if bytes.Compare(decodedKey, shard.Start) >= 0 {
			if len(shard.End) == 0 || bytes.Compare(decodedKey, shard.End) < 0 {
				switch decodedKey[17] {
				case tenantRunnerMetadataPrefix:
					h.doRunnerEvent(shard, value, leader)
				}
			} else {
				return false, nil
			}
		}

		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("scan shard data for loading failed with %+v", err)
	}
}

func (h *beeStorage) doLoadEvent(shard bhmetapb.Shard, leader bool) {
	if shard.Group != uint64(metapb.DefaultGroup) {
		return
	}

	from := raftstore.EncodeDataKey(shard.Group, []byte{workflowCurrentPrefix})
	end := raftstore.EncodeDataKey(shard.Group, []byte{workflowCurrentPrefix + 1})

	err := h.getStore(shard.ID).Scan(from, end, func(key, value []byte) (bool, error) {
		decodedKey := raftstore.DecodeDataKey(key)

		if bytes.Compare(decodedKey, shard.Start) >= 0 {
			if len(shard.End) == 0 || bytes.Compare(decodedKey, shard.End) < 0 {
				switch decodedKey[0] {
				case workflowCurrentPrefix:
					if leader {
						h.doWorkflowEvent(value)
					}
				}
			} else {
				return false, nil
			}
		}

		return true, nil
	}, false)
	if err != nil {
		log.Fatalf("scan shard data for loading failed with %+v", err)
	}
}

func (h *beeStorage) doWorkflowEvent(value []byte) {
	instance := &metapb.WorkflowInstance{}
	protoc.MustUnmarshal(instance, value)

	switch instance.State {
	case metapb.Starting:
		h.eventC <- Event{
			EventType: StartingInstanceEvent,
			Data:      instance,
		}
	case metapb.Running:
		h.eventC <- Event{
			EventType: RunningInstanceEvent,
			Data:      instance,
		}
	case metapb.Stopping:
		h.eventC <- Event{
			EventType: StoppingInstanceEvent,
			Data:      instance,
		}
	}
}

func (h *beeStorage) doRunnerEvent(shard bhmetapb.Shard, value []byte, leader bool) {
	state := &metapb.WorkerRunner{}
	protoc.MustUnmarshal(state, value)

	et := StartRunnerEvent
	if !leader {
		et = StopRunnerEvent
	}
	h.eventC <- Event{
		EventType: et,
		Data:      state,
	}
}
