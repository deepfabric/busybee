package storage

import (
	"bytes"
	"context"
	"time"

	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
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

	if shard.Group == uint64(metapb.TenantRunnerGroup) &&
		h.store.MaybeLeader(shard.ID) {
		h.shardC <- shardCycle{
			shard:  shard,
			action: becomeLeader,
		}
	}

	if action.SetKV != nil {
		for {
			req := rpcpb.AcquireSetRequest()
			req.Key = action.SetKV.KV.Key
			req.Value = action.SetKV.KV.Value
			_, err := h.ExecCommandWithGroup(req, action.SetKV.Group)
			if err != nil {
				log.Errorf("shard %d set kv failed with %+v, retry after 5s",
					shard.ID,
					err)
				time.Sleep(time.Second * 5)
				continue
			}

			break
		}

		log.Infof("shard %d set kv completed",
			shard.ID)
	}

	if action.UpdateTenantInitState != nil {
		for {
			req := rpcpb.AcquireTenantInitStateUpdateRequest()
			req.ID = action.UpdateTenantInitState.ID
			req.Index = action.UpdateTenantInitState.Index
			req.Group = action.UpdateTenantInitState.Group
			_, err := h.ExecCommand(req)
			if err != nil {
				log.Errorf("shard %d update tenant init state failed with %+v, retry after 5s",
					shard.ID,
					err)
				time.Sleep(time.Second * 5)
				continue
			}

			break
		}

		log.Infof("shard %d update tenant init state completed",
			shard.ID)
	}

	return nil
}

func (h *beeStorage) Created(shard bhmetapb.Shard) {

}

func (h *beeStorage) Splited(shard bhmetapb.Shard) {

}

func (h *beeStorage) Destory(shard bhmetapb.Shard) {
	if shard.Group == uint64(metapb.DefaultGroup) ||
		shard.Group == uint64(metapb.TenantRunnerGroup) {
		h.shardC <- shardCycle{
			shard:  shard,
			action: becomeFollower,
		}
	}
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

	h.doRunnerEvent(shard, leader)
}

func (h *beeStorage) doLoadEvent(shard bhmetapb.Shard, leader bool) {
	if shard.Group != uint64(metapb.DefaultGroup) {
		return
	}

	from := raftstore.EncodeDataKey(shard.Group, []byte{workflowCurrentPrefix})
	end := raftstore.EncodeDataKey(shard.Group, []byte{workflowCurrentPrefix + 1})

	err := h.getStoreByGroup(shard.Group).Scan(from, end, func(key, value []byte) (bool, error) {
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

func (h *beeStorage) doRunnerEvent(shard bhmetapb.Shard, leader bool) {
	tid := goetty.Byte2UInt64(shard.Start[1:])
	runner := goetty.Byte2UInt64(shard.Start[9:])
	state := &metapb.WorkerRunner{
		ID:    tid,
		Index: runner,
	}

	et := StartRunnerEvent
	if !leader {
		et = StopRunnerEvent
		log.Infof("raft trigger WR[%d-%d] stop", tid, runner)
	} else {
		log.Infof("raft trigger WR[%d-%d] start", tid, runner)
	}
	h.eventC <- Event{
		EventType: et,
		Data:      state,
	}
}
