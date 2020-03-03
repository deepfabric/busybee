package storage

import (
	"context"

	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
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

func (h *beeStorage) Created(shard bhmetapb.Shard) {

}

func (h *beeStorage) Splited(shard bhmetapb.Shard) {

}

func (h *beeStorage) Destory(shard bhmetapb.Shard) {

}

func (h *beeStorage) BecomeLeader(shard bhmetapb.Shard) {
	if shard.Group == uint64(metapb.DefaultGroup) {
		h.shardC <- shardCycle{
			shard:  shard,
			action: becomeLeader,
		}
	}
}

func (h *beeStorage) BecomeFollower(shard bhmetapb.Shard) {
	if shard.Group == uint64(metapb.DefaultGroup) {
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
				case becomeFollower:
					h.doLoadEvent(shard.shard, false)
				}
			}
		}
	}
}

func (h *beeStorage) doLoadEvent(shard bhmetapb.Shard, leader bool) {
	err := h.getStore(shard.ID).Scan(shard.Start, shard.End, func(key, value []byte) (bool, error) {
		if len(value) == 0 {
			return true, nil
		}

		switch key[0] {
		case workflowCurrentPrefix:
			if leader {
				h.doWorkflowEvent(value)
			}
		case workflowWorkerPrefix:
			h.doWorkflowWorkerEvent(value, leader)
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

func (h *beeStorage) doWorkflowWorkerEvent(value []byte, leader bool) {
	state := &metapb.WorkflowInstanceWorkerState{}
	protoc.MustUnmarshal(state, value)

	et := RunningInstanceWorkerEvent
	if !leader {
		et = RemoveInstanceWorkerEvent
	}
	h.eventC <- Event{
		EventType: et,
		Data:      state,
	}
}
