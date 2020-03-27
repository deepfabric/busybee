package storage

import (
	"math"
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	bhstorage "github.com/deepfabric/beehive/storage"
	bhutil "github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	emptyJoinBytes = protoc.MustMarshal(&rpcpb.QueueJoinGroupResponse{})
)

func (h *beeStorage) queueJoinGroup(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	joinReq := getQueueJoinGroupRequest(attrs)
	protoc.MustUnmarshal(joinReq, req.Cmd)

	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	stateKey := queueStateKey(req.Key[:len(req.Key)-4], joinReq.Group, buf)
	metaKey := queueMetaKey(req.Key[:len(req.Key)-4], buf)
	state := loadQueueState(h.getStore(shard.ID), stateKey, metaKey, attrs)

	joinResp := getQueueJoinGroupResponse(attrs)

	// wait
	if state == nil ||
		state.Consumers >= state.Partitions {
		resp.Value = emptyJoinBytes
		return 0, 0, resp
	}

	now := time.Now().Unix()
	index := state.Consumers
	state.Consumers++

	rebalanceConsumers(state)

	for idx := range state.States {
		if state.States[idx].Consumer == index {
			state.States[idx].LastFetchTS = now
		}
	}

	joinResp.Index = index
	for idx := range state.States {
		if state.States[idx].Consumer == index {
			joinResp.Partitions = append(joinResp.Partitions, uint32(idx))
			joinResp.Versions = append(joinResp.Versions, state.States[idx].Version)
		}
	}

	err := h.getStore(shard.ID).Set(stateKey, protoc.MustMarshal(state))
	if err != nil {
		log.Fatalf("save queue state failed with %+v",
			err)
	}

	resp.Value = protoc.MustMarshal(joinResp)
	return 0, 0, resp
}

func (h *beeStorage) queueFetch(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueFetch := getQueueFetchRequest(attrs)
	protoc.MustUnmarshal(queueFetch, req.Cmd)

	now := time.Now().Unix()
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	stateKey := queueStateKey(req.Key[:len(req.Key)-4], queueFetch.Group, buf)
	metaKey := queueMetaKey(req.Key[:len(req.Key)-4], buf)
	state := loadQueueState(h.getStore(shard.ID), stateKey, metaKey, attrs)

	fetchResp := getQueueFetchResponse(attrs)

	// consumer removed
	if nil == state ||
		state.Consumers == 0 ||
		queueFetch.Consumer >= state.Consumers ||
		queueFetch.Partition >= uint32(len(state.States)) {

		fetchResp.Removed = true
		resp.Value = protoc.MustMarshal(fetchResp)
		return 0, 0, resp
	}

	changed := maybeUpdateCompletedOffset(state, now, queueFetch)
	if maybeRemoveTimeoutConsumers(state, now) {
		changed = true
	}

	p := state.States[queueFetch.Partition]

	// stale consumer
	if p.Consumer != queueFetch.Consumer ||
		p.Version != queueFetch.Version {
		fetchResp.Removed = true
		resp.Value = protoc.MustMarshal(fetchResp)
	}

	// do fetch new items
	if p.Version == queueFetch.Version &&
		p.State == metapb.PSRunning {
		startKey := QueueItemKey(req.Key, p.Completed+1, buf)
		endKey := QueueItemKey(req.Key, p.Completed+1+queueFetch.Count, buf)

		c := uint64(0)
		var items [][]byte
		err := h.getStore(shard.ID).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
			items = append(items, value)
			c++
			return true, nil
		}, false)
		if err != nil {
			log.Fatalf("fetch queue failed with %+v", err)
		}

		if c > 0 {
			fetchResp.LastOffset = p.Completed + c
			fetchResp.Items = items
		}

		state.States[queueFetch.Partition].LastFetchCount = c
		state.States[queueFetch.Partition].LastFetchTS = now
		changed = true
	}

	if changed {
		buf.MarkWrite()
		buf.WriteUint64(state.States[queueFetch.Partition].Completed)
		buf.WriteInt64(now)
		completed := buf.WrittenDataAfterMark()

		wb := bhutil.NewWriteBatch()
		wb.Set(stateKey, protoc.MustMarshal(state))
		wb.Set(committedOffsetKey(req.Key, queueFetch.Group, buf), completed)

		err := h.getStore(shard.ID).Write(wb, false)
		if err != nil {
			log.Fatalf("save concurrency queue state failed with %+v",
				err)
		}
	}

	resp.Value = protoc.MustMarshal(fetchResp)
	return 0, 0, resp
}

func maybeRemoveTimeoutConsumers(state *metapb.QueueState, now int64) bool {
	if state.Consumers > 0 {
		for _, p := range state.States {
			if (now - p.LastFetchTS) > state.Timeout {
				clearConsumers(state)
				return true
			}
		}
	}

	return false
}

func clearConsumers(state *metapb.QueueState) {
	state.Consumers = 0
	for idx := range state.States {
		state.States[idx].State = metapb.PSRebalancing
		state.States[idx].Version++
		state.States[idx].Consumer = 0
	}
}

func rebalanceConsumers(state *metapb.QueueState) {
	a := float64(state.Partitions) / float64(state.Consumers)
	m := uint32(math.Ceil(a))

	for consumer := uint32(0); consumer < state.Consumers; consumer++ {
		from := m * consumer
		to := (consumer+1)*m - 1

		for i := from; i <= to; i++ {
			if state.States[i].Consumer != consumer {
				state.States[i].Version++
				state.States[i].Consumer = consumer
				state.States[i].State = metapb.PSRebalancing
			}
		}
	}
}

func maybeUpdateCompletedOffset(state *metapb.QueueState, now int64, req *rpcpb.QueueFetchRequest) bool {
	p := state.States[req.Partition]

	// stale consumer
	if p.Version > req.Version {
		if p.State == metapb.PSRunning {
			return false
		}

		// no items fetched, only the newest consumer can change state to running
		if p.LastFetchCount == 0 {
			return false
		}

		// rebalancing, commit last completed
		if req.CompletedOffset > p.Completed {
			state.States[req.Partition].Completed = req.CompletedOffset
		}
		state.States[req.Partition].LastFetchCount = 0
		state.States[req.Partition].LastFetchTS = now
		state.States[req.Partition].State = metapb.PSRunning
		return true
	}

	// normal consumer
	if p.State == metapb.PSRunning {
		if req.CompletedOffset <= p.Completed {
			return false
		}

		state.States[req.Partition].LastFetchCount = 0
		state.States[req.Partition].LastFetchTS = now
		state.States[req.Partition].Completed = req.CompletedOffset
		return true
	}

	if req.CompletedOffset > p.Completed {
		log.Fatalf("BUG: the newest consumer in rebalancing, but completed offset %d > prev completed %d",
			req.CompletedOffset,
			p.Completed)
	}

	// in rebalancing, has no last fetch items
	if p.LastFetchCount == 0 {
		state.States[req.Partition].LastFetchCount = 0
		state.States[req.Partition].LastFetchTS = now
		state.States[req.Partition].State = metapb.PSRunning
		return true
	}

	// has last fetch items, but not completed
	if (now - p.LastFetchTS) < state.Timeout {
		return false
	}

	// processing timeout
	state.States[req.Partition].LastFetchCount = 0
	state.States[req.Partition].LastFetchTS = now
	state.States[req.Partition].State = metapb.PSRunning
	return true
}

func loadQueueState(store bhstorage.DataStorage, stateKey, metaKey []byte, attrs map[string]interface{}) *metapb.QueueState {
	value, err := store.Get(stateKey)
	if err != nil {
		log.Fatalf("load queue state failed with %+v", err)
	}

	if len(value) == 0 {
		value, err = store.Get(metaKey)
		if err != nil {
			log.Fatalf("load queue state failed with %+v", err)
		}

		if len(value) == 0 {
			log.Warningf("missing queue state, %+v", stateKey)
			return nil
		}
	}

	state := getQueueState(attrs)
	protoc.MustUnmarshal(state, value)
	return state
}
