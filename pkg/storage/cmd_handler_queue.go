package storage

import (
	"math"
	"time"

	"github.com/deepfabric/beehive/pb"
	bhmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

var (
	emptyJoinBytes                 = protoc.MustMarshal(&rpcpb.QueueJoinGroupResponse{})
	defaultMaxBytesPerFetch uint64 = 1024 * 1024     // 1mb
	maxBytesInBuf                  = 1024 * 1024 * 5 // 5mb
)

func (h *beeStorage) queueJoinGroup(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	joinReq := getQueueJoinGroupRequest(attrs)
	protoc.MustUnmarshal(joinReq, req.Cmd)

	store := h.getStore(shard.ID)
	defer writeWriteBatch(store, attrs)

	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	stateKey := queueStateKey(req.Key[:len(req.Key)-4], joinReq.Group, buf)
	stateAttrKey := hack.SliceToString(stateKey)
	metaKey := queueMetaKey(req.Key[:len(req.Key)-4], buf)

	joinResp := getQueueJoinGroupResponse(attrs)

	state := loadQueueState(store, stateAttrKey, stateKey, metaKey, attrs)
	defer addStateToAttr(stateAttrKey, state, attrs)

	// wait
	if state == nil {
		resp.Value = emptyJoinBytes
		return 0, 0, resp
	}

	now := time.Now().Unix()
	if state.Consumers >= state.Partitions {
		if !maybeRemoveTimeoutConsumers(state, now) {
			resp.Value = emptyJoinBytes
			return 0, 0, resp
		}
	}

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

	addToWriteBatch(stateKey, protoc.MustMarshal(state), attrs)

	resp.Value = protoc.MustMarshal(joinResp)
	return 0, 0, resp
}

func (h *beeStorage) queueFetch(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueFetch := getQueueFetchRequest(attrs)
	protoc.MustUnmarshal(queueFetch, req.Cmd)

	store := h.getStore(shard.ID)
	defer writeWriteBatch(store, attrs)

	now := time.Now().Unix()
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	stateKey := queueStateKey(req.Key[:len(req.Key)-4], queueFetch.Group, buf)
	stateAttrKey := hack.SliceToString(stateKey)
	metaKey := queueMetaKey(req.Key[:len(req.Key)-4], buf)

	fetchResp := getQueueFetchResponse(attrs)

	state := loadQueueState(store, stateAttrKey, stateKey, metaKey, attrs)
	defer addStateToAttr(stateAttrKey, state, attrs)

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
		log.Infof("group %s clear all consumers, because some consumer timeout",
			string(queueFetch.Group))
		changed = true
	}

	p := state.States[queueFetch.Partition]

	// stale consumer
	if p.Consumer != queueFetch.Consumer ||
		p.Version != queueFetch.Version {
		fetchResp.Removed = true
	} else if p.Version == queueFetch.Version &&
		p.State == metapb.PSRunning {

		if queueFetch.Count >= 0 {
			// do fetch new items
			startKey := QueueItemKey(req.Key, p.Completed+1, buf)
			endKey := QueueItemKey(req.Key, p.Completed+1+queueFetch.Count, buf)

			maxBytesPerFetch := queueFetch.MaxBytes
			if maxBytesPerFetch == 0 {
				maxBytesPerFetch = defaultMaxBytesPerFetch
			}

			size := uint64(0)
			c := uint64(0)
			var items [][]byte
			err := h.getStore(shard.ID).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
				if size >= maxBytesPerFetch ||
					!allowWriteToBuf(buf, len(value)) {
					log.Warningf("queue fetch on group %s skipped, fetch %d, buf cap %d, write at %d, value %d",
						string(queueFetch.Group),
						size,
						buf.Capacity(),
						buf.GetWriteIndex(),
						len(value))
					return false, nil
				}

				buf.MarkWrite()
				buf.Write(value)
				items = append(items, buf.WrittenDataAfterMark())
				c++
				size += uint64(len(value))

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
		}

		state.States[queueFetch.Partition].LastFetchTS = now
		changed = true
	}

	if changed {
		buf.MarkWrite()
		buf.WriteUint64(state.States[queueFetch.Partition].Completed)
		buf.WriteInt64(now)
		completed := buf.WrittenDataAfterMark()

		addToWriteBatch(stateKey, protoc.MustMarshal(state), attrs)
		addToWriteBatch(committedOffsetKey(req.Key, queueFetch.Group, buf), completed, attrs)
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
	max := state.Consumers - 1
	a := float64(state.Partitions) / float64(state.Consumers)
	m := uint32(math.Ceil(a))

	for consumer := uint32(0); consumer < state.Consumers; consumer++ {
		from := m * consumer
		to := (consumer+1)*m - 1
		if to > max {
			to = max
		}

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
		log.Warningf("%s the newest consumer in rebalancing, but completed offset %d > prev completed %d",
			string(req.Group),
			req.CompletedOffset,
			p.Completed)
		state.States[req.Partition].Completed = req.CompletedOffset
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

func loadQueueState(store bhstorage.DataStorage, attrStateKey string, stateKey, metaKey []byte, attrs map[string]interface{}) *metapb.QueueState {
	if value, ok := attrs[attrQueueStates]; ok {
		states := value.(map[string]*metapb.QueueState)
		if state, ok := states[attrStateKey]; ok {
			return state
		}
	}

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

	state := getQueueState(attrStateKey, attrs)
	protoc.MustUnmarshal(state, value)
	return state
}

func getQueueState(key string, attrs map[string]interface{}) *metapb.QueueState {
	var value *metapb.QueueState

	if v, ok := attrs[key]; ok {
		value = v.(*metapb.QueueState)
	} else {
		value = &metapb.QueueState{}
		attrs[key] = value
	}

	value.Reset()
	return value
}

func addStateToAttr(key string, state *metapb.QueueState, attrs map[string]interface{}) {
	if state == nil {
		return
	}

	var states map[string]*metapb.QueueState
	if value, ok := attrs[attrQueueStates]; ok {
		states = value.(map[string]*metapb.QueueState)
	} else {
		states = make(map[string]*metapb.QueueState)
	}

	states[key] = state
	attrs[attrQueueStates] = states
}

func addToWriteBatch(key, value []byte, attrs map[string]interface{}) {
	if _, ok := attrs[attrQueueWriteBatchKey]; !ok {
		attrs[attrQueueWriteBatchKey] = util.NewWriteBatch()
	}

	wb := attrs[attrQueueWriteBatchKey].(*util.WriteBatch)
	wb.Set(key, value)
}

func writeWriteBatch(store bhstorage.DataStorage, attrs map[string]interface{}) {
	if !raftstore.IsLastApplyRequest(attrs) {
		return
	}

	if value, ok := attrs[attrQueueWriteBatchKey]; ok {
		wb := value.(*util.WriteBatch)
		err := store.Write(wb, false)
		if err != nil {
			log.Fatalf("save queue state failed with %+v", err)
		}

		wb.Reset()
	}

	if value, ok := attrs[attrQueueStates]; ok {
		states := value.(map[string]*metapb.QueueState)
		for key := range states {
			delete(states, key)
			delete(attrs, key)
		}
	}
}

func allowWriteToBuf(buf *goetty.ByteBuf, size int) bool {
	return buf.GetWriteIndex()+size < maxBytesInBuf
}
