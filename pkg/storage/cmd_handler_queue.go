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

	stateKey := queueStateKey(req.Key[:len(req.Key)-4], joinReq.Group)
	stateAttrKey := hack.SliceToString(stateKey)
	metaKey := queueMetaKey(req.Key)

	joinResp := getQueueJoinGroupResponse(attrs)

	state := loadQueueState(store, stateAttrKey, stateKey, metaKey, attrs)
	defer addStateToAttr(stateAttrKey, state, attrs)

	// wait
	if state == nil {
		resp.Value = emptyJoinBytes
		return 0, 0, resp
	}

	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)
	if _, ok := loadLastCompletedOffset(store, req.Key, joinReq.Group, buf); !ok {
		for i := uint32(0); i < state.Partitions; i++ {
			mustPutCompletedOffset(store, raftstore.EncodeDataKey(req.Group, PartitionKey(joinReq.ID, i)), joinReq.Group, buf, 0)
		}
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
	stateKey := queueStateKey(req.Key[:len(req.Key)-4], queueFetch.Group)
	stateAttrKey := hack.SliceToString(stateKey)
	metaKey := queueMetaKey(req.Key)

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

	changed := false
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
	} else if p.Version == queueFetch.Version {
		if queueFetch.Count >= 0 {
			completed := queueFetch.CompletedOffset
			if completed == 0 {
				completed, _ = loadLastCompletedOffset(store, req.Key, queueFetch.Group, buf)
			}

			// do fetch new items
			startKey := QueueItemKey(req.Key, completed+1)
			endKey := QueueItemKey(req.Key, completed+1+queueFetch.Count)

			maxBytesPerFetch := queueFetch.MaxBytes
			if maxBytesPerFetch == 0 {
				maxBytesPerFetch = defaultMaxBytesPerFetch
			}

			size := uint64(0)
			c := uint64(0)
			var items []goetty.Slice

			err := h.getStore(shard.ID).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
				if !allowWriteToBuf(buf, len(value)) {
					log.Infof("queue fetch on group %s skipped, fetch %d, buf cap %d, write at %d, value %d",
						string(queueFetch.Group),
						size,
						buf.Capacity(),
						buf.GetWriteIndex(),
						len(value))
					return false, nil
				}

				if size >= maxBytesPerFetch {
					log.Infof("queue fetch on group %s skipped, fetch %d",
						string(queueFetch.Group),
						size)
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
				fetchResp.LastOffset = completed + c
				for idx := range items {
					fetchResp.Items = append(fetchResp.Items, items[idx].Data())
				}
			}
		}

		state.States[queueFetch.Partition].LastFetchTS = now
		changed = true
	}

	resp.Value = protoc.MustMarshal(fetchResp)

	if changed {
		addToWriteBatch(stateKey, protoc.MustMarshal(state), attrs)
	}

	return 0, 0, resp
}

func (h *beeStorage) queueDelete(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	deleteReq := getQueueDeleteRequest(attrs)
	protoc.MustUnmarshal(deleteReq, req.Cmd)

	store := h.getStore(shard.ID)
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)

	from := queueItemKey(req.Key, deleteReq.From, buf)
	to := queueItemKey(req.Key, deleteReq.To+1, buf)

	err := store.RangeDelete(from.Data(), to.Data())
	if err != nil {
		log.Fatalf("delete queue items failed with %+v", err)
	}

	buf.MarkWrite()
	buf.WriteUInt64(deleteReq.To)
	err = store.Set(removedOffsetKey(req.Key), buf.WrittenDataAfterMark().Data())
	if err != nil {
		log.Fatalf("delete queue items failed with %+v", err)
	}

	resp.Value = rpcpb.EmptyRespBytes
	return 0, 0, resp
}

func (h *beeStorage) queueScan(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	queueScan := getQueueScanRequest(attrs)
	protoc.MustUnmarshal(queueScan, req.Cmd)

	fetchResp := getQueueFetchResponse(attrs)
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)

	store := h.getStore(shard.ID)

	completed := queueScan.CompletedOffset
	lastCompleted, ok := loadLastCompletedOffset(store, req.Key, queueScan.Consumer, buf)

	if !ok {
		max := loadMaxOffset(store, req.Key, buf)
		mustPutCompletedOffset(store, req.Key, queueScan.Consumer, buf, max)
		resp.Value = protoc.MustMarshal(fetchResp)
		return resp
	}

	if lastCompleted > completed {
		completed = lastCompleted
	}

	// do fetch new items
	startKey := QueueItemKey(req.Key, completed+1)
	endKey := QueueItemKey(req.Key, completed+1+queueScan.Count)

	maxBytesPerFetch := queueScan.MaxBytes
	if maxBytesPerFetch == 0 {
		maxBytesPerFetch = defaultMaxBytesPerFetch
	}

	size := uint64(0)
	c := uint64(0)
	var items []goetty.Slice

	err := store.Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		if !allowWriteToBuf(buf, len(value)) {
			log.Infof("queue scan skipped, fetch %d, buf cap %d, write at %d, value %d",
				size,
				buf.Capacity(),
				buf.GetWriteIndex(),
				len(value))
			return false, nil
		}

		if size >= maxBytesPerFetch {
			log.Infof("queue fetch skipped, fetch %d",
				size)
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
		fetchResp.LastOffset = completed + c
		for idx := range items {
			fetchResp.Items = append(fetchResp.Items, items[idx].Data())
		}
	}

	resp.Value = protoc.MustMarshal(fetchResp)
	return resp
}

func (h *beeStorage) queueCommit(shard bhmetapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	queueCommit := getQueueCommitRequest(attrs)
	protoc.MustUnmarshal(queueCommit, req.Cmd)

	store := h.getStore(shard.ID)
	buf := attrs[raftstore.AttrBuf].(*goetty.ByteBuf)

	if v, ok := loadLastCompletedOffset(store, req.Key, queueCommit.Consumer, buf); ok && v >= queueCommit.CompletedOffset {
		return 0, 0, resp
	}

	mustPutCompletedOffset(store, req.Key, queueCommit.Consumer,
		buf, queueCommit.CompletedOffset)
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
			}
		}
	}
}

func loadMaxOffset(store bhstorage.DataStorage, key []byte, buf *goetty.ByteBuf) uint64 {
	value, err := store.Get(maxOffsetKey(key))
	if err != nil {
		log.Fatalf("load queue max offset failed with %+v", err)
	}

	if len(value) == 0 {
		return 0
	}

	return goetty.Byte2UInt64(value)
}

func loadLastCompletedOffset(store bhstorage.DataStorage, key []byte, consumer []byte, buf *goetty.ByteBuf) (uint64, bool) {
	value, err := store.Get(committedOffsetKey(key, consumer))
	if err != nil {
		log.Fatalf("load consumer last completed offset failed with %+v", err)
	}

	if len(value) == 0 {
		return 0, false
	}

	return goetty.Byte2UInt64(value), true
}

func mustPutCompletedOffset(store bhstorage.DataStorage, key []byte, consumer []byte, buf *goetty.ByteBuf, value uint64) {
	completedKey := committedOffsetKey(key, consumer)
	buf.MarkWrite()
	buf.WriteUint64(value)
	buf.WriteInt64(time.Now().Unix())
	err := store.Set(completedKey, buf.WrittenDataAfterMark().Data())
	if err != nil {
		log.Fatalf("save consumer last completed offset failed with %+v", err)
	}
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
