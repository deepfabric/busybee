package core

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/uuid"
)

var (
	wait = time.Second * 2

	emptyEvent = &metapb.Event{
		Type: metapb.UserType,
	}
)

func runnerKey(state *metapb.WorkerRunner) string {
	return fmt.Sprintf("WR[%d-%d]", state.ID, state.Index)
}

func (eng *engine) doStartRunnerEvent(state *metapb.WorkerRunner) {
	key := runnerKey(state)
	logger.Infof("handle event for start %s", key)

	if _, ok := eng.runners.Load(key); ok {
		logger.Infof("handle event for start %s, skipped by already exists", key)
		return
	}

	wr := newWorkerRunner(state.ID, state.Index, key, eng)
	eng.runners.Store(key, wr)

	logger.Infof("%s created", key)
	wr.start()
	logger.Infof("handle event for start %s completed", key)
}

func (eng *engine) doStopRunnerEvent(state *metapb.WorkerRunner) {
	key := runnerKey(state)
	logger.Infof("handle event for stop %s", key)
	value, ok := eng.runners.Load(key)
	if !ok {
		logger.Infof("handle event for stop %s, skipped by not exists", key)
		return
	}

	logger.Infof("%s stop", key)
	eng.runners.Delete(key)
	value.(*workerRunner).stop()
	logger.Infof("handle event for stop completed %s", key)
}

type worker interface {
	beloneTo(event *metapb.Event) bool
	onEvent(p uint32, offset uint64, event *metapb.Event) (bool, error)
	stop()
	close()
	workflowID() uint64
	isStopped() bool
	init(uint32)
	cachedEventSize() int64
	handleEvent(func(key string, p uint32, offset uint64)) (bool, bool)
	initCompleted() bool
}

type workerRunner struct {
	key             string
	tid             uint64
	runnerIndex     uint64
	stopped         uint32
	eng             *engine
	workers         sync.Map // map[string]worker
	consumer        queue.AsyncConsumer
	events          [][]*metapb.Event
	lockKey         []byte
	lockExpectValue []byte
	reLock          uint32
	metadata        metapb.Tenant

	disableLoadShardState bool
	disableStartConsumer  bool
	disableInitOffsets    bool
	disableLock           bool

	offsetKey        []byte
	completedOffsets *metapb.TenantRunnerOffsets
	offsetChanged    bool
	lastTS           int64
}

func newWorkerRunner(tid uint64, runnerIndex uint64, key string, eng *engine) *workerRunner {
	wr := &workerRunner{
		tid:              tid,
		runnerIndex:      runnerIndex,
		key:              key,
		eng:              eng,
		lockKey:          storage.TenantRunnerLockKey(tid, runnerIndex, []byte(key)),
		lockExpectValue:  uuid.NewV4().Bytes(),
		completedOffsets: &metapb.TenantRunnerOffsets{},
		offsetKey:        storage.TenantRunnerOffsetKey(tid, runnerIndex),
	}

	if nil != eng {
		wr.metadata = eng.mustDoLoadTenantMetadata(tid)
		wr.events = make([][]*metapb.Event, wr.metadata.Input.Partitions, wr.metadata.Input.Partitions)
	}

	return wr
}

func (wr *workerRunner) start() {
	if atomic.CompareAndSwapUint32(&wr.stopped, 0, 1) {
		go wr.run()
	}
}

func (wr *workerRunner) stop() {
	logger.Infof("%s stop flag set", wr.key)
	atomic.CompareAndSwapUint32(&wr.stopped, 1, 0)
}

func (wr *workerRunner) isStopped() bool {
	return atomic.LoadUint32(&wr.stopped) == 0
}

func (wr *workerRunner) addWorker(key string, w worker) {
	if wr.isStopped() {
		return
	}

	if _, ok := wr.workers.Load(key); ok {
		return
	}

	w.init(wr.metadata.Input.Partitions)
	wr.workers.Store(key, w)
}

func (wr *workerRunner) removeWorker(key string) {
	if wr.isStopped() {
		return
	}

	if w, ok := wr.workers.Load(key); ok {
		w.(worker).stop()
		return
	}
}

func (wr *workerRunner) workerCount() int {
	c := 0
	wr.workers.Range(func(k, v interface{}) bool {
		c++
		return true
	})
	return c
}

func (wr *workerRunner) removeWorkers(wid uint64) {
	if wr.isStopped() {
		return
	}

	wr.workers.Range(func(k, v interface{}) bool {
		w := v.(worker)
		if w.workflowID() == wid {
			w.stop()
		}
		return true
	})
}

func (wr *workerRunner) loadInstanceShardStates() (int, error) {
	startKey := storage.TenantRunnerWorkerMinKey(wr.tid, wr.runnerIndex)
	endKey := storage.TenantRunnerWorkerMaxKey(wr.tid, wr.runnerIndex)
	loaded := 0

	for {
		_, values, err := wr.eng.store.ScanWithGroup(startKey, endKey, 4, metapb.TenantRunnerGroup)
		if err != nil {
			return 0, err
		}

		if len(values) == 0 {
			break
		}

		loaded += len(values)
		for idx, value := range values {
			state := metapb.WorkflowInstanceWorkerState{}
			protoc.MustUnmarshal(&state, value)

			wkey := workerKey(state)
			w, err := newStateWorker(wkey, state, wr)
			if err != nil {
				logger.Infof("%s loaded %s failed with %+v",
					wr.key,
					wkey,
					err)
				return 0, err
			}

			logger.Infof("%s loaded %s",
				wr.key,
				wkey)
			wr.addWorker(wkey, w)

			if wr.isStopped() {
				return 0, nil
			}

			if idx == len(values)-1 {
				startKey = storage.TenantRunnerWorkerKey(wr.tid, wr.runnerIndex, state.WorkflowID, state.Index+1)
			}
		}
	}

	return loaded, nil
}

func (wr *workerRunner) startQueueConsumer(offsets []uint64) error {
	if wr.isStopped() {
		logger.Infof("%s create queue consumer skipped by stopped", wr.key)
		return nil
	}

	consumer, err := queue.NewAsyncConsumer(wr.tid, wr.eng.store, []byte(wr.key))
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s create consumer failed with %+v, retry after 5s",
			wr.key,
			err)
		return err
	}

	wr.consumer = consumer
	wr.consumer.Start(wr.onEvent)
	logger.Infof("%s consumer started",
		wr.key)
	return nil
}

func (wr *workerRunner) loadShardStates() error {
	if wr.isStopped() {
		logger.Infof("%s load instance shard states skipped by stopped", wr.key)
		return nil
	}

	loaded, err := wr.loadInstanceShardStates()
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s load instance shard states failed with %+v, retry after 5s",
			wr.key,
			err)
		return err
	}

	logger.Infof("%s load %d instance shard states",
		wr.key,
		loaded)
	return nil
}

func (wr *workerRunner) onEvent(p uint32, lastOffset uint64, items [][]byte) {
	if wr.isStopped() {
		return
	}

	events := wr.events[p]
	events = events[:0]
	for _, item := range items {
		event := &metapb.Event{}
		protoc.MustUnmarshal(event, item)
		events = append(events, event)
	}
	wr.events[p] = events

	wr.workers.Range(func(k, v interface{}) bool {
		w := v.(worker)
		if !w.isStopped() {
			offset := lastOffset - uint64(len(items)) + 1
			ignoreOffset := uint64(0)

			for _, event := range events {
				if !w.beloneTo(event) {
					ignoreOffset = offset

				} else {
					ignoreOffset = 0
					for {
						added, _ := w.onEvent(p, offset, event)
						if added {
							break
						}

						current := w.cachedEventSize()
						if current < eventsCacheSize {
							continue
						}

						logger.Infof("%+v consume events slow down, %s exceed max cached events %d, current is %d, wait %s",
							wr.key,
							k,
							eventsCacheSize,
							current,
							wait)

						time.Sleep(wait)
						if wr.isStopped() {
							return false
						}
					}
				}

				offset++
			}

			if ignoreOffset > 0 {
				w.onEvent(p, offset, emptyEvent)
			}
		}

		return true
	})
}

func (wr *workerRunner) doStop() {
	logger.Infof("%s do stop", wr.key)
	defer logger.Infof("%s do stop completed", wr.key)

	if wr.consumer != nil {
		wr.consumer.Stop()
	}

	wr.workers.Range(func(k, v interface{}) bool {
		w := v.(worker)
		w.close()
		return true
	})
}

func (wr *workerRunner) lock() {
	for {
		if wr.isStopped() {
			return
		}

		ok, err := wr.eng.Storage().Lock(wr.lockKey, wr.lockExpectValue, 5, time.Second*1, false,
			wr.resetLock, metapb.TenantRunnerGroup)
		if err != nil {
			logger.Errorf("%s get lock failed with %+v", wr.key, err)
			time.Sleep(time.Second * 5)
			continue
		}

		if !ok {
			time.Sleep(time.Second * 10)
			continue
		}

		break
	}

	logger.Infof("%s lock %s completed",
		wr.key,
		hex.EncodeToString(wr.lockExpectValue))
}

func (wr *workerRunner) unlock() {
	logger.Infof("%s ready to unlock %s",
		wr.key,
		hex.EncodeToString(wr.lockExpectValue))

	err := wr.eng.Storage().Unlock(wr.lockKey, wr.lockExpectValue)
	if err != nil {
		logger.Errorf("%s unlock %s failed with %+v",
			wr.key,
			hex.EncodeToString(wr.lockExpectValue),
			err)
	}

	logger.Infof("%s unlock %s completed",
		wr.key,
		hex.EncodeToString(wr.lockExpectValue))
}

func (wr *workerRunner) resetLock() {
	atomic.StoreUint32(&wr.reLock, 1)
}

func (wr *workerRunner) maybeRetryLock() {
	if wr.disableLock {
		return
	}

	if atomic.LoadUint32(&wr.reLock) == 0 {
		return
	}

	wr.lock()
	atomic.StoreUint32(&wr.reLock, 0)
}

func (wr *workerRunner) run() {
	if !wr.disableLock {
		logger.Infof("%s ready to lock %s",
			wr.key,
			hex.EncodeToString(wr.lockExpectValue))

		wr.lock()
		defer wr.unlock()
	}

	wr.reset()

	offsets := make([]uint64, wr.metadata.Input.Partitions, wr.metadata.Input.Partitions)
	hasEvent := true
	handleSucceed := true
	for {
		if wr.isStopped() {
			wr.doStop()
			return
		}

		wr.maybeRetryLock()

		if !hasEvent {
			time.Sleep(time.Millisecond * 10)
		}

		hasEvent = false
		handleSucceed = true
		wr.workers.Range(func(k, v interface{}) bool {
			if wr.isStopped() {
				return false
			}

			w := v.(worker)
			if w.isStopped() {
				wr.workers.Delete(k)
				w.close()
				return true
			}

			hasEvent, handleSucceed = w.handleEvent(wr.completed)
			if !handleSucceed {
				logger.Infof("%s handle events failed with stale with lock %s, restart",
					wr.key,
					hex.EncodeToString(wr.lockExpectValue))
				return false
			}

			return true
		})

		if wr.isStopped() {
			wr.doStop()
			return
		}

		if handleSucceed {
			now := time.Now().Unix()
			if wr.offsetChanged && now-wr.lastTS > 5 {
				wr.saveCompletedOffsets()

				for idx := range offsets {
					offsets[idx] = 0
				}
				wr.getOffsets(offsets)
				for p, v := range offsets {
					wr.consumer.CommitPartition(uint32(p), v, wr.commitCB)
				}

				wr.offsetChanged = false
				wr.lastTS = now
				logger.Infof("%s save offsets with %+v, %+v",
					wr.key,
					offsets,
					wr.completedOffsets)
			}

			continue
		}

		logger.Infof("%s with lock %s start reset",
			wr.key,
			hex.EncodeToString(wr.lockExpectValue))

		wr.reset()
		wr.resetLock()

		logger.Infof("%s with lock %s completed reset",
			wr.key,
			hex.EncodeToString(wr.lockExpectValue))
	}
}

func (wr *workerRunner) reset() {
	if !wr.disableLoadShardState {
		wr.workers.Range(func(k, v interface{}) bool {
			wr.workers.Delete(k)
			return true
		})

		logger.Infof("%s start load shard states", wr.key)
		for {
			err := wr.loadShardStates()
			if err == nil {
				break
			}
			time.Sleep(time.Second * 5)
		}
	}

	offsets := wr.loadCompletedOffsets()
	logger.Infof("%s load init offsets %+v, %+v",
		wr.key,
		offsets,
		wr.completedOffsets.String())

	if !wr.disableStartConsumer {
		if wr.consumer != nil {
			wr.consumer.Stop()
		}

		for {
			err := wr.startQueueConsumer(offsets)
			if err == nil {
				break
			}

			if wr.isStopped() {
				return
			}
			time.Sleep(time.Second * 5)
		}
	}
}

func (wr *workerRunner) loadCompletedOffsets() []uint64 {
	offsets := make([]uint64, wr.metadata.Input.Partitions, wr.metadata.Input.Partitions)
	wr.completedOffsets = &metapb.TenantRunnerOffsets{}

	for {
		value, err := wr.eng.store.GetWithGroup(wr.offsetKey, metapb.TenantRunnerGroup)
		if err == nil {
			if len(value) > 0 {
				protoc.MustUnmarshal(wr.completedOffsets, value)
				wr.getOffsets(offsets)
			}
			return offsets
		}

		if wr.isStopped() {
			return offsets
		}

		logger.Errorf("%s load completed offsets failed with %+v",
			wr.key,
			err)
		time.Sleep(time.Second * 5)
	}
}

func (wr *workerRunner) getOffsets(offsets []uint64) {
	for _, v := range wr.completedOffsets.Offsets {
		for p, vv := range v.Offsets {
			if offsets[p] == 0 || vv < offsets[p] {
				offsets[p] = vv
			}
		}
	}
}

func (wr *workerRunner) saveCompletedOffsets() {
	req := rpcpb.AcquireSetRequest()
	req.Key = wr.offsetKey
	req.Value = protoc.MustMarshal(wr.completedOffsets)
	wr.eng.store.AsyncExecCommandWithGroup(req, metapb.TenantRunnerGroup, wr.saveCB, nil)
}

func (wr *workerRunner) completed(key string, p uint32, value uint64) {
	wr.offsetChanged = true

	for idx, w := range wr.completedOffsets.Workers {
		if w == key {
			wr.completedOffsets.Offsets[idx].Offsets[p] = value
			return
		}
	}

	v := &metapb.TenantRunnerWorkerOffsets{
		Offsets: make([]uint64, wr.metadata.Input.Partitions, wr.metadata.Input.Partitions),
	}
	v.Offsets[p] = value
	wr.completedOffsets.Workers = append(wr.completedOffsets.Workers, key)
	wr.completedOffsets.Offsets = append(wr.completedOffsets.Offsets, v)
}

func (wr *workerRunner) saveCB(arg interface{}, data []byte, err error) {
	// just log it, commit next tick
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s save offset failed with %+v", wr.key, err)
	}
}

func (wr *workerRunner) commitCB(err error) {
	// just log it, commit next tick
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s commit offset failed with %+v", wr.key, err)
	}
}
