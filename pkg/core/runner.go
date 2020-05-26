package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	wait = time.Second * 2
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
	onEvent(p uint32, offset uint64, event *metapb.Event) (bool, error)
	stop()
	close()
	workflowID() uint64
	isStopped() bool
	init()
	cachedEventSize() uint64
	handleEvent(func(p uint32, offset uint64)) bool
}

type workerRunner struct {
	sync.RWMutex

	key         string
	tid         uint64
	runnerIndex uint64
	stopped     bool
	eng         *engine
	workers     map[string]worker
	consumer    queue.AsyncConsumer
	lockKey     []byte
	lockValue   []byte

	completedOffsets map[uint32]uint64
}

func newWorkerRunner(tid uint64, runnerIndex uint64, key string, eng *engine) *workerRunner {
	return &workerRunner{
		tid:              tid,
		runnerIndex:      runnerIndex,
		key:              key,
		workers:          make(map[string]worker),
		eng:              eng,
		completedOffsets: make(map[uint32]uint64),
		lockKey:          []byte(key),
		lockValue:        goetty.Uint64ToBytes(eng.store.RaftStore().Meta().ID),
	}
}

func (wr *workerRunner) start() {
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	go wr.run()
}

func (wr *workerRunner) stop() {
	logger.Infof("%s set stop flag", wr.key)
	defer logger.Infof("%s set stop flag completed", wr.key)

	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	wr.stopped = true
}

func (wr *workerRunner) addWorker(key string, w worker, lock bool) {
	if lock {
		wr.Lock()
		defer wr.Unlock()
	}

	if wr.stopped {
		return
	}

	if _, ok := wr.workers[key]; ok {
		return
	}

	wr.workers[key] = w
	w.init()
}

func (wr *workerRunner) removeWorker(key string) {
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	if w, ok := wr.workers[key]; ok {
		w.stop()
		return
	}
}

func (wr *workerRunner) workerCount() int {
	wr.RLock()
	defer wr.RUnlock()

	return len(wr.workers)
}

func (wr *workerRunner) removeWorkers(wid uint64) {
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	for _, w := range wr.workers {
		if w.workflowID() == wid {
			w.stop()
		}
	}
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
			if state.Runner != wr.runnerIndex || state.TenantID != wr.tid {
				logger.Fatalf("BUG: invalid instance shard state %+v on runner %s",
					state,
					wr.key)
			}

			wkey := workerKey(state)
			w, err := newStateWorker(wkey, state, wr.eng)
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
			wr.addWorker(wkey, w, false)

			if idx == len(values)-1 {
				startKey = storage.TenantRunnerWorkerKey(wr.tid, wr.runnerIndex, state.WorkflowID, state.Index+1)
			}
		}
	}

	return loaded, nil
}

func (wr *workerRunner) startQueueConsumer() error {
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
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
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
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
	wr.RLock()
	defer wr.RUnlock()

	if wr.stopped {
		return
	}

	offset := lastOffset - uint64(len(items)) + 1

	for _, item := range items {
		event := metapb.Event{}
		protoc.MustUnmarshal(&event, item)

		for {
			if wr.addEventToWorkers(p, offset, &event) {
				break
			}

			wr.RUnlock()
			time.Sleep(wait)
			wr.RLock()

			if wr.stopped {
				return
			}
		}

		offset++
	}
}

func (wr *workerRunner) addEventToWorkers(p uint32, offset uint64, event *metapb.Event) bool {
	for wkey, w := range wr.workers {
		if !w.isStopped() {
			added, _ := w.onEvent(p, offset, event)
			if !added {
				logger.Infof("%s consume events slow down, %s exceed max cached events %d, current is %d, wait %s",
					wr.key,
					wkey,
					eventsCacheSize,
					w.cachedEventSize(),
					wait)
				return false
			}
		}
	}

	return true
}

func (wr *workerRunner) isStopped() bool {
	wr.RLock()
	defer wr.RUnlock()

	return wr.stopped
}

func (wr *workerRunner) doStop() {
	logger.Infof("%s do stop", wr.key)
	defer logger.Infof("%s do stop completed", wr.key)

	wr.Lock()
	defer wr.Unlock()

	if wr.consumer != nil {
		wr.consumer.Stop()
	}

	for _, w := range wr.workers {
		w.close()
	}
}

func (wr *workerRunner) dlock() {
	resp := rpcpb.AcquireBoolResponse()

	for {
		if wr.isStopped() {
			return
		}

		req := rpcpb.AcquireSetIfRequest()
		req.Key = wr.lockKey
		req.Value = wr.lockValue
		req.Conditions = []rpcpb.ConditionGroup{
			{
				Conditions: []rpcpb.Condition{{Cmp: rpcpb.NotExists}},
			},
			{
				Conditions: []rpcpb.Condition{{Cmp: rpcpb.Equal, Value: wr.lockValue}},
			},
		}
		req.TTL = 10

		value, err := wr.eng.store.ExecCommand(req)
		if err != nil {
			metric.IncStorageFailed()
			logger.Errorf("%s get lock failed with %+v", wr.key, err)
			continue
		}

		resp.Reset()
		protoc.MustUnmarshal(resp, value)

		if resp.Value {
			wr.keepLock(nil)
			logger.Infof("%s distributed lock completed", wr.key)
			return
		}

		time.Sleep(time.Second * 5)
	}
}

func (wr *workerRunner) keepLock(arg interface{}) {
	if wr.isStopped() {
		return
	}

	req := rpcpb.AcquireSetIfRequest()
	req.Key = wr.lockKey
	req.Value = wr.lockValue
	req.Conditions = []rpcpb.ConditionGroup{
		{
			Conditions: []rpcpb.Condition{{Cmp: rpcpb.NotExists}},
		},
		{
			Conditions: []rpcpb.Condition{{Cmp: rpcpb.Equal, Value: wr.lockValue}},
		},
	}
	req.TTL = 10
	wr.eng.store.AsyncExecCommand(req, wr.onLockResponse, nil)
}

func (wr *workerRunner) onLockResponse(arg interface{}, value []byte, err error) {
	retry := time.Second * 5
	if err != nil {
		logger.Infof("%s keep distributed lock failed with %+v", wr.key, err)
		retry = 0
	}

	util.DefaultTimeoutWheel().Schedule(retry, wr.keepLock, nil)
}

func (wr *workerRunner) dunlock() {
	req := rpcpb.AcquireDeleteIfRequest()
	req.Key = wr.lockKey
	req.Conditions = []rpcpb.ConditionGroup{
		{
			Conditions: []rpcpb.Condition{{Cmp: rpcpb.Exists}, {Cmp: rpcpb.Equal, Value: wr.lockValue}},
		},
	}

	wr.eng.store.ExecCommand(req)
	logger.Infof("%s distributed lock released", wr.key)
}

func (wr *workerRunner) run() {
	wr.dlock()
	defer wr.dunlock()

	logger.Infof("%s start load shard states", wr.key)
	for {
		err := wr.loadShardStates()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 5)
	}

	logger.Infof("%s start queue consumer", wr.key)
	for {
		err := wr.startQueueConsumer()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 5)
	}

	var removedWorkers []string
	hasEvent := true
	for {
		if wr.isStopped() {
			wr.doStop()
			return
		}

		if !hasEvent {
			time.Sleep(time.Millisecond * 10)
		}

		for key := range wr.completedOffsets {
			delete(wr.completedOffsets, key)
		}

		hasEvent = false
		wr.RLock()
		for key, w := range wr.workers {
			if w.isStopped() {
				w.close()
				removedWorkers = append(removedWorkers, key)
			} else if w.handleEvent(wr.completed) {
				hasEvent = true
			}
		}
		wr.RUnlock()

		if len(removedWorkers) > 0 {
			wr.Lock()
			for _, key := range removedWorkers {
				delete(wr.workers, key)
			}
			wr.Unlock()
			removedWorkers = removedWorkers[:0]
		}

		if len(wr.completedOffsets) > 0 {
			wr.consumer.Commit(wr.completedOffsets, wr.commitCB)
		}
	}
}

func (wr *workerRunner) completed(p uint32, offset uint64) {
	if offset <= 0 {
		return
	}

	if value, ok := wr.completedOffsets[p]; ok {
		if offset > value {
			wr.completedOffsets[p] = offset
		}
	} else {
		wr.completedOffsets[p] = offset
	}
}

func (wr *workerRunner) commitCB(err error) {
	// just log it, commit next tick
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s commit offset failed with %+v", wr.key, err)
	}
}
