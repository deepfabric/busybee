package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
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
	onEvent(p uint32, offset uint64, event *metapb.Event) (bool, bool, error)
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
	stopped     uint32
	eng         *engine
	workers     sync.Map // map[string]worker
	consumer    queue.AsyncConsumer

	completedOffsetsPerHandle map[uint32]uint64 // uint32 -> uint64
	allWaitCommitOffsets      map[uint32]uint64 // uint32 -> uint64

	disableLoadShardState bool
	disableStartConsumer  bool
}

func newWorkerRunner(tid uint64, runnerIndex uint64, key string, eng *engine) *workerRunner {
	return &workerRunner{
		tid:                       tid,
		runnerIndex:               runnerIndex,
		key:                       key,
		eng:                       eng,
		completedOffsetsPerHandle: make(map[uint32]uint64),
		allWaitCommitOffsets:      make(map[uint32]uint64),
	}
}

func (wr *workerRunner) start() {
	if atomic.CompareAndSwapUint32(&wr.stopped, 0, 1) {
		go wr.run()
	}
}

func (wr *workerRunner) stop() {
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

	wr.workers.Store(key, w)
	w.init()
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
			wr.addWorker(wkey, w)

			if idx == len(values)-1 {
				startKey = storage.TenantRunnerWorkerKey(wr.tid, wr.runnerIndex, state.WorkflowID, state.Index+1)
			}
		}
	}

	return loaded, nil
}

func (wr *workerRunner) startQueueConsumer() error {
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

	offset := lastOffset - uint64(len(items)) + 1
	lastHandledOffset := uint64(0)
	for _, item := range items {
		event := metapb.Event{}
		protoc.MustUnmarshal(&event, item)

		for {
			added, needHandled := wr.addEventToWorkers(p, offset, &event)
			if needHandled {
				lastHandledOffset = offset
			}

			if added {
				if needHandled {
					wr.Lock()
					wr.allWaitCommitOffsets[p] = offset
					wr.Unlock()
				}

				break
			}

			time.Sleep(wait)
			if wr.isStopped() {
				return
			}
		}

		offset++
	}

	// no worker handle this events
	if lastHandledOffset == 0 {
		wr.RLock()
		_, hasWait := wr.allWaitCommitOffsets[p]
		wr.RUnlock()

		// prev events was not committed
		if hasWait {
			return
		}

		wr.consumer.CommitPartition(p, lastOffset, wr.commitCB)
		return
	}
}

func (wr *workerRunner) addEventToWorkers(p uint32, offset uint64, event *metapb.Event) (bool, bool) {
	allAdded := true
	handled := false
	wr.workers.Range(func(k, v interface{}) bool {
		w := v.(worker)
		if !w.isStopped() {
			added, needHandle, _ := w.onEvent(p, offset, event)
			if !handled {
				handled = needHandle
			}

			if !added {
				logger.Infof("%+v consume events slow down, %s exceed max cached events %d, current is %d, wait %s",
					wr.key,
					k,
					eventsCacheSize,
					w.cachedEventSize(),
					wait)
				allAdded = false
				return false
			}
		}
		return true
	})

	return allAdded, handled
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

func (wr *workerRunner) run() {
	if !wr.disableLoadShardState {
		logger.Infof("%s start load shard states", wr.key)
		for {
			err := wr.loadShardStates()
			if err == nil {
				break
			}
			time.Sleep(time.Second * 5)
		}
	}

	if !wr.disableStartConsumer {
		logger.Infof("%s start queue consumer", wr.key)
		for {
			err := wr.startQueueConsumer()
			if err == nil {
				break
			}
			time.Sleep(time.Second * 5)
		}
	}

	hasEvent := true
	for {
		if wr.isStopped() {
			wr.doStop()
			return
		}

		if !hasEvent {
			time.Sleep(time.Millisecond * 10)
		}

		for key := range wr.completedOffsetsPerHandle {
			delete(wr.completedOffsetsPerHandle, key)
		}

		hasEvent = false
		wr.workers.Range(func(k, v interface{}) bool {
			w := v.(worker)
			if w.isStopped() {
				w.close()
				wr.workers.Delete(k)
			} else if w.handleEvent(wr.completed) {
				hasEvent = true
			}
			return true
		})

		if len(wr.completedOffsetsPerHandle) > 0 {
			wr.consumer.Commit(wr.completedOffsetsPerHandle, wr.commitCB)

			wr.Lock()
			for p, v := range wr.completedOffsetsPerHandle {
				if wv, ok := wr.allWaitCommitOffsets[p]; ok && v >= wv {
					delete(wr.allWaitCommitOffsets, p)
				}
			}
			wr.Unlock()
		}
	}
}

func (wr *workerRunner) completed(p uint32, offset uint64) {
	if offset <= 0 {
		return
	}

	if value, ok := wr.completedOffsetsPerHandle[p]; ok {
		if offset > value {
			wr.completedOffsetsPerHandle[p] = offset
		}
	} else {
		wr.completedOffsetsPerHandle[p] = offset
	}
}

func (wr *workerRunner) commitCB(err error) {
	// just log it, commit next tick
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("%s commit offset failed with %+v", wr.key, err)
	}
}
