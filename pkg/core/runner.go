package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/queue"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
)

func runnerKey(state *metapb.WorkerRunner) string {
	return fmt.Sprintf("WR[%d-%d]", state.ID, state.Index)
}

func (eng *engine) doStartRunnerEvent(state *metapb.WorkerRunner) {
	key := runnerKey(state)
	if _, ok := eng.runners.Load(key); ok {
		return
	}

	wr := newWorkerRunner(state.ID, state.Index, key, eng)
	eng.runners.Store(key, wr)

	logger.Infof("%s created", key)
	wr.start()
}

func (eng *engine) doStopRunnerEvent(state *metapb.WorkerRunner) {
	key := runnerKey(state)
	value, ok := eng.runners.Load(key)
	if !ok {
		return
	}

	logger.Infof("%s stop", key)
	eng.runners.Delete(key)
	value.(*workerRunner).stop()
}

type workerRunner struct {
	sync.RWMutex

	key         string
	tid         uint64
	runnerIndex uint64
	stopped     bool
	eng         *engine
	workers     map[string]*stateWorker
	events      map[string][]metapb.Event
	consumer    queue.AsyncConsumer

	completedOffsets map[uint32]uint64
}

func newWorkerRunner(tid uint64, runnerIndex uint64, key string, eng *engine) *workerRunner {
	return &workerRunner{
		tid:              tid,
		runnerIndex:      runnerIndex,
		key:              key,
		workers:          make(map[string]*stateWorker),
		eng:              eng,
		completedOffsets: make(map[uint32]uint64),
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
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	if wr.consumer != nil {
		wr.consumer.Stop()
	}

	wr.stopped = true
	for _, w := range wr.workers {
		w.stop()
	}
}

func (wr *workerRunner) addWorker(w *stateWorker) {
	wr.Lock()
	defer wr.Unlock()

	if wr.stopped {
		return
	}

	if _, ok := wr.workers[w.key]; ok {
		return
	}

	wr.workers[w.key] = w
	w.init()
}

func (wr *workerRunner) removeWorker(key string) {
	wr.Lock()
	defer wr.Unlock()

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

	for _, w := range wr.workers {
		if w.state.WorkflowID == wid {
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

			w, err := newStateWorker(workerKey(state), state, wr.eng)
			if err != nil {
				return 0, err
			}

			wr.addWorker(w)

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

		for _, w := range wr.workers {
			if !w.isStopped() {
				w.onEvent(p, offset, &event)
			}
		}

		offset++
	}
}

func (wr *workerRunner) run() {
	logger.Infof("%s started", wr.key)

	for {
		if wr.stopped {
			logger.Infof("%s load instance shard states skipped by stopped", wr.key)
			break
		}

		err := wr.loadShardStates()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 5)
	}

	for {
		if wr.stopped {
			logger.Infof("%s create queue consumer skipped by stopped", wr.key)
			break
		}

		err := wr.startQueueConsumer()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 5)
	}

	hasEvent := true
	for {
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
				delete(wr.workers, key)
				delete(wr.events, key)
			} else if w.handleEvent(wr.completed) {
				hasEvent = true
			}
		}

		if len(wr.workers) == 0 && wr.stopped {
			wr.RUnlock()

			logger.Infof("%s stopped", wr.key)
			return
		}
		wr.RUnlock()

		if len(wr.completedOffsets) > 0 {
			err := wr.consumer.Commit(wr.completedOffsets)
			// just log it, commit next tick
			if err != nil {
				metric.IncStorageFailed()
				logger.Errorf("%s commit offset failed with %+v", err)
			}
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
