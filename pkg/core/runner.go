package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
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
	workers     map[string]*stateWorker
	eng         *engine
}

func newWorkerRunner(tid uint64, runnerIndex uint64, key string, eng *engine) *workerRunner {
	return &workerRunner{
		tid:         tid,
		runnerIndex: runnerIndex,
		key:         key,
		workers:     make(map[string]*stateWorker),
		eng:         eng,
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

func (wr *workerRunner) run() {
	logger.Infof("%s started", wr.key)

	for {
		if wr.stopped {
			logger.Infof("%s load instance shard states skipped by stopped", wr.key)
			break
		}

		loaded, err := wr.loadInstanceShardStates()
		if err == nil {
			logger.Infof("%s load %d instance shard states ",
				wr.key,
				loaded)
			break
		}

		metric.IncStorageFailed()
		logger.Errorf("%s load instance shard states failed with %+v, retry after 5s",
			wr.key,
			err)
		time.Sleep(time.Second * 5)
	}

	hasEvent := true
	for {
		if !hasEvent {
			time.Sleep(time.Millisecond * 10)
		}

		hasEvent = false
		wr.RLock()
		for key, w := range wr.workers {
			if w.isStopped() {
				delete(wr.workers, key)
			} else if w.handleEvent() {
				hasEvent = true
			}
		}

		if len(wr.workers) == 0 && wr.stopped {
			wr.RUnlock()

			logger.Infof("%s stopped", wr.key)
			return
		}
		wr.RUnlock()
	}
}
