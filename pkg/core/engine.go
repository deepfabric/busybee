package core

import (
	"bytes"
	"context"
	"sync"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/task"
	"github.com/pilosa/pilosa/roaring"
)

var (
	emptyBMData = bytes.NewBuffer(nil)
	initBM      = roaring.NewBTreeBitmap()
)

func init() {
	initBM.WriteTo(emptyBMData)
}

// Engine the engine maintains all state information
type Engine interface {
	// Start start the engine
	Start() error
	// Step drivers the workflow instance
	Step(context.Context, metapb.Event) error
	// Notifier returns notifier
	Notifier() notify.Notifier
	// Storage returns storage
	Storage() storage.Storage
}

// NewEngine returns a engine
func NewEngine(store storage.Storage, notifier notify.Notifier) (Engine, error) {
	return &engine{
		store:                  store,
		notifier:               notifier,
		eventC:                 store.WatchEvent(),
		retryNewInstanceC:      make(chan metapb.WorkflowInstance, 1024),
		retryCompleteInstanceC: make(chan uint64, 1024),
		runner:                 task.NewRunner(),
	}, nil
}

type engine struct {
	opts     options
	store    storage.Storage
	notifier notify.Notifier
	runner   *task.Runner

	workers                sync.Map // key -> *worker
	eventC                 chan storage.Event
	retryNewInstanceC      chan metapb.WorkflowInstance
	retryCompleteInstanceC chan uint64
}

func (eng *engine) Start() error {
	eng.runner.RunCancelableTask(eng.handleEvent)
	return nil
}

func (eng *engine) Step(ctx context.Context, event metapb.Event) error {
	var cb *stepCB
	found := false
	eng.workers.Range(func(key, value interface{}) bool {
		w := value.(*stateWorker)
		if w.matches(event.UserID) {
			found = true
			cb = acquireCB()
			cb.ctx = ctx
			cb.c = make(chan error)
			w.step(event, cb)
			return false
		}

		return true
	})

	if !found {
		return ErrWorkerNotFound
	}

	return cb.wait()
}

func (eng *engine) Notifier() notify.Notifier {
	return eng.notifier
}

func (eng *engine) Storage() storage.Storage {
	return eng.store
}

func (eng *engine) handleEvent(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("handler instance task stopped")
			return
		case event, ok := <-eng.eventC:
			if ok {
				eng.doEvent(event)
			}
		case instance, ok := <-eng.retryNewInstanceC:
			if ok {
				eng.doStartInstance(instance)
			}
		case id, ok := <-eng.retryCompleteInstanceC:
			if ok {
				eng.doCreateInstanceComplete(id)
			}
		}
	}
}

func (eng *engine) doEvent(event storage.Event) {
	switch event.EventType {
	case storage.InstanceLoadedEvent:
		eng.doStartInstance(event.Data.(metapb.WorkflowInstance))
	case storage.InstanceStateLoadedEvent:
		eng.doStartInstanceState(event.Data.(metapb.WorkflowInstanceState))
	case storage.InstanceStateUpdatedEvent:
		eng.doUpdateInstanceState(event.Data.(metapb.WorkflowInstanceState))
	case storage.InstanceStateRemovedEvent:
		eng.doStopInstanceState(event.Data.(metapb.WorkflowInstanceState))
	}
}

func (eng *engine) doStartInstanceState(state metapb.WorkflowInstanceState) {
	key := hack.SliceToString(storage.InstanceStateKey(state.InstanceID, state.Start, state.End))

	if _, ok := eng.workers.Load(key); ok {
		log.Fatalf("BUG: start a exists state worker")
	}

	w, err := newStateWorker(key, state)
	if err != nil {
		log.Errorf("create worker for state %+v failed with %+v",
			state,
			err)
		return
	}

	eng.workers.Store(w.key, w)
	w.run()
}

func (eng *engine) doUpdateInstanceState(state metapb.WorkflowInstanceState) {
	key := hack.SliceToString(storage.InstanceStateKey(state.InstanceID, state.Start, state.End))
	w, ok := eng.workers.Load(key)
	if !ok {
		eng.doStartInstanceState(state)
		return
	}

	w.(*stateWorker).instanceStateUpdated(state)
}

func (eng *engine) doStopInstanceState(state metapb.WorkflowInstanceState) {
	key := hack.SliceToString(storage.InstanceStateKey(state.InstanceID, state.Start, state.End))
	if w, ok := eng.workers.Load(key); ok {
		eng.workers.Delete(key)
		w.(*stateWorker).stop()
	}
}

func (eng *engine) doStartInstance(instance metapb.WorkflowInstance) {
	state := metapb.WorkflowInstanceState{}
	state.TenantID = instance.Snapshot.TenantID
	state.WorkflowID = instance.Snapshot.ID
	state.InstanceID = instance.ID

	bm := bbutil.MustParseBM(instance.Crowd)
	buf := bbutil.AcquireBuf()
	stateBM := bbutil.AcquireBitmap()
	start := uint64(0)
	count := uint64(0)

	defer bbutil.ReleaseBuf(buf)

	itr := bm.Iterator()
	for {
		value, eof := itr.Next()
		if eof {
			break
		}

		if start == 0 {
			stateBM = bbutil.AcquireBitmap()
			for _, step := range instance.Snapshot.Steps {
				state.States = append(state.States, metapb.StepState{
					Step:  step,
					Crowd: emptyBMData.Bytes(),
				})
			}
			start = value
		}

		stateBM.Add(value)
		count++

		if count == eng.opts.maxCrowdShardSize {
			buf.Reset()
			_, err := stateBM.WriteTo(buf)
			if err != nil {
				log.Fatalf("BUG: bitmap write failed with %+v", err)
			}

			state.States[0].Crowd = buf.Bytes()
			state.Start = start
			state.End = value + 1

			start = 0
			if !eng.doCreateInstanceState(instance, state) {
				return
			}
		}
	}

	if start > 0 {
		buf.Reset()
		bbutil.MustWriteTo(stateBM, buf)
		state.States[0].Crowd = buf.Bytes()
		state.Start = start
		state.End = stateBM.Max() + 1
		if !eng.doCreateInstanceState(instance, state) {
			return
		}
	}

	eng.doCreateInstanceComplete(instance.ID)
}

func (eng *engine) doCreateInstanceState(instance metapb.WorkflowInstance, state metapb.WorkflowInstanceState) bool {
	_, err := eng.store.ExecCommand(&rpcpb.CreateStateRequest{
		State: state,
	})
	if err != nil {
		log.Errorf("create workflow instance failed with %+v, retry later", err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryNewInstance, instance)
		return false
	}

	return true
}

func (eng *engine) doCreateInstanceComplete(id uint64) {
	_, err := eng.store.ExecCommand(&rpcpb.RemoveWFRequest{
		InstanceID: id,
	})
	if err != nil {
		log.Errorf("delete workflow instance failed with %+v, retry later", err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryCompleteInstance, id)
	}
}

func (eng *engine) addToRetryNewInstance(arg interface{}) {
	eng.retryNewInstanceC <- arg.(metapb.WorkflowInstance)
}

func (eng *engine) addToRetryCompleteInstance(arg interface{}) {
	eng.retryCompleteInstanceC <- arg.(uint64)
}
