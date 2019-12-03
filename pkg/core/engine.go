package core

import (
	"bytes"
	"context"
	"sync"

	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/log"
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
	Step(metapb.Event) error
}

// NewEngine returns a engine
func NewEngine(store storage.Storage) (Engine, error) {
	return &engine{
		store:  store,
		eventC: store.WatchEvent(),
		runner: task.NewRunner(),
	}, nil
}

type engine struct {
	opts   options
	store  storage.Storage
	runner *task.Runner

	workers                sync.Map // instanceID + idx -> *worker
	eventC                 chan storage.Event
	retryNewInstanceC      chan metapb.WorkflowInstance
	retryCompleteInstanceC chan uint64
}

func (eng *engine) Start() error {
	eng.runner.RunCancelableTask(eng.handleEvent)
	return nil
}

func (eng *engine) Step(metapb.Event) error {
	return nil
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
	case storage.InstanceStateRemovedEvent:
	}
}

func (eng *engine) doStartInstance(instance metapb.WorkflowInstance) {
	bm := roaring.NewBTreeBitmap()
	_, _, err := bm.ImportRoaringBits(instance.Crowd, false, false, 0)
	if err != nil {
		log.Fatalf("parse bitmap failed with %+v", err)
	}

	state := metapb.WorkflowInstanceState{}
	state.InstanceID = instance.ID

	buf := bytes.NewBuffer(nil)
	stateBM := roaring.NewBTreeBitmap()
	start := uint64(0)
	count := uint64(0)

	itr := bm.Iterator()
	for {
		value, eof := itr.Next()
		if eof {
			break
		}

		if start == 0 {
			stateBM.Containers.Reset()
			for _, step := range instance.Snapshot.Steps {
				state.States = append(state.States, metapb.StepState{
					Name:  step.Name,
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
		_, err := stateBM.WriteTo(buf)
		if err != nil {
			log.Fatalf("BUG: bitmap write failed with %+v", err)
		}

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
