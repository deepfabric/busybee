package core

import (
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
)

var (
	workerBatch = int64(32)
)

const (
	stopAction = iota
	timerAction
	eventAction
	updateStateAction
)

type item struct {
	action int
	value  interface{}
	cb     *stepCB
}

type stateWorker struct {
	key        string
	eng        Engine
	state      metapb.WorkflowInstanceState
	stepCrowds []*roaring.Bitmap
	timeout    goetty.Timeout
	interval   time.Duration
	steps      map[string]excution
	queue      *task.Queue
}

func newStateWorker(key string, state metapb.WorkflowInstanceState) (*stateWorker, error) {
	var stepCrowds []*roaring.Bitmap
	for _, state := range state.States {
		stepCrowds = append(stepCrowds, bbutil.MustParseBM(state.Crowd))
	}

	w := &stateWorker{
		key:        key,
		state:      state,
		stepCrowds: stepCrowds,
		steps:      make(map[string]excution),
		queue:      task.New(1024),
	}
	if state.States[0].Step.Execution.Type == metapb.Timer {
		w.interval = time.Second * time.Duration(state.States[0].Step.Execution.Timer.Interval)
	}

	for _, stepState := range state.States {
		exec, err := newExcution(stepState.Step.Name, stepState.Step.Execution, w)
		if err != nil {
			return nil, err
		}

		w.steps[stepState.Step.Name] = exec
	}

	return w, nil
}

func (w *stateWorker) stop() {
	w.queue.Put(item{
		action: stopAction,
	})
}

func (w *stateWorker) matches(id uint64, uid uint32) bool {
	return w.state.InstanceID == id && uid >= w.state.Start && uid < w.state.End
}

func (w *stateWorker) run() {
	log.Infof("worker %s started", w.key)
	go func() {
		items := make([]interface{}, workerBatch, workerBatch)
		batch := newExecutionbatch()

		for {
			n, err := w.queue.Get(workerBatch, items)
			if err != nil {
				log.Fatalf("BUG: fetch from work queue failed with %+v", err)
			}

			for i := int64(0); i < n; i++ {
				value := items[i].(item)
				if value.action == stopAction {
					w.queue.Dispose()
					w.timeout.Stop()
					log.Infof("worker %s stopped", w.key)
					return
				}

				switch value.action {
				case timerAction:
					w.doStepTimer(batch)
				case eventAction:
					select {
					case <-value.cb.ctx.Done():
						value.cb.c <- ErrTimeout
						releaseCB(value.cb)
					default:
						batch.cbs = append(batch.cbs, value.cb)
						w.doStepEvent(value.value.(metapb.Event), batch)
					}
				case updateStateAction:
					w.execBatch(batch)
					w.doInstanceStateUpdated(value.value.(metapb.WorkflowInstanceState))
				}
			}

			w.execBatch(batch)
		}
	}()
}

func (w *stateWorker) execBatch(batch *executionbatch) {
	if len(batch.notifies) > 0 {
		err := w.eng.Notifier().Notify(w.state.InstanceID, batch.notifies...)
		if err != nil {
			log.Fatalf("instance state notify failed with %+v", err)
		}

		req := rpcpb.AcquireUpdateStateRequest()
		req.State = w.state
		_, err = w.eng.Storage().ExecCommand(req)
		if err != nil {
			log.Fatalf("update instance state failed with %+v", err)
		}
	}

	for _, c := range batch.cbs {
		releaseCB(c)
	}
	batch.reset()
}

func (w *stateWorker) stepChanged(batch *executionbatch) error {
	for idx, state := range w.state.States {
		changed := false
		if state.Step.Name == batch.from {
			changed = true
			if nil != batch.crowd {
				w.stepCrowds[idx] = bbutil.BMXOr(w.stepCrowds[idx], batch.crowd)
			} else {
				w.stepCrowds[idx].Remove(batch.event.UserID)
			}
		} else if state.Step.Name == batch.to {
			changed = true
			if nil != batch.crowd {
				w.stepCrowds[idx] = bbutil.BMOr(w.stepCrowds[idx], batch.crowd)
			} else {
				w.stepCrowds[idx].Add(batch.event.UserID)
			}
		}

		if changed {
			w.state.States[idx].Crowd = bbutil.MustMarshalBM(w.stepCrowds[idx])
			w.state.Version++
		}
	}

	batch.next()
	return nil
}

func (w *stateWorker) instanceStateUpdated(state metapb.WorkflowInstanceState) {
	w.queue.Put(item{
		action: updateStateAction,
		value:  state,
	})
}

func (w *stateWorker) step(event metapb.Event, cb *stepCB) {
	w.queue.Put(item{
		action: eventAction,
		value:  event,
		cb:     cb,
	})
}

func (w *stateWorker) doInstanceStateUpdated(state metapb.WorkflowInstanceState) {
	if w.state.Version >= state.Version {
		log.Infof("ignore state, %d >= %d", w.state.Version, state.Version)
		return
	}

	for idx, s := range state.States {
		w.stepCrowds[idx] = bbutil.MustParseBM(s.Crowd)
	}
}

func (w *stateWorker) doStepEvent(event metapb.Event, batch *executionbatch) {
	for idx, crowd := range w.stepCrowds {
		if crowd.Contains(event.UserID) {
			err := w.steps[w.state.States[idx].Step.Name].Execute(event, w.stepChanged, batch)
			if err != nil {
				log.Errorf("step event %+v failed with %+v", event, err)
			}
			break
		}
	}
}

func (w *stateWorker) doStepTimer(batch *executionbatch) {
	err := w.steps[w.state.States[0].Step.Name].Execute(metapb.Event{
		TenantID:   w.state.TenantID,
		InstanceID: w.state.InstanceID,
		WorkflowID: w.state.WorkflowID,
	}, w.stepChanged, batch)
	if err != nil {
		log.Errorf("worker trigger timer failed with %+v", err)
	}
	w.schedule()
}

func (w *stateWorker) schedule() {
	if w.interval == 0 {
		return
	}

	w.timeout, _ = util.DefaultTimeoutWheel().Schedule(w.interval, w.doTimeout, nil)
}

func (w *stateWorker) doTimeout(arg interface{}) {
	w.queue.Put(item{
		action: timerAction,
	})
}

func (w *stateWorker) Bitmap(key []byte) (*roaring.Bitmap, error) {
	req := rpcpb.AcquireGetRequest()
	req.Key = key
	value, err := w.eng.Storage().ExecCommand(req)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return bbutil.AcquireBitmap(), nil
	}

	return bbutil.MustParseBM(value), nil
}
