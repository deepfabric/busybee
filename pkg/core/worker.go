package core

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/task"
	"github.com/robfig/cron/v3"
)

var (
	workerBatch = int64(32)
)

const (
	stopAction = iota
	timerAction
	eventAction
)

type item struct {
	action int
	value  interface{}
	cb     *stepCB
}

type stateWorker struct {
	key          string
	eng          Engine
	state        metapb.WorkflowInstanceState
	totalCrowds  *roaring.Bitmap
	stepCrowds   []*roaring.Bitmap
	steps        map[string]excution
	entryActions map[string]string
	leaveActions map[string]string
	queue        *task.Queue

	cronIDs []cron.EntryID
}

func newStateWorker(key string, state metapb.WorkflowInstanceState, eng Engine) (*stateWorker, error) {
	var stepCrowds []*roaring.Bitmap
	for _, state := range state.States {
		stepCrowds = append(stepCrowds, bbutil.MustParseBM(state.Crowd))
	}

	w := &stateWorker{
		key:          key,
		state:        state,
		eng:          eng,
		stepCrowds:   stepCrowds,
		totalCrowds:  bbutil.BMOr(stepCrowds...),
		steps:        make(map[string]excution),
		queue:        task.New(1024),
		entryActions: make(map[string]string),
		leaveActions: make(map[string]string),
	}

	for _, stepState := range state.States {
		exec, err := newExcution(stepState.Step.Name, stepState.Step.Execution)
		if err != nil {
			return nil, err
		}

		if stepState.Step.Execution.Timer != nil {
			name := stepState.Step.Name
			id, err := w.eng.AddCronJob(stepState.Step.Execution.Timer.Cron, func() {
				w.queue.Put(item{
					action: timerAction,
					value:  name,
				})
			})
			if err != nil {
				return nil, err
			}

			w.cronIDs = append(w.cronIDs, id)
		}

		w.steps[stepState.Step.Name] = exec
		w.entryActions[stepState.Step.Name] = stepState.Step.EnterAction
		w.leaveActions[stepState.Step.Name] = stepState.Step.LeaveAction
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
					for _, id := range w.cronIDs {
						w.eng.StopCronJob(id)
					}
					log.Infof("worker %s stopped", w.key)
					return
				}

				switch value.action {
				case timerAction:
					w.doStepTimer(batch, value.value.(string))
				case eventAction:
					batch.cbs = append(batch.cbs, value.cb)
					w.doStepEvent(value.value.(metapb.Event), batch)
				}
			}

			w.execBatch(batch)
		}
	}()
}

func (w *stateWorker) execBatch(batch *executionbatch) {
	if len(batch.notifies) > 0 {
		for idx := range batch.notifies {
			batch.notifies[idx].FromAction = w.leaveActions[batch.notifies[idx].FromStep]
			batch.notifies[idx].ToAction = w.entryActions[batch.notifies[idx].ToStep]
		}

		err := w.eng.Notifier().Notify(w.state.InstanceID, batch.notifies...)
		if err != nil {
			log.Fatalf("%s instance %d state notify failed with %+v",
				w.key,
				w.state.InstanceID,
				err)
		}

		req := rpcpb.AcquireUpdateInstanceStateShardRequest()
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
	if batch.crowd != nil {
		batch.crowd.And(w.totalCrowds)
	}

	doNotify := false
	for idx := range w.state.States {
		changed := false
		if w.state.States[idx].Step.Name == batch.from {
			changed = true
			if nil != batch.crowd {
				afterChanged := bbutil.BMAndnot(w.stepCrowds[idx], batch.crowd)
				if w.stepCrowds[idx].GetCardinality() == afterChanged.GetCardinality() {
					changed = false
				} else {
					w.stepCrowds[idx] = afterChanged
				}
			} else {
				w.stepCrowds[idx].Remove(batch.event.UserID)
			}
		} else if w.state.States[idx].Step.Name == batch.to {
			changed = true
			if nil != batch.crowd {
				afterChanged := bbutil.BMOr(w.stepCrowds[idx], batch.crowd)
				if w.stepCrowds[idx].GetCardinality() == afterChanged.GetCardinality() {
					changed = false
				} else {
					w.stepCrowds[idx] = afterChanged
				}
			} else {
				w.stepCrowds[idx].Add(batch.event.UserID)
			}
		}

		if changed {
			w.state.States[idx].Crowd = bbutil.MustMarshalBM(w.stepCrowds[idx])
			w.state.Version++
			doNotify = true
		}
	}

	batch.next(doNotify)
	return nil
}

func (w *stateWorker) step(event metapb.Event, cb *stepCB) {
	w.queue.Put(item{
		action: eventAction,
		value:  event,
		cb:     cb,
	})
}

func (w *stateWorker) doStepEvent(event metapb.Event, batch *executionbatch) {
	for idx, crowd := range w.stepCrowds {
		if crowd.Contains(event.UserID) {
			err := w.steps[w.state.States[idx].Step.Name].Execute(newExprCtx(event, w.eng),
				w.stepChanged, batch)
			if err != nil {
				log.Errorf("step event %+v failed with %+v", event, err)
			}
			break
		}
	}
}

func (w *stateWorker) doStepTimer(batch *executionbatch, name string) {
	err := w.steps[name].Execute(newExprCtx(metapb.Event{
		TenantID:   w.state.TenantID,
		InstanceID: w.state.InstanceID,
		WorkflowID: w.state.WorkflowID,
	}, w.eng),
		w.stepChanged, batch)
	if err != nil {
		log.Errorf("worker trigger timer failed with %+v", err)
	}
}

type exprCtx struct {
	event metapb.Event
	eng   Engine
}

func newExprCtx(event metapb.Event, eng Engine) expr.Ctx {
	return &exprCtx{
		event: event,
		eng:   eng,
	}
}

func (c *exprCtx) Event() metapb.Event {
	return c.event
}

func (c *exprCtx) Profile(key []byte) ([]byte, error) {
	values, err := c.eng.Service().GetProfileField(c.event.TenantID, c.event.UserID, hack.SliceToString(key))
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	return hack.StringToSlice(values[0]), nil
}

func (c *exprCtx) KV(key []byte) ([]byte, error) {
	return c.eng.Storage().Get(key)
}
