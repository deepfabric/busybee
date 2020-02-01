package core

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/robfig/cron/v3"
)

var (
	workerBatch = int64(16)
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
	cronIDs      []cron.EntryID
	consumer     queue.Consumer
}

func newStateWorker(key string, state metapb.WorkflowInstanceState, eng Engine) (*stateWorker, error) {
	consumer, err := queue.NewConsumer(state.TenantID, metapb.TenantInputGroup, eng.Storage(), []byte(key))
	if err != nil {
		return nil, err
	}

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
		consumer:     consumer,
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

func (w *stateWorker) matches(uid uint32) bool {
	return w.totalCrowds.Contains(uid)
}

func (w *stateWorker) run() {
	go func() {
		items := make([]interface{}, workerBatch, workerBatch)
		batch := newExecutionbatch()

		for {
			n, err := w.queue.Get(workerBatch, items)
			if err != nil {
				logger.Fatalf("BUG: fetch from work queue failed with %+v", err)
			}

			for i := int64(0); i < n; i++ {
				value := items[i].(item)
				if value.action == stopAction {
					w.consumer.Stop()

					for _, v := range w.queue.Dispose() {
						releaseCB(v.(item).cb)
					}

					for _, id := range w.cronIDs {
						w.eng.StopCronJob(id)
					}
					logger.Infof("worker %s stopped", w.key)
					return
				}

				switch value.action {
				case timerAction:
					w.doStepTimer(batch, value.value.(string))
				case eventAction:
					batch.cbs = append(batch.cbs, value.cb)
					w.doStepEvents(value.value.([]metapb.Event), batch)
				}
			}

			w.execBatch(batch)
		}
	}()

	w.consumer.Start(uint64(workerBatch), w.onConsume)
	logger.Infof("worker %s started", w.key)
}

func (w *stateWorker) onConsume(offset uint64, items ...[]byte) error {
	logger.Debugf("worker %s consumer from queue, offset %d, %d items",
		w.key,
		offset,
		len(items))

	var events []metapb.Event
	var event metapb.Event
	for _, item := range items {
		protoc.MustUnmarshal(&event, item)
		if w.matches(event.UserID) {
			event.WorkflowID = w.state.WorkflowID
			events = append(events, event)
		}
	}

	if len(events) > 0 {
		cb := acquireCB()
		err := w.queue.Put(item{
			action: eventAction,
			value:  events,
			cb:     cb,
		})
		if err != nil {
			releaseCB(cb)
			return err
		}

		cb.wait()
	}

	return nil
}

func (w *stateWorker) execBatch(batch *executionbatch) {
	if len(batch.notifies) > 0 {
		for idx := range batch.notifies {
			batch.notifies[idx].FromAction = w.leaveActions[batch.notifies[idx].FromStep]
			batch.notifies[idx].ToAction = w.entryActions[batch.notifies[idx].ToStep]
		}

		err := w.eng.Notifier().Notify(w.state.WorkflowID, batch.notifies...)
		if err != nil {
			logger.Fatalf("worker %s notify failed with %+v",
				w.key,
				err)
		}

		req := rpcpb.AcquireUpdateInstanceStateShardRequest()
		req.State = w.state
		_, err = w.eng.Storage().ExecCommand(req)
		if err != nil {
			logger.Fatalf("worker %s update failed with %+v",
				w.key,
				err)
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

func (w *stateWorker) doStepEvents(events []metapb.Event, batch *executionbatch) {
	for _, event := range events {
		for idx, crowd := range w.stepCrowds {
			if crowd.Contains(event.UserID) {
				err := w.steps[w.state.States[idx].Step.Name].Execute(newExprCtx(event, w.eng),
					w.stepChanged, batch)
				if err != nil {
					logger.Errorf("worker %s step event %+v failed with %+v",
						w.key,
						event,
						err)
				}
				break
			}
		}
	}
}

func (w *stateWorker) doStepTimer(batch *executionbatch, name string) {
	err := w.steps[name].Execute(newExprCtx(metapb.Event{
		TenantID:   w.state.TenantID,
		WorkflowID: w.state.WorkflowID,
	}, w.eng),
		w.stepChanged, batch)
	if err != nil {
		logger.Errorf("worker %s trigger timer failed with %+v",
			w.key,
			err)
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
	value, err := c.eng.Service().GetProfileField(c.event.TenantID, c.event.UserID, hack.SliceToString(key))
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (c *exprCtx) KV(key []byte) ([]byte, error) {
	return c.eng.Storage().Get(key)
}
