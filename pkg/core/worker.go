package core

import (
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/expr"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/log"
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
	userEventAction
	updateCrowdEventAction
	updateWorkflowEventAction
)

type item struct {
	action int
	value  interface{}
	cb     *stepCB
}

type stateWorker struct {
	stopped      uint32
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
	consumer, err := queue.NewConsumer(state.TenantID,
		metapb.TenantInputGroup,
		eng.Storage(), []byte(key))
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
	atomic.StoreUint32(&w.stopped, 1)
	w.queue.Put(item{
		action: stopAction,
	})
}

func (w *stateWorker) isStopped() bool {
	return atomic.LoadUint32(&w.stopped) == 1
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
						(v.(item).cb).complete(task.ErrDisposed)
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
				case userEventAction:
					batch.cbs = append(batch.cbs, value.cb)
					w.doStepEvents(value.value.([]metapb.UserEvent), batch)
				case updateCrowdEventAction:
					value.cb.complete(w.doUpdateCrowd(value.value.([]byte)))
				case updateWorkflowEventAction:
					value.cb.complete(w.doUpdateWorkflow(value.value.(metapb.Workflow)))
				}
			}

			w.execBatch(batch)
		}
	}()

	w.consumer.Start(uint64(workerBatch), 0, w.onEvent)
	logger.Infof("worker %s started", w.key)
}

func (w *stateWorker) onEvent(offset uint64, items ...[]byte) error {
	logger.Debugf("worker %s consumer from queue, offset %d, %d items",
		w.key,
		offset,
		len(items))

	var event metapb.Event
	var userEvents []metapb.UserEvent
	for _, item := range items {
		event.Reset()
		protoc.MustUnmarshal(&event, item)
		logger.Debugf("worker %s consumer event %+v",
			w.key,
			event)

		switch event.Type {
		case metapb.UserType:
			if w.matches(event.User.UserID) {
				evt := *event.User
				evt.WorkflowID = w.state.WorkflowID
				userEvents = append(userEvents, evt)
			}
		case metapb.UpdateCrowdType:
			if event.UpdateCrowd.WorkflowID == w.state.WorkflowID &&
				event.UpdateCrowd.Index == w.state.Index {
				err := w.doSystemEvent(updateCrowdEventAction,
					event.UpdateCrowd.Crowd, userEvents)
				if err != nil {
					return err
				}
				userEvents = userEvents[:0]
			}
		case metapb.UpdateWorkflowType:
			if event.UpdateWorkflow.Workflow.ID == w.state.WorkflowID {
				err := w.doSystemEvent(updateWorkflowEventAction,
					event.UpdateWorkflow.Workflow, userEvents)
				if err != nil {
					return err
				}
				userEvents = userEvents[:0]
			}
		}
	}

	if len(userEvents) > 0 {
		return w.doEvent(userEventAction, userEvents)
	}

	return nil
}

func (w *stateWorker) doSystemEvent(action int, data interface{}, userEvents []metapb.UserEvent) error {
	if len(userEvents) > 0 {
		err := w.doEvent(userEventAction, userEvents)
		if err != nil {
			return err
		}
	}

	return w.doEvent(action, data)
}

func (w *stateWorker) doEvent(action int, data interface{}) error {
	cb := acquireCB()
	cb.reset()
	err := w.queue.Put(item{
		action: action,
		value:  data,
		cb:     cb,
	})
	if err != nil {
		releaseCB(cb)
		return err
	}

	err = cb.wait()
	releaseCB(cb)
	if err != nil {
		return err
	}

	return nil
}

func (w *stateWorker) execBatch(batch *executionbatch) {
	if len(batch.notifies) > 0 {
		for idx := range batch.notifies {
			batch.notifies[idx].FromAction = w.leaveActions[batch.notifies[idx].FromStep]
			batch.notifies[idx].ToAction = w.entryActions[batch.notifies[idx].ToStep]
		}

		w.retryDo("exec notify", batch, w.execNotify)
		log.Infof("worker %s added %d notifies to tenant %d",
			w.key,
			len(batch.notifies),
			w.state.TenantID)

		w.retryDo("exec update state", batch, w.execUpdate)
		log.Infof("worker %s state update to version %d",
			w.key,
			w.state.Version)
	}

	for _, c := range batch.cbs {
		c.complete(nil)
	}
	batch.reset()
}

func (w *stateWorker) retryDo(thing string, batch *executionbatch, fn func(*executionbatch) error) {
	times := 1
	after := 2
	maxAfter := 30
	for {
		if w.isStopped() {
			return
		}

		err := fn(batch)
		if err == nil {
			return
		}

		logger.Errorf("worker %s do %s failed %d times with %+v, retry after %d sec",
			w.key,
			thing,
			times,
			err,
			after)
		times++
		if after < maxAfter {
			after = after * times
			if after > maxAfter {
				after = maxAfter
			}
		}
		time.Sleep(time.Second * time.Duration(after))
	}
}

func (w *stateWorker) execNotify(batch *executionbatch) error {
	return w.eng.Notifier().Notify(w.state.TenantID, batch.notifies...)
}

func (w *stateWorker) execUpdate(batch *executionbatch) error {
	req := rpcpb.AcquireUpdateInstanceStateShardRequest()
	req.State = w.state
	_, err := w.eng.Storage().ExecCommand(req)
	return err
}

func (w *stateWorker) stepChanged(batch *executionbatch) error {
	if batch.crowd != nil {
		// filter other shard state crowds
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

func (w *stateWorker) doUpdateCrowd(crowd []byte) error {
	bm := bbutil.MustParseBM(crowd)
	new := bbutil.BMMinus(bm, w.totalCrowds)
	w.totalCrowds = bm.Clone()
	for idx, sc := range w.stepCrowds {
		if idx == 0 {
			sc.Or(new)
		}
		sc.And(bm)
	}
	for idx := range w.state.States {
		w.state.States[idx].Crowd = bbutil.MustMarshalBM(w.stepCrowds[idx])
	}
	w.state.Version++

	w.retryDo("exec update crowd", nil, w.execUpdate)
	log.Infof("worker %s crowd updated to %d numbers",
		w.key,
		w.totalCrowds.GetCardinality())
	return nil
}

func (w *stateWorker) doUpdateWorkflow(workflow metapb.Workflow) error {
	oldCrowds := make(map[string]*roaring.Bitmap)
	for idx, step := range w.state.States {
		oldCrowds[step.Step.Name] = w.stepCrowds[idx]
	}

	w.steps = make(map[string]excution)
	w.entryActions = make(map[string]string)
	w.leaveActions = make(map[string]string)

	var newStates []metapb.StepState
	var newCrowds []*roaring.Bitmap
	for _, step := range workflow.Steps {
		exec, err := newExcution(step.Name, step.Execution)
		if err != nil {
			return err
		}

		w.steps[step.Name] = exec
		w.entryActions[step.Name] = step.EnterAction
		w.leaveActions[step.Name] = step.LeaveAction

		if bm, ok := oldCrowds[step.Name]; ok {
			newCrowds = append(newCrowds, bm)
			newStates = append(newStates, metapb.StepState{
				Step:  step,
				Crowd: bbutil.MustMarshalBM(bm),
			})
			continue
		}

		newStates = append(newStates, metapb.StepState{
			Step:  step,
			Crowd: emptyBMData.Bytes(),
		})
		newCrowds = append(newCrowds, bbutil.AcquireBitmap())
	}
	w.stepCrowds = newCrowds

	w.state.States = newStates
	w.state.Version++

	w.retryDo("exec update workflow", nil, w.execUpdate)
	log.Infof("worker %s workflow updated", w.key)
	return nil
}

func (w *stateWorker) doStepEvents(events []metapb.UserEvent, batch *executionbatch) {
	for _, event := range events {
		log.Infof("worker %s step event %+v", w.key, event)

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
	log.Infof("worker %s step timer %s", name)

	err := w.steps[name].Execute(newExprCtx(metapb.UserEvent{
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
	event metapb.UserEvent
	eng   Engine
}

func newExprCtx(event metapb.UserEvent, eng Engine) expr.Ctx {
	return &exprCtx{
		event: event,
		eng:   eng,
	}
}

func (c *exprCtx) Event() metapb.UserEvent {
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
