package core

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

var (
	pool = sync.Pool{
		New: func() interface{} {
			return util.AcquireBitmap()
		},
	}
)

func acquireBM() *roaring.Bitmap {
	return pool.Get().(*roaring.Bitmap)
}

func releaseBM(value *roaring.Bitmap) {
	value.Clear()
	pool.Put(value)
}

type transaction struct {
	w          *stateWorker
	err        error
	stepCrowds []*roaring.Bitmap
	changes    []changedCtx
	cbs        []*stepCB
	restart    bool

	event   *metapb.UserEvent
	index   int
	buf     *goetty.ByteBuf
	kvCache map[string][]byte
}

func newTransaction() *transaction {
	return &transaction{
		buf:     goetty.NewByteBuf(32),
		kvCache: make(map[string][]byte),
		event:   &metapb.UserEvent{},
	}
}

func (tran *transaction) resetExprCtx() {
	tran.index = 0
	tran.event.UserID = 0
	tran.event.Data = nil
	tran.event.TenantID = tran.w.state.TenantID
	tran.event.WorkflowID = tran.w.state.WorkflowID
	tran.event.InstanceID = tran.w.state.InstanceID

	tran.buf.Clear()
	for key := range tran.kvCache {
		delete(tran.kvCache, key)
	}
}

func (tran *transaction) start(w *stateWorker) {
	tran.w = w
	tran.resetExprCtx()

	for _, crowd := range w.stepCrowds {
		v := acquireBM()
		v.Or(crowd)
		tran.stepCrowds = append(tran.stepCrowds, v)
	}
}

func (tran *transaction) doStepTimerEvent(item item) {
	idx := item.value.(int)
	if tran.err != nil {
		return
	}

	step := tran.w.state.States[idx]
	if step.Step.Execution.Type != metapb.Timer {
		return
	}

	tran.resetExprCtx()
	tran.index = idx

	target := who{}
	if step.Step.Execution.Timer.UseStepCrowdToDrive {
		if tran.stepCrowds[idx].GetCardinality() <= 0 {
			return
		}
		target.users = tran.stepCrowds[idx].Clone()
	}

	err := tran.w.steps[step.Step.Name].Execute(tran, tran, target)
	if err != nil {
		metric.IncWorkflowWorkerFailed()
		logger.Errorf("worker %s trigger timer failed with %+v",
			tran.w.key,
			err)
		tran.err = err
		return
	}
}

func (tran *transaction) doStepUserEvents(item item) {
	if item.cb != nil {
		tran.cbs = append(tran.cbs, item.cb)
	}
	if tran.err != nil {
		return
	}

	events := item.value.([]metapb.UserEvent)
	for idx := range events {
		tran.doUserEvent(&events[idx])
	}
}

func (tran *transaction) doUserEvent(event *metapb.UserEvent) {
	logger.Debugf("worker %s step event %+v", tran.w.key, event)
	for idx, crowd := range tran.stepCrowds {
		if crowd.Contains(event.UserID) {
			tran.resetExprCtx()
			tran.index = idx
			tran.event.UserID = event.UserID
			tran.event.Data = event.Data

			err := tran.w.steps[tran.w.state.States[idx].Step.Name].Execute(tran, tran, who{event.UserID, nil})
			if err != nil {
				metric.IncWorkflowWorkerFailed()
				logger.Errorf("worker %s step event %+v failed with %+v",
					tran.w.key,
					event,
					err)
				tran.err = err
				return
			}

			return
		}
	}
}

// this function will called by every step exectuion, if the target crowd or user
// removed to other step.
func (tran *transaction) stepChanged(ctx changedCtx) {
	// the users is the timer step to filter a crowd on the all workflow crowd,
	// so it's contains other shards crowds.
	if ctx.who.users != nil {
		// filter other shard state crowds
		ctx.who.users.And(tran.w.totalCrowds)
	}

	from := tran.w.stepIndexs[ctx.from]
	to := tran.w.stepIndexs[ctx.to]

	tran.removeFromStep(from, ctx.who)
	if tran.moveToStep(to, ctx.who) {
		ctx.ttl = tran.w.state.States[to].Step.TTL
		tran.addChanged(ctx)
		tran.maybeTriggerDirectSteps(to, ctx)
	}
}

func (tran *transaction) maybeTriggerDirectSteps(current int, ctx changedCtx) {
	if !tran.w.isDirectStep(ctx.to) {
		return
	}

	from := ctx.to
	to := tran.w.directNexts[ctx.to]
	for {
		tran.addChanged(changedCtx{from, to, ctx.who, 0})
		if !tran.w.isDirectStep(to) {
			break
		}
		from = to
		to = tran.w.directNexts[to]
	}

	tran.removeFromStep(current, ctx.who)
	tran.moveToStep(tran.w.stepIndexs[to], ctx.who)
}

func (tran *transaction) removeFromStep(idx int, target who) bool {
	return target.removeFrom(tran.stepCrowds[idx])
}

func (tran *transaction) moveToStep(idx int, target who) bool {
	return target.appendTo(tran.stepCrowds[idx])
}

func (tran *transaction) addChanged(changed changedCtx) {
	for idx := range tran.changes {
		if tran.changes[idx].from == changed.from &&
			tran.changes[idx].to == changed.to {
			changed.who.appendTo(tran.changes[idx].who.users)
			return
		}
	}

	ctx := changedCtx{changed.from, changed.to, who{0, acquireBM()}, changed.ttl}
	changed.who.appendTo(ctx.who.users)
	tran.changes = append(tran.changes, ctx)
}

func (tran *transaction) reset() {
	if len(tran.changes) > 0 {
		for idx := range tran.stepCrowds {
			releaseBM(tran.stepCrowds[idx])
			tran.stepCrowds[idx] = nil
		}
	}

	for idx := range tran.changes {
		releaseBM(tran.changes[idx].who.users)
		tran.changes[idx].who.users = nil
		tran.changes[idx].who.user = 0
	}

	tran.stepCrowds = tran.stepCrowds[:0]
	tran.changes = tran.changes[:0]
	tran.cbs = tran.cbs[:0]
	tran.err = nil
	tran.restart = false
}

func (tran *transaction) Event() *metapb.UserEvent {
	return tran.event
}

func (tran *transaction) Profile(key []byte) ([]byte, error) {
	attr := tran.profileKey(key, tran.event.UserID)
	if value, ok := tran.kvCache[attr]; ok {
		return value, nil
	}

	value, err := tran.w.eng.Service().GetProfileField(tran.event.TenantID, tran.event.UserID, hack.SliceToString(key))
	if err != nil {
		return nil, err
	}

	tran.kvCache[attr] = value
	return value, nil
}

func (tran *transaction) KV(key []byte) ([]byte, error) {
	attr := tran.kvKey(key)
	if value, ok := tran.kvCache[attr]; ok {
		return value, nil
	}

	value, err := tran.w.eng.Storage().Get(key)
	if err != nil {
		return nil, err
	}

	tran.kvCache[attr] = value
	return value, nil
}

func (tran *transaction) TotalCrowd() *roaring.Bitmap {
	return tran.w.totalCrowds
}

func (tran *transaction) StepCrowd() *roaring.Bitmap {
	return tran.w.stepCrowds[tran.index]
}

func (tran *transaction) profileKey(key []byte, id uint32) string {
	tran.buf.MarkWrite()
	tran.buf.WriteByte(0)
	tran.buf.WriteUInt32(id)
	tran.buf.Write(key)
	return hack.SliceToString(tran.buf.WrittenDataAfterMark())
}

func (tran *transaction) kvKey(key []byte) string {
	tran.buf.MarkWrite()
	tran.buf.WriteByte(1)
	tran.buf.Write(key)
	return hack.SliceToString(tran.buf.WrittenDataAfterMark())
}
