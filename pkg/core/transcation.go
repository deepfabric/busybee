package core

import (
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
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

	event *metapb.UserEvent
	index int
	buf   *goetty.ByteBuf

	kvCache     map[string][]byte
	preLoadKeys [][]byte
	completed   uint64
	completedC  chan struct{}
}

func newTransaction() *transaction {
	return &transaction{
		buf:        goetty.NewByteBuf(32),
		event:      &metapb.UserEvent{},
		kvCache:    make(map[string][]byte),
		completedC: make(chan struct{}, 1),
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

func (tran *transaction) close() {
	tran.buf.Release()
	close(tran.completedC)
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
	tran.doUserEvents(events)
}

func (tran *transaction) doUserEvents(events []metapb.UserEvent) {
	tran.doPreLoad(events)

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
			}

			return
		}
	}
}

func (tran *transaction) doPreLoad(events []metapb.UserEvent) {
	tran.resetPreLoad()

	for i := range events {
		tran.resetExprCtx()
		tran.event.UserID = events[i].UserID
		tran.event.Data = events[i].Data

		for j, crowd := range tran.stepCrowds {
			if crowd.Contains(events[i].UserID) {
				err := tran.w.steps[tran.w.state.States[j].Step.Name].Pre(tran, true, tran.addPreLoadKey)
				if err != nil {
					tran.err = err
					return
				}
				break
			}
		}
	}

	if len(tran.preLoadKeys) == 0 {
		return
	}

	for _, key := range tran.preLoadKeys {
		req := rpcpb.AcquireGetRequest()
		req.Key = key
		tran.w.eng.Storage().AsyncExecCommand(req, tran.onLoadKey, key)
	}

	<-tran.completedC
}

func (tran *transaction) addPreLoadKey(key []byte) {
	tran.preLoadKeys = append(tran.preLoadKeys, key)
}

func (tran *transaction) onLoadKey(arg interface{}, value []byte, err error) {
	completed := atomic.AddUint64(&tran.completed, 1)

	if err != nil {
		logger.Errorf("worker %s pre load %+v failed with %+v", arg, err)
	} else {
		resp := rpcpb.AcquireBytesResponse()
		protoc.MustUnmarshal(resp, value)
		tran.kvCache[hack.SliceToString(arg.([]byte))] = resp.Value
		rpcpb.ReleaseBytesResponse(resp)
	}

	if completed == uint64(len(tran.preLoadKeys)) {
		tran.completedC <- struct{}{}
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
		tran.addChanged(changedCtx{from, to, ctx.who,
			tran.w.state.States[tran.w.stepIndexs[to]].Step.TTL})
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
}

func (tran *transaction) resetExprCtx() {
	tran.index = 0
	tran.event.UserID = 0
	tran.event.Data = nil
	tran.event.TenantID = tran.w.state.TenantID
	tran.event.WorkflowID = tran.w.state.WorkflowID
	tran.event.InstanceID = tran.w.state.InstanceID

	tran.buf.Clear()
}

func (tran *transaction) resetPreLoad() {
	tran.completed = 0
	tran.preLoadKeys = tran.preLoadKeys[:0]
	for key := range tran.kvCache {
		delete(tran.kvCache, key)
	}
}

func (tran *transaction) Event() *metapb.UserEvent {
	return tran.event
}

func (tran *transaction) Profile(key []byte) []byte {
	return storage.ProfileKey(tran.event.TenantID, tran.event.UserID)
}

func (tran *transaction) KV(key []byte) ([]byte, error) {
	attr := hack.SliceToString(key)
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
