package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

var (
	emptyBMData = bytes.NewBuffer(nil)
	initBM      = roaring.NewBitmap()
)

func init() {
	initBM.WriteTo(emptyBMData)
}

// Engine the engine maintains all state information
type Engine interface {
	// Start start the engine
	Start() error
	// Stop stop the engine
	Stop() error
	// Create create a work flow definition meta
	Create(meta metapb.Workflow) (uint64, error)
	// Update update a work flow definition meta
	Update(meta metapb.Workflow) error
	// CreateInstance create a new work flow instance,
	// an instance may contain a lot of people, so an instance will be divided into many shards,
	// each shard handles some people's events.
	CreateInstance(workflowID uint64, crow []byte, maxPerShard uint64) (uint64, error)
	// DeleteInstance delete instance
	DeleteInstance(instanceID uint64) error
	// StartInstance start instance
	StartInstance(id uint64) error
	// InstanceCountState returns instance count state
	InstanceCountState(id uint64) (metapb.InstanceCountState, error)
	// InstanceStepState returns instance step state
	InstanceStepState(id uint64, name string) (metapb.StepState, error)
	// Step drivers the workflow instance
	Step(metapb.Event) error
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

	rangeCache sync.Map // instanceID -> []uint32{start, end}
}

func (eng *engine) Start() error {
	err := eng.store.Start(eng.doStep)
	if err != nil {
		return err
	}

	eng.runner.RunCancelableTask(eng.handleEvent)
	return nil
}

func (eng *engine) Stop() error {
	return eng.runner.Stop()
}

func (eng *engine) Create(meta metapb.Workflow) (uint64, error) {
	id := eng.store.RaftStore().MustAllocID()
	meta.ID = id
	err := eng.set(uint64Key(id), protoc.MustMarshal(&meta))
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (eng *engine) Update(meta metapb.Workflow) error {
	if meta.ID == 0 {
		return errors.New("missing workflow id")
	}

	return eng.set(uint64Key(meta.ID), protoc.MustMarshal(&meta))
}

func (eng *engine) CreateInstance(workflowID uint64, crow []byte, maxPerShard uint64) (uint64, error) {
	value, err := eng.get(uint64Key(workflowID))
	if err != nil {
		return 0, err
	}

	if len(value) == 0 {
		return 0, fmt.Errorf("missing workflow %d", workflowID)
	}

	snapshot := metapb.Workflow{}
	protoc.MustUnmarshal(&snapshot, value)
	instance := metapb.WorkflowInstance{
		ID:          eng.store.RaftStore().MustAllocID(),
		Snapshot:    snapshot,
		Crowd:       crow,
		MaxPerShard: maxPerShard,
	}

	err = eng.set(uint64Key(instance.ID), protoc.MustMarshal(&instance))
	if err != nil {
		return 0, err
	}

	return instance.ID, nil
}

func (eng *engine) DeleteInstance(id uint64) error {
	key := storage.InstanceStartKey(id)
	value, err := eng.get(key)
	if err != nil {
		return err
	}
	if len(value) > 0 {
		return fmt.Errorf("instance %d already started", id)
	}

	req := rpcpb.AcquireDeleteRequest()
	req.Key = uint64Key(id)
	_, err = eng.store.ExecCommand(req)
	rpcpb.ReleaseDeleteRequest(req)
	return err
}

func (eng *engine) StartInstance(id uint64) error {
	value, err := eng.get(storage.InstanceStartKey(id))
	if err != nil {
		return err
	}
	if len(value) > 0 {
		return nil
	}

	value, err = eng.get(uint64Key(id))
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return fmt.Errorf("instance %d not create", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	req := rpcpb.AcquireStartingInstanceRequest()
	req.Instance = instance
	_, err = eng.store.ExecCommand(req)
	rpcpb.ReleaseStartingInstanceRequest(req)

	return err
}

func (eng *engine) InstanceCountState(id uint64) (metapb.InstanceCountState, error) {
	value, err := eng.get(storage.InstanceStartKey(id))
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	if len(value) == 0 {
		return metapb.InstanceCountState{}, fmt.Errorf("instance %d not started", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	bm := bbutil.MustParseBM(instance.Crowd)
	m := make(map[string]*metapb.CountState)
	state := metapb.InstanceCountState{}
	state.Total = bm.GetCardinality()
	state.Snapshot = instance.Snapshot
	for _, step := range instance.Snapshot.Steps {
		m[step.Name] = &metapb.CountState{
			Step:  step.Name,
			Count: 0,
		}
	}

	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	for _, shard := range shards {
		key := storage.InstanceStateKey(instance.ID, shard.Minimum(), shard.Maximum()+1)
		stepState := metapb.WorkflowInstanceState{}
		value, err = eng.get(key)
		if err != nil {
			return metapb.InstanceCountState{}, err
		}

		if len(value) == 0 {
			return metapb.InstanceCountState{}, fmt.Errorf("missing step state key %+v", key)
		}

		protoc.MustUnmarshal(&stepState, value)
		for _, ss := range stepState.States {
			m[ss.Step.Name].Count += bbutil.MustParseBM(ss.Crowd).GetCardinality()
		}
	}

	for _, v := range m {
		state.States = append(state.States, *v)
	}

	return state, nil
}

func (eng *engine) InstanceStepState(id uint64, name string) (metapb.StepState, error) {
	value, err := eng.get(storage.InstanceStartKey(id))
	if err != nil {
		return metapb.StepState{}, err
	}

	if len(value) == 0 {
		return metapb.StepState{}, fmt.Errorf("instance %d not started", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	var valueStep metapb.Step
	valueBM := bbutil.AcquireBitmap()
	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	for _, shard := range shards {
		key := storage.InstanceStateKey(instance.ID, shard.Minimum(), shard.Maximum()+1)
		stepState := metapb.WorkflowInstanceState{}
		value, err = eng.get(key)
		if err != nil {
			return metapb.StepState{}, err
		}

		if len(value) == 0 {
			return metapb.StepState{}, fmt.Errorf("missing step state key %+v", key)
		}

		protoc.MustUnmarshal(&stepState, value)
		for _, ss := range stepState.States {
			if ss.Step.Name == name {
				valueStep = ss.Step
				valueBM = bbutil.BMOr(valueBM, bbutil.MustParseBM(ss.Crowd))
			}
		}
	}

	return metapb.StepState{
		Step:  valueStep,
		Crowd: bbutil.MustMarshalBM(valueBM),
	}, nil
}

func (eng *engine) Step(event metapb.Event) error {
	ranges, err := eng.maybeLoadInstanceStateShardRanges(event.InstanceID)
	if err != nil {
		return err
	}

	var start uint32
	var end uint32
	for _, r := range ranges {
		if event.UserID >= r[0] && event.UserID < r[1] {
			start = r[0]
			end = r[1]
			break
		}
	}

	if start == 0 && end == 0 {
		return fmt.Errorf("instance state range not found by user %d", event.InstanceID)
	}

	req := rpcpb.AcquireStepInstanceStateShardRequest()
	req.Event = event
	req.Start = start
	req.End = end

	_, err = eng.store.ExecCommand(req)
	rpcpb.ReleaseStepInstanceStateShardRequest(req)
	return err
}

func (eng *engine) doStep(id uint64, req *raftcmdpb.Request) (*raftcmdpb.Response, error) {
	customReq := rpcpb.StepInstanceStateShardRequest{}
	protoc.MustUnmarshal(&customReq, req.Cmd)

	var cb *stepCB
	found := false
	eng.workers.Range(func(key, value interface{}) bool {
		w := value.(*stateWorker)
		if w.matches(customReq.Event.InstanceID, customReq.Event.UserID) {
			found = true
			cb = acquireCB()
			cb.c = make(chan struct{})
			w.step(customReq.Event, cb)
			return false
		}

		return true
	})

	if !found {
		return nil, errWorkerNotFound
	}

	cb.wait()

	customResp := rpcpb.AcquireStepInstanceStateShardResponse()
	customResp.ID = customReq.ID
	resp := pb.AcquireResponse()
	resp.Value = protoc.MustMarshal(customResp)
	rpcpb.ReleaseStepInstanceStateShardResponse(customResp)
	return resp, nil
}

func (eng *engine) maybeLoadInstanceStateShardRanges(id uint64) ([][]uint32, error) {
	if ranges, ok := eng.rangeCache.Load(id); ok {
		return ranges.([][]uint32), nil
	}

	value, err := eng.get(storage.InstanceStartKey(id))
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, fmt.Errorf("instance %d not started", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	return eng.addShardRangesToCache(instance.ID, shards), nil
}

func (eng *engine) addShardRangesToCache(id uint64, shards []*roaring.Bitmap) [][]uint32 {
	if ranges, ok := eng.rangeCache.Load(id); ok {
		return ranges.([][]uint32)
	}

	var ranges [][]uint32
	for _, shard := range shards {
		ranges = append(ranges, []uint32{shard.Minimum(), shard.Maximum() + 1})
	}
	eng.rangeCache.Store(id, ranges)
	return ranges
}

func (eng *engine) Notifier() notify.Notifier {
	return eng.notifier
}

func (eng *engine) Storage() storage.Storage {
	return eng.store
}

func uint64Key(id uint64) []byte {
	return goetty.Uint64ToBytes(id)
}

func (eng *engine) get(key []byte) ([]byte, error) {
	req := rpcpb.AcquireGetRequest()
	req.Key = key

	value, err := eng.store.ExecCommand(req)
	rpcpb.ReleaseGetRequest(req)
	if err != nil {
		return nil, err
	}

	resp := rpcpb.GetResponse{}
	protoc.MustUnmarshal(&resp, value)
	return resp.Value, err
}

func (eng *engine) set(key, value []byte) error {
	req := rpcpb.AcquireSetRequest()
	req.Key = key
	req.Value = value

	_, err := eng.store.ExecCommand(req)
	rpcpb.ReleaseSetRequest(req)
	return err
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
				eng.doCreateInstanceStateShardComplete(id)
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
	case storage.InstanceStateRemovedEvent:
		eng.doStopInstanceState(event.Data.(metapb.WorkflowInstanceState))
	}
}

func (eng *engine) doStartInstanceState(state metapb.WorkflowInstanceState) {
	key := workerKey(state)
	if _, ok := eng.workers.Load(key); ok {
		log.Fatalf("BUG: start a exists state worker")
	}

	w, err := newStateWorker(key, state, eng)
	if err != nil {
		log.Errorf("create worker for state %+v failed with %+v",
			state,
			err)
		return
	}

	eng.workers.Store(w.key, w)
	w.run()
}

func (eng *engine) doStopInstanceState(state metapb.WorkflowInstanceState) {
	key := workerKey(state)
	if w, ok := eng.workers.Load(key); ok {
		eng.workers.Delete(key)
		w.(*stateWorker).stop()
	}
}

func (eng *engine) doStartInstance(instance metapb.WorkflowInstance) {
	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	eng.addShardRangesToCache(instance.ID, shards)

	for _, shard := range shards {
		state := metapb.WorkflowInstanceState{}
		state.TenantID = instance.Snapshot.TenantID
		state.WorkflowID = instance.Snapshot.ID
		state.InstanceID = instance.ID
		state.Start = shard.Minimum()
		state.End = shard.Maximum() + 1

		for _, step := range instance.Snapshot.Steps {
			state.States = append(state.States, metapb.StepState{
				Step:  step,
				Crowd: emptyBMData.Bytes(),
			})
		}

		state.States[0].Crowd = bbutil.MustMarshalBM(shard)
		if !eng.doCreateInstanceState(instance, state) {
			return
		}
	}

	eng.doCreateInstanceStateShardComplete(instance.ID)
}

func (eng *engine) doCreateInstanceState(instance metapb.WorkflowInstance, state metapb.WorkflowInstanceState) bool {
	_, err := eng.store.ExecCommand(&rpcpb.CreateInstanceStateShardRequest{
		State: state,
	})
	if err != nil {
		log.Errorf("create workflow instance failed with %+v, retry later", err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryNewInstance, instance)
		return false
	}

	return true
}

func (eng *engine) doCreateInstanceStateShardComplete(id uint64) {
	_, err := eng.store.ExecCommand(&rpcpb.StartedInstanceRequest{
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

func workerKey(state metapb.WorkflowInstanceState) string {
	return fmt.Sprintf("%d/[%d-%d)", state.InstanceID, state.Start, state.End)
}
