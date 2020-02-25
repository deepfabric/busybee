package core

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	hbmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/crm"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/robfig/cron/v3"
)

var (
	emptyBMData = bytes.NewBuffer(nil)
	initBM      = roaring.NewBitmap()
	logger      log.Logger
)

func init() {
	initBM.WriteTo(emptyBMData)
	logger = log.NewLoggerWithPrefix("[engine]")
}

// Engine the engine maintains all state information
type Engine interface {
	// Start start the engine
	Start() error
	// Stop stop the engine
	Stop() error
	// StartInstance start instance, an instance may contain a lot of people,
	// so an instance will be divided into many shards, each shard handles some
	// people's events.
	StartInstance(workflow metapb.Workflow, crowd []byte, workers uint64) error
	// UpdateCrowd update instance crowd
	UpdateCrowd(id uint64, crowd []byte) error
	// UpdateInstance update running workflow
	UpdateWorkflow(workflow metapb.Workflow) error
	// StopInstance stop instance
	StopInstance(id uint64) error
	// InstanceCountState returns instance count state
	InstanceCountState(id uint64) (metapb.InstanceCountState, error)
	// InstanceStepState returns instance step state
	InstanceStepState(id uint64, name string) (metapb.StepState, error)
	// CreateTenantQueue create the tenant input and output queue
	CreateTenantQueue(id, partitions uint64) error
	// Notifier returns notifier
	Notifier() notify.Notifier
	// Storage returns storage
	Storage() storage.Storage
	// Service returns a crm service
	Service() crm.Service
	// AddCronJob add cron job
	AddCronJob(string, func()) (cron.EntryID, error)
	// StopCronJob stop the cron job
	StopCronJob(cron.EntryID)
}

// NewEngine returns a engine
func NewEngine(store storage.Storage, notifier notify.Notifier) (Engine, error) {
	return &engine{
		store:                  store,
		notifier:               notifier,
		eventC:                 store.WatchEvent(),
		retryNewInstanceC:      make(chan metapb.WorkflowInstance, 16),
		retryStoppingInstanceC: make(chan metapb.WorkflowInstance, 16),
		retryCompleteInstanceC: make(chan uint64, 1024),
		stopInstanceC:          make(chan uint64, 1024),
		runner:                 task.NewRunner(),
		cronRunner:             cron.New(cron.WithSeconds()),
		service:                crm.NewService(store),
	}, nil
}

type engine struct {
	opts       options
	store      storage.Storage
	service    crm.Service
	notifier   notify.Notifier
	runner     *task.Runner
	cronRunner *cron.Cron

	workers                sync.Map // key -> *worker
	eventC                 chan storage.Event
	retryNewInstanceC      chan metapb.WorkflowInstance
	retryStoppingInstanceC chan metapb.WorkflowInstance
	retryCompleteInstanceC chan uint64
	stopInstanceC          chan uint64
}

func (eng *engine) Start() error {
	err := eng.store.Start()
	if err != nil {
		return err
	}

	eng.cronRunner.Start()
	eng.runner.RunCancelableTask(eng.handleEvent)
	return nil
}

func (eng *engine) Stop() error {
	eng.cronRunner.Stop()
	return eng.runner.Stop()
}

func (eng *engine) StartInstance(workflow metapb.Workflow, crow []byte, workers uint64) error {
	if err := checkExcution(workflow); err != nil {
		return err
	}

	bm := bbutil.AcquireBitmap()
	err := bm.UnmarshalBinary(crow)
	if err != nil {
		logger.Errorf("start workflow-%d failed with %+v",
			workflow.ID,
			err)
		return err
	}

	logger.Infof("start workflow-%d with crow %d, workers %d",
		workflow.ID,
		bm.GetCardinality(),
		workers)

	value, err := eng.store.Get(storage.StartedInstanceKey(workflow.ID))
	if err != nil {
		return err
	}
	if len(value) > 0 {
		logger.Warningf("start workflow-%d already started",
			workflow.ID)
		return nil
	}

	instance := metapb.WorkflowInstance{
		Snapshot: workflow,
		Crowd:    crow,
		Workers:  workers,
	}

	req := rpcpb.AcquireStartingInstanceRequest()
	req.Instance = instance
	_, err = eng.store.ExecCommand(req)
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("start workflow-%d failed with %+v",
			workflow.ID,
			err)
	}

	return err
}

func (eng *engine) UpdateWorkflow(workflow metapb.Workflow) error {
	if err := checkExcution(workflow); err != nil {
		return err
	}

	value, err := eng.store.Get(storage.StartedInstanceKey(workflow.ID))
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return fmt.Errorf("workflow-%d is not started", workflow.ID)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	instance.Snapshot = workflow
	err = eng.doUpdateInstance(instance)
	if err != nil {
		return err
	}

	return eng.store.PutToQueue(instance.Snapshot.TenantID, 0,
		metapb.TenantInputGroup, protoc.MustMarshal(&metapb.Event{
			Type: metapb.UpdateWorkflowType,
			UpdateWorkflow: &metapb.UpdateWorkflowEvent{
				Workflow: workflow,
			},
		}))
}

func (eng *engine) UpdateCrowd(id uint64, crowd []byte) error {
	value, err := eng.store.Get(storage.StartedInstanceKey(id))
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return fmt.Errorf("workflow-%d is not started", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	instance.Crowd = crowd
	err = eng.doUpdateInstance(instance)
	if err != nil {
		return err
	}

	shards, err := eng.getInstanceShards(instance)
	if err != nil {
		return err
	}

	var old []*roaring.Bitmap
	for _, shard := range shards {
		bm := bbutil.AcquireBitmap()
		for _, state := range shard.States {
			bm.Or(bbutil.MustParseBM(state.Crowd))
		}
		old = append(old, bm)
	}

	bbutil.BMAlloc(bbutil.MustParseBM(crowd), old...)

	var events [][]byte
	for idx, bm := range old {
		events = append(events, protoc.MustMarshal(&metapb.Event{
			Type: metapb.UpdateCrowdType,
			UpdateCrowd: &metapb.UpdateCrowdEvent{
				WorkflowID: id,
				Index:      uint32(idx),
				Crowd:      bbutil.MustMarshalBM(bm),
			},
		}))
	}

	return eng.store.PutToQueue(instance.Snapshot.TenantID, 0,
		metapb.TenantInputGroup, events...)
}

func (eng *engine) StopInstance(id uint64) error {
	value, err := eng.store.Get(storage.StartedInstanceKey(id))
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}

	_, err = eng.store.ExecCommand(&rpcpb.StopInstanceRequest{
		WorkflowID: id,
	})
	return err
}

func (eng *engine) InstanceCountState(id uint64) (metapb.InstanceCountState, error) {
	value, err := eng.store.Get(storage.StartedInstanceKey(id))
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

	shards, err := eng.getInstanceShards(instance)
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	for _, stepState := range shards {
		for _, ss := range stepState.States {
			if _, ok := m[ss.Step.Name]; ok {
				m[ss.Step.Name].Count += bbutil.MustParseBM(ss.Crowd).GetCardinality()
			}
		}
	}

	for _, v := range m {
		state.States = append(state.States, *v)
	}

	return state, nil
}

func (eng *engine) InstanceStepState(id uint64, name string) (metapb.StepState, error) {
	value, err := eng.store.Get(storage.StartedInstanceKey(id))
	if err != nil {
		return metapb.StepState{}, err
	}

	if len(value) == 0 {
		return metapb.StepState{}, fmt.Errorf("instance %d not started", id)
	}

	instance := metapb.WorkflowInstance{}
	protoc.MustUnmarshal(&instance, value)

	var target metapb.Step
	valueBM := bbutil.AcquireBitmap()
	shards, err := eng.getInstanceShards(instance)
	if err != nil {
		return metapb.StepState{}, err
	}

	for _, stepState := range shards {
		for _, ss := range stepState.States {
			if ss.Step.Name == name {
				target = ss.Step
				valueBM = bbutil.BMOr(valueBM, bbutil.MustParseBM(ss.Crowd))
			}
		}
	}

	return metapb.StepState{
		Step:  target,
		Crowd: bbutil.MustMarshalBM(valueBM),
	}, nil
}

func (eng *engine) CreateTenantQueue(id, partitions uint64) error {
	err := eng.store.Set(storage.QueueMetadataKey(id, metapb.TenantInputGroup),
		goetty.Uint64ToBytes(partitions))
	if err != nil {
		return err
	}

	err = eng.store.Set(storage.QueueMetadataKey(id, metapb.TenantOutputGroup),
		goetty.Uint64ToBytes(1))
	if err != nil {
		return err
	}

	var shards []hbmetapb.Shard
	for i := uint64(0); i < partitions; i++ {
		shards = append(shards, hbmetapb.Shard{
			Group: uint64(metapb.TenantInputGroup),
			Start: storage.PartitionKey(id, i),
			End:   storage.PartitionKey(id, i+1),
		})
	}
	shards = append(shards, hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(id, 0),
		End:   storage.PartitionKey(id, 1),
	})

	return eng.store.RaftStore().AddShards(shards...)
}

func (eng *engine) Notifier() notify.Notifier {
	return eng.notifier
}

func (eng *engine) Storage() storage.Storage {
	return eng.store
}

func (eng *engine) Service() crm.Service {
	return eng.service
}

func (eng *engine) AddCronJob(cronExpr string, fn func()) (cron.EntryID, error) {
	return eng.cronRunner.AddFunc(cronExpr, fn)
}

func (eng *engine) StopCronJob(id cron.EntryID) {
	eng.cronRunner.Remove(id)
}

func (eng *engine) doUpdateInstance(instance metapb.WorkflowInstance) error {
	instance.Version++
	req := rpcpb.AcquireUpdateInstanceRequest()
	req.Instance = instance
	_, err := eng.store.ExecCommand(req)
	return err
}

func (eng *engine) getInstanceShards(instance metapb.WorkflowInstance) ([]metapb.WorkflowInstanceState, error) {
	from := storage.InstanceShardKey(instance.Snapshot.ID, 0)
	end := storage.InstanceShardKey(instance.Snapshot.ID, uint32(instance.Workers))

	var shards []metapb.WorkflowInstanceState
	for {
		values, err := eng.store.Scan(from, end, instance.Workers, 0)
		if err != nil {
			return nil, err
		}

		if len(values) == 0 {
			break
		}

		for _, value := range values {
			if !storage.RunningInstanceState(value) {
				return nil, fmt.Errorf("has stopped instance shard")
			}

			shard := metapb.WorkflowInstanceState{}
			protoc.MustUnmarshal(&shard, storage.OriginInstanceStatePBValue(value[1:]))
			shards = append(shards, shard)
		}

		from = storage.InstanceShardKey(instance.Snapshot.ID, shards[len(shards)-1].Index+1)
	}

	return shards, nil
}

func (eng *engine) handleEvent(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Infof("handler instance task stopped")
			return
		case event, ok := <-eng.eventC:
			if ok {
				eng.doEvent(event)
			}
		case instance, ok := <-eng.retryNewInstanceC:
			if ok {
				eng.doStartInstanceEvent(instance)
			}
		case instance, ok := <-eng.retryStoppingInstanceC:
			if ok {
				eng.doStoppingInstanceEvent(instance)
			}
		case id, ok := <-eng.retryCompleteInstanceC:
			if ok {
				eng.doCreateInstanceStateShardComplete(id)
			}
		case id, ok := <-eng.stopInstanceC:
			if ok {
				eng.doStopInstance(id)
			}
		}
	}
}

func (eng *engine) doEvent(event storage.Event) {
	switch event.EventType {
	case storage.InstanceLoadedEvent:
		eng.doStartInstanceEvent(event.Data.(metapb.WorkflowInstance))
	case storage.InstanceStartedEvent:
		eng.doStartedInstanceEvent(event.Data.(metapb.WorkflowInstance))
	case storage.InstanceStoppingEvent:
		eng.doStoppingInstanceEvent(event.Data.(metapb.WorkflowInstance))
	case storage.InstanceStoppedEvent:
		eng.doStoppedInstanceEvent(event.Data.(uint64))
	case storage.InstanceStateLoadedEvent:
		eng.doStartInstanceStateEvent(event.Data.(metapb.WorkflowInstanceState))
	case storage.InstanceStateRemovedEvent:
		eng.doInstanceStateRemovedEvent(event.Data.(metapb.WorkflowInstanceState))
	}
}

func (eng *engine) doStartInstanceStateEvent(state metapb.WorkflowInstanceState) {
	logger.Infof("create workflow-%d shard %d",
		state.WorkflowID,
		state.Index)

	key := workerKey(state)
	if _, ok := eng.workers.Load(key); ok {
		logger.Fatalf("BUG: start a exists state worker")
	}

	now := time.Now().Unix()
	if state.StopAt != 0 && now >= state.StopAt {
		return
	}

	for {
		w, err := newStateWorker(key, state, eng)
		if err != nil {
			logger.Errorf("create worker %s failed with %+v",
				key,
				err)
			continue
		}

		eng.workers.Store(w.key, w)
		w.run()

		if state.StopAt != 0 {
			after := time.Second * time.Duration(state.StopAt-now)
			util.DefaultTimeoutWheel().Schedule(after, eng.stopWorker, w)
		}
		break
	}

}

func (eng *engine) stopWorker(arg interface{}) {
	w := arg.(*stateWorker)
	eng.workers.Delete(w.key)
	w.stop()
}

func (eng *engine) doInstanceStateRemovedEvent(state metapb.WorkflowInstanceState) {
	logger.Infof("stop workflow-%d shard %d, moved to the other node",
		state.WorkflowID,
		state.Index)

	key := workerKey(state)
	if w, ok := eng.workers.Load(key); ok {
		eng.workers.Delete(key)
		w.(*stateWorker).stop()
	}
}

func (eng *engine) doStartInstanceEvent(instance metapb.WorkflowInstance) {
	logger.Infof("starting workflow-%d",
		instance.Snapshot.ID)

	var stopAt int64
	if instance.Snapshot.Duration > 0 {
		stopAt = time.Now().Add(time.Second * time.Duration(instance.Snapshot.Duration)).Unix()
	}

	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.Workers)
	for index, shard := range shards {
		state := metapb.WorkflowInstanceState{}
		state.TenantID = instance.Snapshot.TenantID
		state.WorkflowID = instance.Snapshot.ID
		state.Index = uint32(index)
		state.StopAt = stopAt

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

	eng.doCreateInstanceStateShardComplete(instance.Snapshot.ID)
}

func (eng *engine) doStartedInstanceEvent(instance metapb.WorkflowInstance) {
	if instance.Snapshot.Duration == 0 {
		logger.Infof("workflow-%d started",
			instance.Snapshot.ID)
		return
	}

	now := time.Now().Unix()
	after := instance.Snapshot.Duration - (now - instance.StartedAt)
	if after <= 0 {
		eng.addToInstanceStop(instance.Snapshot.ID)
		return
	}

	util.DefaultTimeoutWheel().Schedule(time.Second*time.Duration(after), eng.addToInstanceStop, instance.Snapshot.ID)
	logger.Infof("workflow-%d started, duration %d seconds",
		instance.Snapshot.ID,
		after)
}

func (eng *engine) doStoppingInstanceEvent(instance metapb.WorkflowInstance) {
	logger.Infof("stop workflow-%d %d workers",
		instance.Snapshot.ID,
		instance.Workers)
	n := uint32(instance.Workers)
	for index := uint32(0); index < n; index++ {
		req := rpcpb.AcquireRemoveInstanceStateShardRequest()
		req.WorkflowID = instance.Snapshot.ID
		req.Index = index

		_, err := eng.store.ExecCommand(req)
		if err != nil {
			metric.IncStorageFailed()
			logger.Errorf("stop workflow-%d failed with %+v, retry later",
				instance.Snapshot.ID,
				err)
			util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval,
				eng.addToRetryStoppingInstance,
				instance)
			return
		}
	}
}

func (eng *engine) doStoppedInstanceEvent(id uint64) {
	var removed []interface{}
	eng.workers.Range(func(key, value interface{}) bool {
		w := value.(*stateWorker)
		if w.state.WorkflowID == id {
			removed = append(removed, key)
		}
		return true
	})

	for _, key := range removed {
		if w, ok := eng.workers.Load(key); ok {
			eng.stopWorker(w)
		}
	}

	logger.Infof("workflow-%d stopped",
		id)
}

func (eng *engine) doCreateInstanceState(instance metapb.WorkflowInstance, state metapb.WorkflowInstanceState) bool {
	_, err := eng.store.ExecCommand(&rpcpb.CreateInstanceStateShardRequest{
		State: state,
	})
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("create workflow-%d state %s failed with %+v, retry later",
			instance.Snapshot.ID,
			workerKey(state),
			err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryNewInstance, instance)
		return false
	}

	return true
}

func (eng *engine) doCreateInstanceStateShardComplete(id uint64) {
	_, err := eng.store.ExecCommand(&rpcpb.StartedInstanceRequest{
		WorkflowID: id,
	})
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("start workflow-%d state failed with %+v, retry later",
			id, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryCompleteInstance, id)
		return
	}
}

func (eng *engine) doStopInstance(id uint64) {
	_, err := eng.store.ExecCommand(&rpcpb.StopInstanceRequest{
		WorkflowID: id,
	})
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("stopping workflow-%d failed with %+v, retry later",
			id, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToInstanceStop, id)
		return
	}
}

func (eng *engine) addToRetryNewInstance(arg interface{}) {
	eng.retryNewInstanceC <- arg.(metapb.WorkflowInstance)
}

func (eng *engine) addToRetryStoppingInstance(arg interface{}) {
	eng.retryStoppingInstanceC <- arg.(metapb.WorkflowInstance)
}

func (eng *engine) addToRetryCompleteInstance(arg interface{}) {
	eng.retryCompleteInstanceC <- arg.(uint64)
}

func (eng *engine) addToInstanceStop(id interface{}) {
	eng.stopInstanceC <- id.(uint64)
}

func workerKey(state metapb.WorkflowInstanceState) string {
	return fmt.Sprintf("%d/%d",
		state.WorkflowID,
		state.Index)
}
