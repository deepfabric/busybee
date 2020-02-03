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
	"github.com/deepfabric/busybee/pkg/notify"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/queue"
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
	// StartInstance start instance
	// an instance may contain a lot of people, so an instance will be divided into many shards,
	// each shard handles some people's events.
	StartInstance(workflow metapb.Workflow, crow []byte, maxPerShard uint64) error
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

func (eng *engine) StartInstance(workflow metapb.Workflow, crow []byte, maxPerShard uint64) error {
	bm := bbutil.AcquireBitmap()
	err := bm.UnmarshalBinary(crow)
	if err != nil {
		logger.Errorf("start workflow-%d failed with %+v",
			workflow.ID,
			err)
		return err
	}

	logger.Infof("start workflow-%d with crow %d, maxPerShard %d",
		workflow.ID,
		bm.GetCardinality(),
		maxPerShard)

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
		Snapshot:    workflow,
		Crowd:       crow,
		MaxPerShard: maxPerShard,
	}

	req := rpcpb.AcquireStartingInstanceRequest()
	req.Instance = instance
	_, err = eng.store.ExecCommand(req)
	if err != nil {
		logger.Errorf("start workflow-%d failed with %+v",
			workflow.ID,
			err)
	}

	return err
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

	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	for _, shard := range shards {
		key := storage.InstanceShardKey(instance.Snapshot.ID, shard.Minimum(), shard.Maximum()+1)
		stepState := metapb.WorkflowInstanceState{}
		value, err = eng.store.Get(key)
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
	value, err := eng.store.Get(storage.StartedInstanceKey(id))
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
		key := storage.InstanceShardKey(instance.Snapshot.ID, shard.Minimum(), shard.Maximum()+1)
		stepState := metapb.WorkflowInstanceState{}
		value, err = eng.store.Get(key)
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
			Start: queue.PartitionKey(id, i),
			End:   queue.PartitionKey(id, i+1),
		})
	}
	shards = append(shards, hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: queue.PartitionKey(id, 0),
		End:   queue.PartitionKey(id, 1),
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
		value := event.Data.(metapb.WorkflowInstance)
		logger.Infof("do event with starting workflow-%d",
			value.Snapshot.ID)
		eng.doStartInstanceEvent(value)
	case storage.InstanceStartedEvent:
		value := event.Data.(metapb.WorkflowInstance)
		logger.Infof("do event with workflow-%d started",
			value.Snapshot.ID)
		eng.doStartedInstanceEvent(value)
	case storage.InstanceStoppingEvent:
		value := event.Data.(metapb.WorkflowInstance)
		logger.Infof("do event with stopping workflow-%d",
			value.Snapshot.ID)
		eng.doStoppingInstanceEvent(value)
	case storage.InstanceStoppedEvent:
		value := event.Data.(uint64)
		logger.Infof("do event with workflow-%d stopped",
			value)
		eng.doStoppedInstanceEvent(value)
	case storage.InstanceStateLoadedEvent:
		value := event.Data.(metapb.WorkflowInstanceState)
		logger.Infof("do event with create workflow-%d shard [%d,%d)",
			value.WorkflowID,
			value.Start,
			value.End)
		eng.doStartInstanceStateEvent(value)
	case storage.InstanceStateRemovedEvent:
		value := event.Data.(metapb.WorkflowInstanceState)
		logger.Infof("do event with stop workflow-%d shard [%d,%d)",
			value.WorkflowID,
			value.Start,
			value.End)
		eng.doInstanceStateRemovedEvent(value)
	}
}

func (eng *engine) doStartInstanceStateEvent(state metapb.WorkflowInstanceState) {
	key := workerKey(state)
	if _, ok := eng.workers.Load(key); ok {
		logger.Fatalf("BUG: start a exists state worker")
	}

	now := time.Now().Unix()
	if state.StopAt != 0 && now >= state.StopAt {
		return
	}

	w, err := newStateWorker(key, state, eng)
	if err != nil {
		logger.Errorf("create worker %s failed with %+v",
			w.key,
			err)
		return
	}

	eng.workers.Store(w.key, w)
	w.run()

	if state.StopAt != 0 {
		after := time.Second * time.Duration(state.StopAt-now)
		util.DefaultTimeoutWheel().Schedule(after, eng.stopWorker, w)
	}
}

func (eng *engine) stopWorker(arg interface{}) {
	w := arg.(*stateWorker)
	eng.workers.Delete(w.key)
	w.stop()
}

func (eng *engine) doInstanceStateRemovedEvent(state metapb.WorkflowInstanceState) {
	key := workerKey(state)
	if w, ok := eng.workers.Load(key); ok {
		eng.workers.Delete(key)
		w.(*stateWorker).stop()
	}
}

func (eng *engine) doStartInstanceEvent(instance metapb.WorkflowInstance) {
	var stopAt int64
	if instance.Snapshot.Duration > 0 {
		stopAt = time.Now().Add(time.Second * time.Duration(instance.Snapshot.Duration)).Unix()
	}

	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	for _, shard := range shards {
		state := metapb.WorkflowInstanceState{}
		state.TenantID = instance.Snapshot.TenantID
		state.WorkflowID = instance.Snapshot.ID
		state.Start = shard.Minimum()
		state.End = shard.Maximum() + 1
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
		return
	}

	now := time.Now().Unix()
	after := instance.Snapshot.Duration - (now - instance.StartedAt)
	if after <= 0 {
		eng.addToInstanceStop(instance.Snapshot.ID)
	} else {
		util.DefaultTimeoutWheel().Schedule(time.Second*time.Duration(after), eng.addToInstanceStop, instance.Snapshot.ID)
	}
}

func (eng *engine) doStoppingInstanceEvent(instance metapb.WorkflowInstance) {
	bm := bbutil.MustParseBM(instance.Crowd)
	shards := bbutil.BMSplit(bm, instance.MaxPerShard)
	for _, shard := range shards {
		req := rpcpb.AcquireRemoveInstanceStateShardRequest()
		req.WorkflowID = instance.Snapshot.ID
		req.Start = shard.Minimum()
		req.End = shard.Maximum() + 1

		_, err := eng.store.ExecCommand(req)
		if err != nil {
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
}

func (eng *engine) doCreateInstanceState(instance metapb.WorkflowInstance, state metapb.WorkflowInstanceState) bool {
	_, err := eng.store.ExecCommand(&rpcpb.CreateInstanceStateShardRequest{
		State: state,
	})
	if err != nil {
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
		logger.Errorf("start workflow-%d state failed with %+v, retry later",
			id, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToRetryCompleteInstance, id)
		return
	}

	logger.Infof("workflow-%d started", id)
}

func (eng *engine) doStopInstance(id uint64) {
	_, err := eng.store.ExecCommand(&rpcpb.StopInstanceRequest{
		WorkflowID: id,
	})
	if err != nil {
		logger.Errorf("stopping workflow-%d failed with %+v, retry later",
			id, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.addToInstanceStop, id)
		return
	}

	logger.Infof("stopping workflow-%d", id)
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
	return fmt.Sprintf("%d/[%d-%d)",
		state.WorkflowID,
		state.Start,
		state.End)
}
