package core

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
	hbmetapb "github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/crm"
	"github.com/deepfabric/busybee/pkg/crowd"
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
	"github.com/fagongzi/util/uuid"
	"github.com/robfig/cron/v3"
)

const (
	maxPerWorker = 1000000
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
	StartInstance(workflow metapb.Workflow, loader metapb.BMLoader, crowd []byte) (uint64, error)
	// LastInstance returns last instance
	LastInstance(id uint64) (*metapb.WorkflowInstance, error)
	// HistoryInstance returens a workflow instance snapshot
	HistoryInstance(uint64, uint64) (*metapb.WorkflowInstanceSnapshot, error)
	// UpdateCrowd update instance crowd
	UpdateCrowd(id uint64, loader metapb.BMLoader, crowdMeta []byte) error
	// UpdateInstance update running workflow
	UpdateWorkflow(workflow metapb.Workflow) error
	// StopInstance stop instance
	StopInstance(id uint64) error
	// InstanceCountState returns instance count state
	InstanceCountState(id uint64) (metapb.InstanceCountState, error)
	// InstanceStepState returns instance step state
	InstanceStepState(id uint64, name string) (metapb.StepState, error)
	// TenantInit init tenant, and create the tenant input and output queue
	TenantInit(metapb.Tenant) error
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

type createWorkerAction struct {
	bootstrap bool
	state     metapb.WorkflowInstanceWorkerState
	completed *actionCompleted
}

type actionCompleted struct {
	completed uint64
	total     uint64
}

func (c *actionCompleted) done() bool {
	return atomic.AddUint64(&c.completed, 1) == c.total
}

// NewEngine returns a engine
func NewEngine(store storage.Storage, notifier notify.Notifier, opts ...Option) (Engine, error) {
	eng := &engine{
		opts:                   &options{},
		store:                  store,
		notifier:               notifier,
		eventC:                 store.WatchEvent(),
		retryNewInstanceC:      make(chan *metapb.WorkflowInstance, 16),
		retryStoppingInstanceC: make(chan *metapb.WorkflowInstance, 16),
		retryCompleteInstanceC: make(chan uint64, 1024),
		stopInstanceC:          make(chan uint64, 1024),
		runner:                 task.NewRunner(),
		cronRunner:             cron.New(cron.WithSeconds()),
		service:                crm.NewService(store),
		loaders:                make(map[metapb.BMLoader]crowd.Loader),
	}

	for _, opt := range opts {
		opt(eng.opts)
	}

	eng.opts.adjust()
	return eng, nil
}

type engine struct {
	opts       *options
	store      storage.Storage
	service    crm.Service
	notifier   notify.Notifier
	runner     *task.Runner
	cronRunner *cron.Cron

	runners                sync.Map // key -> worker runner
	eventC                 chan storage.Event
	retryNewInstanceC      chan *metapb.WorkflowInstance
	retryStoppingInstanceC chan *metapb.WorkflowInstance
	retryCompleteInstanceC chan uint64
	stopInstanceC          chan uint64
	workerCreateC          []chan createWorkerAction
	op                     uint64

	loaders map[metapb.BMLoader]crowd.Loader
}

func (eng *engine) Start() error {
	eng.initBMLoaders()
	eng.initWorkerCreate()
	eng.initCron()
	eng.initEvent()
	eng.initReport()

	err := eng.store.Start()
	if err != nil {
		return err
	}

	return nil
}

func (eng *engine) initReport() {
	eng.runner.RunCancelableTask(func(ctx context.Context) {
		timer := time.NewTicker(time.Second * 5)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				c := 0
				rc := 0
				eng.runners.Range(func(key, value interface{}) bool {
					c += value.(*workerRunner).workerCount()
					rc++
					return true
				})

				logger.Debugf("%d running instance state shards", c)
				metric.SetWorkflowShardsCount(c)
				metric.SetRunnersCount(rc)
			}
		}
	})
}

func (eng *engine) workerCount() int {
	c := 0
	eng.runners.Range(func(key, value interface{}) bool {
		c += value.(*workerRunner).workerCount()
		return true
	})
	return c
}

func (eng *engine) initCron() {
	eng.cronRunner.Start()
}

func (eng *engine) initEvent() {
	eng.runner.RunCancelableTask(eng.handleEvent)
}

func (eng *engine) initWorkerCreate() {
	for i := uint64(0); i < eng.opts.workerCreateCount; i++ {
		c := make(chan createWorkerAction, 256)
		eng.workerCreateC = append(eng.workerCreateC, c)
		eng.handleCreateWorker(c)
	}
}

func (eng *engine) Stop() error {
	eng.cronRunner.Stop()
	return eng.runner.Stop()
}

func (eng *engine) TenantInit(metadata metapb.Tenant) error {
	return eng.tenantInitWithReplicas(metadata, 0)
}

func (eng *engine) tenantInitWithReplicas(metadata metapb.Tenant, replicas uint32) error {
	if metadata.Runners == 0 {
		metadata.Runners = 1
	}

	if metadata.Input.Partitions == 0 {
		metadata.Input.Partitions = 1
		metadata.Input.ConsumerTimeout = 60
	}

	var shards []hbmetapb.Shard
	for i := uint64(0); i < metadata.Runners; i++ {
		metadata.RunnersState = append(metadata.RunnersState, false)
		shards = append(shards, hbmetapb.Shard{
			Group: uint64(metapb.TenantRunnerGroup),
			Start: storage.TenantRunnerKey(metadata.ID, i),
			End:   storage.TenantRunnerKey(metadata.ID, i+1),
			Data: protoc.MustMarshal(&metapb.CallbackAction{
				SetKV: &metapb.SetKVAction{
					KV: metapb.KV{
						Key: storage.TenantRunnerMetadataKey(metadata.ID, i),
						Value: protoc.MustMarshal(&metapb.WorkerRunner{
							ID:    metadata.ID,
							Index: i,
							State: metapb.WRRunning,
						}),
					},
					Group: metapb.TenantRunnerGroup,
				},
				UpdateTenantInitState: &metapb.TenantInitStateUpdateAction{
					ID:    metadata.ID,
					Index: int32(i),
					Group: metapb.TenantRunnerGroup,
				},
			}),
			DisableSplit:  true,
			LeastReplicas: replicas,
		})
	}

	for i := uint32(0); i < metadata.Input.Partitions; i++ {
		metadata.InputsState = append(metadata.InputsState, false)
		shards = append(shards, hbmetapb.Shard{
			Group: uint64(metapb.TenantInputGroup),
			Start: storage.PartitionKey(metadata.ID, i),
			End:   storage.PartitionKey(metadata.ID, i+1),
			Data: protoc.MustMarshal(&metapb.CallbackAction{
				SetKV: &metapb.SetKVAction{
					KV: metapb.KV{
						Key: storage.QueueMetaKey(metadata.ID, i),
						Value: protoc.MustMarshal(&metapb.QueueState{
							Partitions: metadata.Input.Partitions,
							Timeout:    metadata.Input.ConsumerTimeout,
							States:     make([]metapb.Partiton, metadata.Input.Partitions, metadata.Input.Partitions),
							CleanBatch: metadata.Input.CleanBatch,
							MaxAlive:   metadata.Input.MaxAlive,
						}),
					},
					Group: metapb.TenantInputGroup,
				},
				UpdateTenantInitState: &metapb.TenantInitStateUpdateAction{
					ID:    metadata.ID,
					Index: int32(i),
					Group: metapb.TenantInputGroup,
				},
			}),
			DisableSplit:  true,
			LeastReplicas: replicas,
		})
	}

	metadata.OutputsState = append(metadata.OutputsState, false)
	shards = append(shards, hbmetapb.Shard{
		Group: uint64(metapb.TenantOutputGroup),
		Start: storage.PartitionKey(metadata.ID, 0),
		End:   storage.PartitionKey(metadata.ID+1, 0),
		Data: protoc.MustMarshal(&metapb.CallbackAction{
			SetKV: &metapb.SetKVAction{
				KV: metapb.KV{
					Key: storage.QueueMetaKey(metadata.ID, 0),
					Value: protoc.MustMarshal(&metapb.QueueState{
						Partitions: metadata.Output.Partitions,
						Timeout:    metadata.Output.ConsumerTimeout,
						States:     make([]metapb.Partiton, metadata.Output.Partitions, metadata.Output.Partitions),
						CleanBatch: metadata.Output.CleanBatch,
						MaxAlive:   metadata.Output.MaxAlive,
					}),
				},
				Group: metapb.TenantOutputGroup,
			},
			UpdateTenantInitState: &metapb.TenantInitStateUpdateAction{
				ID:    metadata.ID,
				Index: 0,
				Group: metapb.TenantOutputGroup,
			},
		}),
		DisableSplit:  true,
		LeastReplicas: replicas,
	})

	err := eng.store.Set(storage.TenantMetadataKey(metadata.ID), protoc.MustMarshal(&metadata))
	if err != nil {
		return err
	}

	return eng.store.RaftStore().AddShards(shards...)
}

func (eng *engine) doCheckTenant(tid uint64) error {
	metadata, err := eng.loadTenantMetadata(tid)
	if err != nil {
		return err
	}

	if metadata == nil {
		return fmt.Errorf("%d not created", tid)
	}

	for _, value := range metadata.InputsState {
		if !value {
			return fmt.Errorf("%d init not completed", tid)
		}
	}

	for _, value := range metadata.OutputsState {
		if !value {
			return fmt.Errorf("%d init not completed", tid)
		}
	}

	for _, value := range metadata.RunnersState {
		if !value {
			return fmt.Errorf("%d init not completed", tid)
		}
	}

	return nil
}

func (eng *engine) doCheckTenantRunner(tid uint64) error {
	runners, err := eng.getTenantRunnerByState(tid, metapb.WRRunning)
	if err != nil {
		return err
	}

	if len(runners) == 0 {
		return fmt.Errorf("%d has no runner worker", tid)
	}

	return nil
}

func (eng *engine) StartInstance(workflow metapb.Workflow, loader metapb.BMLoader, crowdMeta []byte) (uint64, error) {
	t := time.Now().Unix()
	logger.Infof("workflow-%d start instance with %s",
		workflow.ID,
		workflow.String())

	if err := checkExcution(workflow); err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}
	logger.Infof("workflow-%d check excution succeed",
		workflow.ID)

	if err := eng.doCheckTenant(workflow.TenantID); err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}
	logger.Infof("workflow-%d check tenant info succeed",
		workflow.ID)

	if err := eng.doCheckTenantRunner(workflow.TenantID); err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}
	logger.Infof("workflow-%d check tenant runner succeed",
		workflow.ID)

	old, err := eng.loadInstance(workflow.ID)
	if err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}

	start := time.Now().Unix()
	logger.Infof("workflow-%d start instance, do load bitmap crowd",
		workflow.ID)
	bm, err := eng.loadBM(loader, crowdMeta)
	if err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}

	if bm.GetCardinality() == 0 {
		return 0, fmt.Errorf("workflow-%d start instance failed with 0 crowd",
			workflow.ID)
	}

	end := time.Now().Unix()

	logger.Infof("workflow-%d start instance, do load bitmap crowd completed in %d secs",
		workflow.ID,
		end-start)

	if old != nil {
		if old.State != metapb.Stopped {
			logger.Warningf("workflow-%d start instance skipped, last instance is not stopped",
				workflow.ID)
			return 0, nil
		}

		start = time.Now().Unix()
		logger.Infof("workflow-%d start instance, do load old bitmap crowd",
			workflow.ID)
		oldBM, err := eng.loadBM(old.Loader, old.LoaderMeta)
		if err != nil {
			logger.Errorf("workflow-%d start instance failed with %+v",
				workflow.ID,
				err)
			return 0, err
		}

		end = time.Now().Unix()
		logger.Infof("workflow-%d start instance, do load old bitmap crowd completed in %d secs",
			workflow.ID,
			end-start)

		bm.AndNot(oldBM)
		logger.Infof("workflow-%d start instance with new instance, crow changed to %d",
			workflow.ID,
			bm.GetCardinality())
	} else {
		logger.Infof("workflow-%d start instance with first instance, crow %d",
			workflow.ID,
			bm.GetCardinality())
	}

	id, err := eng.Storage().RaftStore().Prophet().GetRPC().AllocID()
	if err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}

	key := storage.WorkflowCrowdShardKey(workflow.ID, id, 0)
	start = time.Now().Unix()
	logger.Infof("workflow-%d start instance, do put bitmap crowd",
		workflow.ID)
	loader, loaderMeta, err := eng.putBM(bm, key, 0)
	if err != nil {
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
		return 0, err
	}

	end = time.Now().Unix()
	logger.Infof("workflow-%d start instance, do put bitmap crowd completed in %d secs",
		workflow.ID,
		end-start)

	instance := metapb.WorkflowInstance{
		Snapshot:   workflow,
		InstanceID: id,
		Loader:     loader,
		LoaderMeta: loaderMeta,
		TotalCrowd: bm.GetCardinality(),
	}

	req := rpcpb.AcquireStartingInstanceRequest()
	req.Instance = instance
	_, err = eng.store.ExecCommand(req)
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("workflow-%d start instance failed with %+v",
			workflow.ID,
			err)
	}

	end = time.Now().Unix()
	logger.Infof("workflow-%d start instance completed in %d secs",
		workflow.ID,
		(end - t))
	return id, nil
}

func (eng *engine) LastInstance(id uint64) (*metapb.WorkflowInstance, error) {
	return eng.loadInstance(id)
}

func (eng *engine) HistoryInstance(wid uint64, instanceID uint64) (*metapb.WorkflowInstanceSnapshot, error) {
	key := storage.WorkflowHistoryInstanceKey(wid, instanceID)
	value, err := eng.store.Get(key)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}

	v := &metapb.WorkflowInstanceSnapshot{}
	protoc.MustUnmarshal(v, value)
	return v, nil
}

func (eng *engine) StopInstance(id uint64) error {
	value, err := eng.store.Get(storage.WorkflowCurrentInstanceKey(id))
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

func (eng *engine) UpdateWorkflow(workflow metapb.Workflow) error {
	if err := checkExcution(workflow); err != nil {
		return err
	}

	instance, err := eng.loadRunningInstance(workflow.ID)
	if err != nil {
		return err
	}

	instance.Snapshot = workflow
	err = eng.doUpdateWorkflow(workflow)
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

func (eng *engine) UpdateCrowd(id uint64, loader metapb.BMLoader, crowdMeta []byte) error {
	instance, err := eng.loadRunningInstance(id)
	if err != nil {
		return err
	}

	newBM, err := eng.loadBM(loader, crowdMeta)
	if err != nil {
		return err
	}

	workers, err := eng.getInstanceWorkers(instance)
	if err != nil {
		return err
	}
	var old []*roaring.Bitmap
	for _, worker := range workers {
		bm := bbutil.AcquireBitmap()
		for _, state := range worker.States {
			v, err := eng.loadBM(state.Loader, state.LoaderMeta)
			if err != nil {
				log.Fatalf("BUG: state crowd must success, using raw")
			}

			bm.Or(v)
		}
		old = append(old, bm)
	}
	bbutil.BMAlloc(newBM, old...)

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

func (eng *engine) InstanceCountState(id uint64) (metapb.InstanceCountState, error) {
	instance, err := eng.loadRunningInstance(id)
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	m := make(map[string]*metapb.CountState)
	state := metapb.InstanceCountState{}
	state.Total = instance.TotalCrowd
	state.Snapshot = instance.Snapshot
	for _, step := range instance.Snapshot.Steps {
		m[step.Name] = &metapb.CountState{
			Step:  step.Name,
			Count: 0,
		}
	}

	workers, err := eng.getInstanceWorkers(instance)
	if err != nil {
		return metapb.InstanceCountState{}, err
	}

	for _, stepState := range workers {
		for _, ss := range stepState.States {
			if _, ok := m[ss.Step.Name]; ok {
				m[ss.Step.Name].Count += ss.TotalCrowd
			}
		}
	}

	for _, v := range m {
		state.States = append(state.States, *v)
	}

	return state, nil
}

func (eng *engine) InstanceStepState(id uint64, name string) (metapb.StepState, error) {
	instance, err := eng.loadRunningInstance(id)
	if err != nil {
		return metapb.StepState{}, err
	}

	var target metapb.Step
	valueBM := bbutil.AcquireBitmap()

	workers, err := eng.getInstanceWorkers(instance)
	if err != nil {
		return metapb.StepState{}, err
	}

	for _, stepState := range workers {
		for _, ss := range stepState.States {
			if ss.Step.Name == name {
				target = ss.Step
				v, err := eng.loadBM(ss.Loader, ss.LoaderMeta)
				if err != nil {
					return metapb.StepState{}, err
				}
				valueBM = bbutil.BMOr(valueBM, v)
			}
		}
	}

	key := storage.TempKey(uuid.NewV4().Bytes())
	loader, loaderMeta, err := eng.putBM(valueBM, key, eng.opts.tempKeyTTL)
	if err != nil {
		return metapb.StepState{}, err
	}

	return metapb.StepState{
		Step:       target,
		Loader:     loader,
		LoaderMeta: loaderMeta,
	}, nil
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

func (eng *engine) doUpdateWorkflow(value metapb.Workflow) error {
	req := &rpcpb.UpdateWorkflowRequest{}
	req.Workflow = value
	_, err := eng.store.ExecCommand(req)
	return err
}

func (eng *engine) loadTenantMetadata(tid uint64) (*metapb.Tenant, error) {
	value, err := eng.store.Get(storage.TenantMetadataKey(tid))
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}

	metatdata := &metapb.Tenant{}
	protoc.MustUnmarshal(metatdata, value)
	return metatdata, nil
}

func (eng *engine) loadInstance(id uint64) (*metapb.WorkflowInstance, error) {
	value, err := eng.store.Get(storage.WorkflowCurrentInstanceKey(id))
	if err != nil {
		metric.IncStorageFailed()
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}

	instance := &metapb.WorkflowInstance{}
	protoc.MustUnmarshal(instance, value)
	return instance, nil
}

func (eng *engine) loadRunningInstance(id uint64) (*metapb.WorkflowInstance, error) {
	instance, err := eng.loadInstance(id)
	if err != nil {
		return nil, err
	}

	if instance == nil {
		return nil, fmt.Errorf("workflow-%d has no instance",
			id)
	}

	if instance.State != metapb.Running {
		return nil, fmt.Errorf("workflow-%d state %s, not running",
			id,
			instance.State.String())
	}

	return instance, nil
}

func (eng *engine) getTenantRunnerByState(tid uint64, expects ...metapb.WorkerRunnerState) ([]metapb.WorkerRunner, error) {
	tenant := eng.mustDoLoadTenantMetadata(tid)
	var runners []metapb.WorkerRunner
	for i := uint64(0); i < tenant.Runners; i++ {
		value, err := eng.store.GetWithGroup(storage.TenantRunnerMetadataKey(tid, i), metapb.TenantRunnerGroup)
		if err != nil {
			metric.IncStorageFailed()
			return nil, err
		}

		if len(value) == 0 {
			return nil, fmt.Errorf("missing tenant %d runner %d", tid, i)
		}

		state := metapb.WorkerRunner{}
		protoc.MustUnmarshal(&state, value)
		for _, expect := range expects {
			if state.State == expect {
				runners = append(runners, state)
				break
			}
		}
	}

	return runners, nil
}

func (eng *engine) getInstanceWorkers(instance *metapb.WorkflowInstance) ([]metapb.WorkflowInstanceWorkerState, error) {
	return eng.getInstanceWorkersWithGroup(instance, metapb.TenantRunnerGroup)
}

func (eng *engine) getInstanceWorkersWithGroup(instance *metapb.WorkflowInstance, group metapb.Group) ([]metapb.WorkflowInstanceWorkerState, error) {
	tenant := eng.mustDoLoadTenantMetadata(instance.Snapshot.TenantID)

	var shards []metapb.WorkflowInstanceWorkerState
	for i := uint64(0); i < tenant.Runners; i++ {
		startKey := storage.TenantRunnerWorkerInstanceMinKey(instance.Snapshot.TenantID, i, instance.Snapshot.ID)
		end := storage.TenantRunnerWorkerInstanceMaxKey(instance.Snapshot.TenantID, i, instance.Snapshot.ID)

		for {
			_, values, err := eng.store.ScanWithGroup(startKey, end, 4, group)
			if err != nil {
				return nil, err
			}

			if len(values) == 0 {
				break
			}

			last := uint32(0)
			for _, value := range values {
				shard := metapb.WorkflowInstanceWorkerState{}
				protoc.MustUnmarshal(&shard, value)
				shards = append(shards, shard)
				last = shard.Index
			}

			last++
			startKey = storage.TenantRunnerWorkerKey(instance.Snapshot.TenantID, i, instance.Snapshot.ID, last)
		}
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Index < shards[j].Index
	})
	return shards, nil
}

func (eng *engine) mustDoLoadTenantMetadata(tid uint64) metapb.Tenant {
	for {
		metadata, err := eng.loadTenantMetadata(tid)
		if err != nil {
			time.Sleep(time.Second * 5)
			continue
		}

		if metadata == nil {
			logger.Fatalf("Missing tenant metadata")
		}

		return *metadata
	}
}

func (eng *engine) buildSnapshot(instance *metapb.WorkflowInstance, buf *goetty.ByteBuf) (*metapb.WorkflowInstanceSnapshot, []metapb.WorkflowInstanceWorkerState, error) {
	shards, err := eng.getInstanceWorkers(instance)
	if err != nil {
		return nil, nil, err
	}

	snapshot := &metapb.WorkflowInstanceSnapshot{
		Snapshot:  *instance,
		Timestamp: time.Now().Unix(),
	}
	snapshot.Snapshot.State = metapb.Stopped
	snapshot.Snapshot.StoppedAt = time.Now().Unix()

	value := make(map[string]*roaring.Bitmap)
	for _, shard := range shards {
		for _, state := range shard.States {
			v, err := eng.loadBM(state.Loader, state.LoaderMeta)
			if err != nil {
				return nil, nil, err
			}

			if bm, ok := value[state.Step.Name]; ok {
				bm.Or(v)
			} else {
				value[state.Step.Name] = v
			}
		}
	}

	for _, step := range instance.Snapshot.Steps {
		key := storage.TempKey(uuid.NewV4().Bytes())

		loader, loadMeta, err := eng.putBM(value[step.Name], key, eng.opts.snapshotTTL)
		if err != nil {
			return nil, nil, err
		}

		snapshot.States = append(snapshot.States, metapb.StepState{
			Step:       step,
			TotalCrowd: value[step.Name].GetCardinality(),
			Loader:     loader,
			LoaderMeta: loadMeta,
		})
	}

	return snapshot, shards, nil
}

func (eng *engine) handleEvent(ctx context.Context) {
	buf := goetty.NewByteBuf(32)
	defer buf.Release()

	for {
		buf.Clear()

		select {
		case <-ctx.Done():
			logger.Infof("handler instance task stopped")
			return
		case event, ok := <-eng.eventC:
			if ok {
				eng.doEvent(event, buf)
			}
		case instance, ok := <-eng.retryNewInstanceC:
			if ok {
				eng.doStartInstanceEvent(instance)
			}
		case instance, ok := <-eng.retryStoppingInstanceC:
			if ok {
				eng.doStoppingInstanceEvent(instance, buf)
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

func (eng *engine) doEvent(event storage.Event, buf *goetty.ByteBuf) {
	switch event.EventType {
	case storage.StartingInstanceEvent:
		eng.doStartInstanceEvent(event.Data.(*metapb.WorkflowInstance))
	case storage.RunningInstanceEvent:
		eng.doStartedInstanceEvent(event.Data.(*metapb.WorkflowInstance))
	case storage.StoppingInstanceEvent:
		eng.doStoppingInstanceEvent(event.Data.(*metapb.WorkflowInstance), buf)
	case storage.StoppedInstanceEvent:
		eng.doStoppedInstanceEvent(event.Data.(uint64))
	case storage.InstanceWorkerCreatedEvent:
		eng.doStartInstanceStateEvent(event.Data.(metapb.WorkflowInstanceWorkerState))
	case storage.InstanceWorkerDestoriedEvent:
		eng.doInstanceStateRemovedEvent(event.Data.(metapb.WorkflowInstanceWorkerState))
	case storage.StartRunnerEvent:
		eng.doStartRunnerEvent(event.Data.(*metapb.WorkerRunner))
	case storage.StopRunnerEvent:
		eng.doStopRunnerEvent(event.Data.(*metapb.WorkerRunner))
	}
}

func (eng *engine) doStartInstanceStateEvent(state metapb.WorkflowInstanceWorkerState) {
	eng.addCreateWorkAction(createWorkerAction{
		state:     state,
		bootstrap: true,
	})
}

func (eng *engine) addCreateWorkAction(action createWorkerAction) {
	eng.workerCreateC[atomic.AddUint64(&eng.op, 1)%eng.opts.workerCreateCount] <- action
}

func (eng *engine) handleCreateWorker(c chan createWorkerAction) {
	eng.runner.RunCancelableTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case action, ok := <-c:
				if !ok {
					return
				}

				if action.bootstrap {
					eng.doBootstrapWorker(action.state)
				} else {
					eng.doCreateWorker(action)
				}
			}
		}
	})
}

func (eng *engine) doCreateWorker(arg interface{}) {
	action := arg.(createWorkerAction)
	_, err := eng.store.ExecCommandWithGroup(&rpcpb.CreateInstanceStateShardRequest{
		State: action.state,
	}, metapb.TenantRunnerGroup)
	if err != nil {
		metric.IncStorageFailed()
		logger.Errorf("create worker %s failed with %+v, retry later",
			workerKey(action.state),
			err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval, eng.doCreateWorker, arg)
		return
	}

	logger.Infof("create worker %s on runner %s completed",
		workerKey(action.state),
		runnerKey(&metapb.WorkerRunner{ID: action.state.TenantID, Index: action.state.Runner}))
	if action.completed.done() {
		eng.doCreateInstanceStateShardComplete(action.state.WorkflowID)
	}
}

func (eng *engine) doBootstrapWorker(state metapb.WorkflowInstanceWorkerState) {
	now := time.Now().Unix()
	if state.StopAt != 0 && now >= state.StopAt {
		return
	}

	key := workerKey(state)
	runner := runnerKey(&metapb.WorkerRunner{ID: state.TenantID, Index: state.Runner})
	value, ok := eng.runners.Load(runner)
	if !ok {
		logger.Warningf("worker %s missing worker runner %s",
			runner, key)
		return
	}

	for {
		w, err := newStateWorker(key, state, eng)
		if err != nil {
			logger.Errorf("create worker %s failed with %+v, retry later",
				key,
				err)
			time.Sleep(time.Second * 5)
			continue
		}

		value.(*workerRunner).addWorker(w.key, w)
		if state.StopAt != 0 {
			after := time.Second * time.Duration(state.StopAt-now)
			util.DefaultTimeoutWheel().Schedule(after, eng.stopWorker, w)
		}
		break
	}
}

func (eng *engine) stopWorker(arg interface{}) {
	w := arg.(*stateWorker)
	w.stop()
}

func (eng *engine) doInstanceStateRemovedEvent(state metapb.WorkflowInstanceWorkerState) {
	key := workerKey(state)
	rkey := runnerKey(&metapb.WorkerRunner{ID: state.TenantID, Index: state.Runner})

	if runner, ok := eng.runners.Load(rkey); ok {
		runner.(*workerRunner).removeWorker(key)
	}
}

func (eng *engine) doStartInstanceEvent(instance *metapb.WorkflowInstance) {
	logger.Infof("starting workflow-%d",
		instance.Snapshot.ID)

	bm, err := eng.loadBM(instance.Loader, instance.LoaderMeta)
	if err != nil {
		logger.Errorf("start workflow-%d failed with %+v, retry later",
			instance.Snapshot.ID, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval,
			eng.addToRetryNewInstance, instance)
		return
	}
	logger.Infof("starting workflow-%d load bitmap completed with %d, [%d, %d]",
		instance.Snapshot.ID,
		bm.GetCardinality(),
		bm.Minimum(),
		bm.Maximum())

	// load all runners, and alloc worker to these runners using RR balance
	runners, err := eng.getTenantRunnerByState(instance.Snapshot.TenantID, metapb.WRRunning)
	if err != nil {
		logger.Errorf("start workflow-%d failed with %+v, retry later",
			instance.Snapshot.ID, err)
		util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval,
			eng.addToRetryNewInstance, instance)
		return
	}
	if len(runners) == 0 {
		logger.Fatalf("BUG: starting workflow-%d with no running worker",
			instance.Snapshot.ID)
	}
	logger.Infof("starting workflow-%d load %d worker runners completed",
		instance.Snapshot.ID,
		len(runners))

	bms := bbutil.BMSplit(bm, maxPerWorker)
	logger.Infof("starting workflow-%d %d shards on %d runners",
		instance.Snapshot.ID,
		len(bms),
		len(runners))

	completed := &actionCompleted{0, uint64(len(bms))}
	logger.Infof("starting workflow-%d completed bimap split",
		instance.Snapshot.ID)
	for index, bm := range bms {
		state := metapb.WorkflowInstanceWorkerState{}
		state.Runner = uint64(index % len(runners))
		state.TenantID = instance.Snapshot.TenantID
		state.WorkflowID = instance.Snapshot.ID
		state.InstanceID = instance.InstanceID
		state.Index = uint32(index)
		state.StopAt = instance.Snapshot.StopAt

		for _, step := range instance.Snapshot.Steps {
			state.States = append(state.States, metapb.StepState{
				Step:       step,
				TotalCrowd: 0,
				Loader:     metapb.RawLoader,
				LoaderMeta: emptyBMData.Bytes(),
			})
		}

		state.States[0].TotalCrowd = bm.GetCardinality()
		state.States[0].LoaderMeta = bbutil.MustMarshalBM(bm)

		logger.Infof("starting workflow-%d create %d shard",
			instance.Snapshot.ID,
			index)
		eng.addCreateWorkAction(createWorkerAction{false, state, completed})
		logger.Infof("starting workflow-%d after create %d shard",
			instance.Snapshot.ID,
			index)
	}
}

func (eng *engine) doStartedInstanceEvent(instance *metapb.WorkflowInstance) {
	if instance.Snapshot.StopAt == 0 {
		logger.Infof("workflow-%d started",
			instance.Snapshot.ID)
		return
	}

	now := time.Now().Unix()
	after := instance.Snapshot.StopAt - now
	if after <= 0 {
		eng.addToInstanceStop(instance.Snapshot.ID)
		return
	}

	util.DefaultTimeoutWheel().Schedule(time.Second*time.Duration(after), eng.addToInstanceStop, instance.Snapshot.ID)
	logger.Infof("workflow-%d started, duration %d seconds",
		instance.Snapshot.ID,
		after)
}

func (eng *engine) doStoppingInstanceEvent(instance *metapb.WorkflowInstance, buf *goetty.ByteBuf) {
	logger.Infof("stopping workflow-%d",
		instance.Snapshot.ID)

	shards, err := eng.doSaveSnapshot(instance, buf)
	if err != nil {
		eng.doRetryStoppingInstance(instance, err)
		return
	}

	logger.Infof("workflow-%d history added with instance id %d",
		instance.Snapshot.ID,
		instance.InstanceID)

	for _, shard := range shards {
		req := rpcpb.AcquireRemoveInstanceStateShardRequest()
		req.TenantID = instance.Snapshot.TenantID
		req.WorkflowID = instance.Snapshot.ID
		req.InstanceID = instance.InstanceID
		req.Index = shard.Index
		req.Runner = shard.Runner
		_, err := eng.store.ExecCommandWithGroup(req, metapb.TenantRunnerGroup)
		if err != nil {
			eng.doRetryStoppingInstance(instance, err)
			return
		}
	}

	eng.doInstanceStoppingComplete(instance)
}

func (eng *engine) doInstanceStoppingComplete(instance *metapb.WorkflowInstance) {
	_, err := eng.store.ExecCommand(&rpcpb.StoppedInstanceRequest{
		WorkflowID: instance.Snapshot.ID,
	})
	if err != nil {
		eng.doRetryStoppingInstance(instance, err)
		return
	}

	go eng.doRemoveShardBitmaps(instance)
}

func (eng *engine) doRetryStoppingInstance(instance *metapb.WorkflowInstance, err error) {
	metric.IncStorageFailed()
	logger.Errorf("stopping workflow-%d failed with %+v, retry later",
		instance.Snapshot.ID,
		err)
	util.DefaultTimeoutWheel().Schedule(eng.opts.retryInterval,
		eng.addToRetryStoppingInstance,
		instance)
}

func (eng *engine) doSaveSnapshot(instance *metapb.WorkflowInstance, buf *goetty.ByteBuf) ([]metapb.WorkflowInstanceWorkerState, error) {
	key := storage.WorkflowHistoryInstanceKey(instance.Snapshot.ID, instance.InstanceID)
	value, err := eng.store.Get(key)
	if err != nil {
		return nil, err
	}

	if len(value) > 0 {
		return nil, nil
	}

	snapshot, shards, err := eng.buildSnapshot(instance, buf)
	if err != nil {
		return nil, err
	}

	err = eng.store.SetWithTTL(key, protoc.MustMarshal(snapshot), int64(eng.opts.snapshotTTL))
	if err != nil {
		return nil, err
	}

	return shards, nil
}

func (eng *engine) doStoppedInstanceEvent(wid uint64) {
	eng.runners.Range(func(key, value interface{}) bool {
		value.(*workerRunner).removeWorkers(wid)
		return true
	})

	logger.Infof("workflow-%d stopped",
		wid)
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

func (eng *engine) doRemoveShardBitmaps(instance *metapb.WorkflowInstance) {
	switch instance.Loader {
	case metapb.KVLoader:
		return
	case metapb.RawLoader:
		return
	case metapb.KVShardLoader:
		meta := &metapb.ShardBitmapLoadMeta{}
		protoc.MustUnmarshal(meta, instance.LoaderMeta)
		logger.Infof("start remove workflow-%d/%d shard bitmaps",
			instance.Snapshot.ID,
			instance.InstanceID)

		for i := uint32(0); i < meta.Shards; i++ {
			err := eng.store.Delete(storage.ShardBitmapKey(meta.Key, i))
			if err != nil {
				logger.Errorf("remove workflow shard bitmap workflow-%d/%d/%d failed",
					instance.Snapshot.ID,
					instance.InstanceID,
					i)
				continue
			}

			logger.Infof("remove workflow shard bitmap workflow-%d/%d/%d completed",
				instance.Snapshot.ID,
				instance.InstanceID,
				i)
		}
	}
}

func (eng *engine) addToRetryNewInstance(arg interface{}) {
	eng.retryNewInstanceC <- arg.(*metapb.WorkflowInstance)
}

func (eng *engine) addToRetryStoppingInstance(arg interface{}) {
	eng.retryStoppingInstanceC <- arg.(*metapb.WorkflowInstance)
}

func (eng *engine) addToRetryCompleteInstance(arg interface{}) {
	eng.retryCompleteInstanceC <- arg.(uint64)
}

func (eng *engine) addToInstanceStop(id interface{}) {
	eng.stopInstanceC <- id.(uint64)
}

func workerKey(state metapb.WorkflowInstanceWorkerState) string {
	return fmt.Sprintf("%d:%d-%d:%d",
		state.TenantID,
		state.WorkflowID,
		state.InstanceID,
		state.Index)
}
