package storage

import (
	"context"
	"math"
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	defaultRPCTimeout        = time.Second * 10
	reportGroup       uint64 = 1314
)

// Storage storage
type Storage interface {
	// Start the storage
	Start() error
	// Close close the storage
	Close()
	// WatchInstance watch instance event
	WatchEvent() chan Event
	// Set set key value
	Set([]byte, []byte) error
	// Set set key value with a TTL in seconds
	SetWithTTL([]byte, []byte, int64) error
	// Get returns the value of key
	Get([]byte) ([]byte, error)
	// Delete remove the key from the store
	Delete([]byte) error
	// Scan scan [start,end) data
	Scan([]byte, []byte, uint64) ([][]byte, error)
	// PutToQueue put data to queue
	PutToQueue(id uint64, partition uint64, group metapb.Group, data ...[]byte) error
	// ExecCommand exec command
	ExecCommand(cmd interface{}) ([]byte, error)
	// AsyncExecCommand async exec command
	AsyncExecCommand(interface{}, func(interface{}, []byte, error), interface{})
	// ExecCommandWithGroup exec command with group
	ExecCommandWithGroup(interface{}, metapb.Group) ([]byte, error)
	// AsyncExecCommandWithGroup async exec command with group
	AsyncExecCommandWithGroup(interface{}, metapb.Group, func(interface{}, []byte, error), interface{})
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
}

type beeStorage struct {
	reportElector prophet.Elector
	app           *server.Application
	store         raftstore.Store
	eventC        chan Event
	shardC        chan shardCycle
	runner        *task.Runner

	scanTaskID uint64
}

// NewStorage returns a beehive request handler
func NewStorage(dataPath string,
	metadataStorages []bhstorage.MetadataStorage,
	dataStorages []bhstorage.DataStorage) (Storage, error) {

	h := &beeStorage{
		eventC: make(chan Event, 1024),
		shardC: make(chan shardCycle, 1024),
		runner: task.NewRunner(),
	}

	store, err := beehive.CreateRaftStoreFromFile(dataPath,
		metadataStorages,
		dataStorages,
		raftstore.WithShardStateAware(h),
		raftstore.WithWriteBatchFunc(h.WriteBatch))
	if err != nil {
		return nil, err
	}

	h.store = store
	return h, nil
}

func (h *beeStorage) Start() error {
	h.init()

	app := server.NewApplication(server.Cfg{
		Store:          h.store,
		Handler:        h,
		ExternalServer: true,
	})
	err := app.Start()
	if err != nil {
		return err
	}

	elector, err := prophet.NewElector(h.store.Prophet().GetEtcdClient())
	if err != nil {
		return err
	}

	go elector.ElectionLoop(context.Background(),
		reportGroup,
		h.store.Meta().ShardAddr,
		h.becomeReportLeader,
		h.becomeReportFollower)

	h.app = app
	return nil
}

func (h *beeStorage) Close() {
	h.runner.Stop()
	h.app.Stop()
	close(h.shardC)
	close(h.eventC)
	h.store.Stop()
}

func (h *beeStorage) Set(key, value []byte) error {
	return h.SetWithTTL(key, value, 0)
}

func (h *beeStorage) SetWithTTL(key, value []byte, ttl int64) error {
	req := rpcpb.AcquireSetRequest()
	req.Key = key
	req.Value = value
	req.TTL = ttl
	_, err := h.ExecCommand(req)
	return err
}

func (h *beeStorage) Get(key []byte) ([]byte, error) {
	req := rpcpb.AcquireGetRequest()
	req.Key = key
	value, err := h.ExecCommand(req)
	if err != nil {
		return nil, err
	}

	resp := rpcpb.BytesResponse{}
	protoc.MustUnmarshal(&resp, value)
	return resp.Value, nil
}

func (h *beeStorage) Delete(key []byte) error {
	req := rpcpb.AcquireDeleteRequest()
	req.Key = key

	_, err := h.ExecCommand(req)
	return err
}

func (h *beeStorage) Scan(start []byte, end []byte, limit uint64) ([][]byte, error) {
	req := rpcpb.AcquireScanRequest()
	req.Start = start
	req.End = end
	req.Limit = limit

	data, err := h.ExecCommand(req)
	if err != nil {
		return nil, err
	}

	resp := rpcpb.AcquireBytesSliceResponse()
	protoc.MustUnmarshal(resp, data)
	items := resp.Values

	rpcpb.ReleaseBytesSliceResponse(resp)
	return items, nil
}

func (h *beeStorage) PutToQueue(id uint64, partition uint64, group metapb.Group, data ...[]byte) error {
	req := rpcpb.AcquireQueueAddRequest()
	req.Key = PartitionKey(id, partition)
	req.Items = data

	_, err := h.ExecCommandWithGroup(req, group)
	return err
}

func (h *beeStorage) ExecCommand(cmd interface{}) ([]byte, error) {
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *beeStorage) AsyncExecCommand(cmd interface{}, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithTimeout(cmd, cb, defaultRPCTimeout, arg)
}

func (h *beeStorage) AsyncExecCommandWithGroup(cmd interface{}, group metapb.Group, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithGroupAndTimeout(cmd, uint64(group), cb, defaultRPCTimeout, arg)
}

func (h *beeStorage) ExecCommandWithGroup(cmd interface{}, group metapb.Group) ([]byte, error) {
	return h.app.ExecWithGroup(cmd, uint64(group), defaultRPCTimeout)
}

func (h *beeStorage) WatchEvent() chan Event {
	return h.eventC
}

func (h *beeStorage) RaftStore() raftstore.Store {
	return h.store
}

func (h *beeStorage) becomeReportLeader() {
	h.startWorkflowScan()
}

func (h *beeStorage) becomeReportFollower() {
	h.stopWorkflowScan()
}

func (h *beeStorage) startWorkflowScan() {
	if h.scanTaskID > 0 {
		h.runner.StopCancelableTask(h.scanTaskID)
	}

	h.scanTaskID, _ = h.runner.RunCancelableTask(h.doWorkflowScan)
}

func (h *beeStorage) stopWorkflowScan() {
	if h.scanTaskID > 0 {
		h.runner.StopCancelableTask(h.scanTaskID)
	}
}

func (h *beeStorage) doWorkflowScan(ctx context.Context) {
	log.Infof("workflow scan worker started")
	timer := time.NewTicker(time.Second * 10)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("workflow scan worker stopped")
			return
		case <-timer.C:
			log.Infof("start scan workflow")
			h.scanWorkflow()
			h.scanWorkflowShards()
		}
	}
}

func (h *beeStorage) scanWorkflow() {
	instance := &metapb.WorkflowInstance{}
	startingCount := 0
	startedCount := 0
	stoppingCount := 0
	stoppedCount := 0

	start := WorkflowCurrentInstanceKey(0)
	end := WorkflowCurrentInstanceKey(math.MaxUint64)

	for {
		values, err := h.Scan(start, end, 16)
		if err != nil {
			metric.IncStorageFailed()
			log.Errorf("scan workflow failed with %+v")
			return
		}

		if len(values) == 0 {
			break
		}

		for idx, value := range values {
			instance.Reset()
			protoc.MustUnmarshal(instance, value)

			switch instance.State {
			case metapb.Starting:
				startingCount++
			case metapb.Running:
				startedCount++
			case metapb.Stopping:
				stoppingCount++
			case metapb.Stopped:
				stoppedCount++
			}

			if idx == len(values)-1 {
				start = WorkflowCurrentInstanceKey(instance.Snapshot.ID + 1)
			}
		}
	}

	log.Infof("start scan workflow with %d statrting, %d started, %d stopping, %d stopped workflows",
		startingCount,
		startedCount,
		stoppingCount,
		stoppedCount)
	metric.SetWorkflowCount(startingCount, startedCount, stoppingCount, stoppedCount)
}

func (h *beeStorage) scanWorkflowShards() {
	shard := &metapb.WorkflowInstanceWorkerState{}
	count := 0
	start := InstanceShardKey(0, 0)
	end := InstanceShardKey(math.MaxUint64, math.MaxUint32)
	for {
		values, err := h.Scan(start, end, 16)
		if err != nil {
			metric.IncStorageFailed()
			log.Errorf("scan workflow shard failed with %+v",
				err)
			return
		}

		if len(values) == 0 {
			break
		}

		for idx, value := range values {
			count++

			if idx == len(values)-1 {
				shard.Reset()
				protoc.MustUnmarshal(shard, value)
				start = InstanceShardKey(shard.WorkflowID, shard.Index+1)
			}
		}
	}

	log.Infof("start scan workflow with %d running workers",
		count)
	metric.SetWorkflowShardsCount(count)
}

func (h *beeStorage) getStore(shard uint64) bhstorage.DataStorage {
	return h.store.DataStorage(shard)
}
