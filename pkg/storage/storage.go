package storage

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	defaultRPCTimeout        = time.Second * 30
	reportGroup       uint64 = 13141314131413141314
)

// Storage storage
type Storage interface {
	// Start the storage
	Start() error
	// Close close the storage
	Close()
	// WatchInstance watch instance event
	WatchEvent() chan Event
	// Lock distruibuted lock
	Lock([]byte, []byte, int, time.Duration, bool) (bool, error)
	// Unlock unlock
	Unlock([]byte, []byte) error
	// Set set key value
	Set([]byte, []byte) error
	// Set set key value with a TTL in seconds
	SetWithTTL([]byte, []byte, int64) error
	// Get returns the value of key
	Get([]byte) ([]byte, error)
	// GetWithGroup returns the value of key
	GetWithGroup([]byte, metapb.Group) ([]byte, error)
	// Delete remove the key from the store
	Delete([]byte) error
	// Scan scan [start,end) data
	Scan([]byte, []byte, uint64) ([][]byte, [][]byte, error)
	// Scan scan [start,end) data
	ScanWithGroup([]byte, []byte, uint64, metapb.Group) ([][]byte, [][]byte, error)
	// PutToQueue put data to queue
	PutToQueue(id uint64, partition uint32, group metapb.Group, data ...[]byte) error
	// PutToQueueAndKV put data to queue and put a kv
	PutToQueueWithKV(id uint64, partition uint32, group metapb.Group, items [][]byte, kvs ...[]byte) error
	// PutToQueueWithKVAndCondition put data to queue and put a kv and a condition
	PutToQueueWithKVAndCondition(id uint64, partition uint32, group metapb.Group, items [][]byte, cond *rpcpb.Condition, kvs ...[]byte) error
	// PutToQueueWithAlloc put data to queue
	PutToQueueWithAlloc(id uint64, group metapb.Group, data ...[]byte) error
	// PutToQueueWithAllocAndKV put data to queue and put a kv
	PutToQueueWithAllocAndKV(id uint64, group metapb.Group, items [][]byte, kvs ...[]byte) error
	// PutToQueueWithAllocAndKVAndCondition put data to queue and put a kv and a condition
	PutToQueueWithAllocAndKVAndCondition(id uint64, group metapb.Group, items [][]byte, cond *rpcpb.Condition, kvs ...[]byte) error
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
	reportElector    prophet.Elector
	app              *server.Application
	store            raftstore.Store
	eventC           chan Event
	shardC           chan shardCycle
	runner           *task.Runner
	elector          prophet.Elector
	reportCancelFunc context.CancelFunc
	scanTaskID       uint64

	locks sync.Map // key -> lock
}

// NewStorage returns a beehive request handler
func NewStorage(dataPath string,
	metadataStorage bhstorage.MetadataStorage,
	dataStorages []bhstorage.DataStorage) (Storage, error) {
	return NewStorageWithOptions(dataPath, metadataStorage, dataStorages)
}

// NewStorageWithOptions returns a beehive request handler
func NewStorageWithOptions(dataPath string,
	metadataStorage bhstorage.MetadataStorage,
	dataStorages []bhstorage.DataStorage, opts ...raftstore.Option) (Storage, error) {

	h := &beeStorage{
		eventC: make(chan Event, 1024),
		shardC: make(chan shardCycle, 1024),
		runner: task.NewRunner(),
	}

	opts = append(opts, raftstore.WithDisableRaftLogCompactProtect(uint64(metapb.TenantInputGroup),
		uint64(metapb.TenantOutputGroup),
		uint64(metapb.TenantRunnerGroup)))
	opts = append(opts, raftstore.WithShardStateAware(h))
	opts = append(opts, raftstore.WithWriteBatchFunc(h.WriteBatch))
	opts = append(opts, raftstore.WithReadBatchFunc(h.ReadBatch))
	opts = append(opts, raftstore.WithShardAddHandleFun(h.addShardCallback))
	opts = append(opts, raftstore.WithProphetOptions(prophet.WithResourceSortCompareFunc(func(res1 prophet.Resource, res2 prophet.Resource) int {
		shard1 := res1.(raftstore.ShardResource).Meta()
		shard2 := res2.(raftstore.ShardResource).Meta()

		if shard1.Group == shard2.Group {
			return 0
		}

		if shard1.Group < shard2.Group {
			return -1
		}

		return 1
	})))

	store, err := beehive.CreateRaftStoreFromFile(dataPath,
		metadataStorage,
		dataStorages,
		opts...)
	if err != nil {
		return nil, err
	}

	h.store = store
	h.app = server.NewApplication(server.Cfg{
		Store:          h.store,
		Handler:        h,
		ExternalServer: true,
	})
	h.init()

	err = h.app.Start()
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *beeStorage) Start() error {
	if !h.store.Prophet().StorageNode() {
		return nil
	}

	elector, err := prophet.NewElector(h.store.Prophet().GetEtcdClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.elector = elector
	h.reportCancelFunc = cancel
	go h.elector.ElectionLoop(ctx,
		reportGroup,
		h.store.Meta().ShardAddr,
		h.becomeReportLeader,
		h.becomeReportFollower)
	return nil
}

func (h *beeStorage) Close() {
	if h.elector != nil {
		h.elector.Stop(reportGroup)
		h.reportCancelFunc()
	}

	h.locks.Range(func(key, value interface{}) bool {
		h.locks.Delete(key)
		return true
	})

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
	return h.GetWithGroup(key, metapb.DefaultGroup)
}

// GetWithGroup returns the value of key
func (h *beeStorage) GetWithGroup(key []byte, group metapb.Group) ([]byte, error) {
	req := rpcpb.AcquireGetRequest()
	req.Key = key
	value, err := h.ExecCommandWithGroup(req, group)
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

func (h *beeStorage) Scan(start []byte, end []byte, limit uint64) ([][]byte, [][]byte, error) {
	return h.ScanWithGroup(start, end, limit, metapb.DefaultGroup)
}

func (h *beeStorage) ScanWithGroup(start []byte, end []byte, limit uint64, group metapb.Group) ([][]byte, [][]byte, error) {
	req := rpcpb.AcquireScanRequest()
	req.Start = start
	req.End = end
	req.Limit = limit

	data, err := h.ExecCommandWithGroup(req, group)
	if err != nil {
		return nil, nil, err
	}

	resp := rpcpb.BytesSliceResponse{}
	protoc.MustUnmarshal(&resp, data)

	items := resp.Values
	keys := resp.Keys
	return keys, items, nil
}

func (h *beeStorage) PutToQueue(id uint64, partition uint32, group metapb.Group, items ...[]byte) error {
	return h.PutToQueueWithKV(id, partition, group, items)
}

func (h *beeStorage) PutToQueueWithKV(id uint64, partition uint32, group metapb.Group, items [][]byte, kvs ...[]byte) error {
	return h.PutToQueueWithKVAndCondition(id, partition, group, items, nil, kvs...)
}

func (h *beeStorage) PutToQueueWithKVAndCondition(id uint64, partition uint32, group metapb.Group, items [][]byte, cond *rpcpb.Condition, kvs ...[]byte) error {
	req := rpcpb.AcquireQueueAddRequest()
	req.Key = PartitionKey(id, partition)
	req.Items = items
	req.KVS = kvs
	req.Condition = cond

	_, err := h.ExecCommandWithGroup(req, group)
	return err
}

func (h *beeStorage) PutToQueueWithAlloc(id uint64, group metapb.Group, items ...[]byte) error {
	return h.PutToQueueWithAllocAndKV(id, group, items)
}

func (h *beeStorage) PutToQueueWithAllocAndKV(id uint64, group metapb.Group, items [][]byte, kvs ...[]byte) error {
	return h.PutToQueueWithAllocAndKVAndCondition(id, group, items, nil, kvs...)
}

func (h *beeStorage) PutToQueueWithAllocAndKVAndCondition(id uint64, group metapb.Group, items [][]byte, cond *rpcpb.Condition, kvs ...[]byte) error {
	req := rpcpb.AcquireQueueAddRequest()
	req.Key = PartitionKey(id, 0)
	req.Items = items
	req.KVS = kvs
	req.Condition = cond
	req.AllocPartition = true

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
	h.startScanAndCleanJob()
}

func (h *beeStorage) becomeReportFollower() {
	h.stopScanAndCleanJob()
}

func (h *beeStorage) startScanAndCleanJob() {
	if h.scanTaskID > 0 {
		h.runner.StopCancelableTask(h.scanTaskID)
	}

	h.scanTaskID, _ = h.runner.RunCancelableTask(h.doScanAndCleanJob)
}

func (h *beeStorage) stopScanAndCleanJob() {
	if h.scanTaskID > 0 {
		h.runner.StopCancelableTask(h.scanTaskID)
	}
}

func (h *beeStorage) doScanAndCleanJob(ctx context.Context) {
	log.Infof("scan and clean job started")
	timer := time.NewTicker(time.Second * 30)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("scan and clean job stopped")
			return
		case <-timer.C:
			log.Infof("do scan workflow")
			h.scanWorkflow()

			log.Infof("do scan tenant")
			h.scanTenants()
		}
	}
}

func (h *beeStorage) scanTenants() {
	start := uint64(0)
	end := TenantMetadataKey(math.MaxUint64)
	metadata := &metapb.Tenant{}

	for {
		_, values, err := h.Scan(TenantMetadataKey(start), end, 16)
		if err != nil {
			metric.IncStorageFailed()
			log.Errorf("scan queues failed with %+v",
				err)
			return
		}

		if len(values) == 0 {
			break
		}

		for idx, value := range values {
			metadata.Reset()
			protoc.MustUnmarshal(metadata, value)

			h.cleanQueues(metadata.ID, metapb.TenantInputGroup, metadata.Input.Partitions, metadata.Input.MaxAlive)
			h.cleanQueues(metadata.ID, metapb.TenantOutputGroup, metadata.Output.Partitions, metadata.Output.MaxAlive)

			if idx == len(values)-1 {
				start = metadata.ID + 1
			}
		}
	}
}

func (h *beeStorage) cleanQueues(tid uint64, group metapb.Group, n uint32, maxAlive int64) {
	tenant := fmt.Sprintf("t-%d", tid)

	for i := uint32(0); i < n; i++ {
		key := PartitionKey(tid, i)
		partition := fmt.Sprintf("p-%d", i)

		v, err := h.GetWithGroup(maxOffsetKey(key), group)
		if err != nil {
			log.Errorf("scan queues failed with %+v", err)
			return
		}
		if len(v) == 0 {
			return
		}
		max := goetty.Byte2UInt64(v)

		v, err = h.GetWithGroup(removedOffsetKey(key), group)
		if err != nil {
			log.Errorf("scan queues failed with %+v", err)
			return
		}
		removed := uint64(0)
		if len(v) > 0 {
			removed = goetty.Byte2UInt64(v)
		}

		log.Infof("%d/%s/%d offset (%d, %d]",
			tid,
			group.String(),
			i,
			removed,
			max)
		metric.SetEventQueueSize(max, removed,
			tenant,
			partition,
			group)

		low := uint64(math.MaxUint64)
		found := false
		start := consumerStartKey(key)
		for {
			keys, values, err := h.ScanWithGroup(start, consumerEndKey(key), 8, group)
			if err != nil {
				metric.IncStorageFailed()
				log.Errorf("scan tenant queues failed with %+v",
					err)
				return
			}

			if len(values) == 0 {
				break
			}

			for idx, value := range values {
				v := goetty.Byte2UInt64(value)
				log.Infof("%d/%s/%d %s committed offset %d",
					tid,
					group.String(),
					i,
					string(decodeConsumerFromCommittedOffsetKey(keys[idx])),
					v)

				metric.SetQueueConsumerCompleted(v,
					tenant,
					partition,
					string(decodeConsumerFromCommittedOffsetKey(keys[idx])),
					group)
				if v < low {
					low = v
					found = true
				}

				start = keys[idx]
			}

			start = append(start, 0)
		}

		if found && low > removed {
			req := rpcpb.AcquireQueueDeleteRequest()
			req.Key = key
			req.From = removed
			req.To = low
			_, err := h.ExecCommandWithGroup(req, group)

			if err != nil {
				log.Errorf("scan tenant queues failed with %+v", err)
				return
			}

			log.Infof("%d/%s/%d removed offset changed to (%d, %d]",
				tid,
				group.String(),
				i,
				low,
				max)
			metric.SetEventQueueSize(max, low,
				tenant,
				partition,
				group)
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
		_, values, err := h.Scan(start, end, 16)
		if err != nil {
			metric.IncStorageFailed()
			log.Errorf("scan workflow failed with %+v",
				err)
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

func (h *beeStorage) getStoreByGroup(group uint64) bhstorage.DataStorage {
	return h.store.DataStorageByGroup(group)
}
