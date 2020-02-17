package storage

import (
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	defaultRPCTimeout = time.Second * 10
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
	Scan([]byte, []byte, uint64, int32) ([][]byte, error)
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
	app    *server.Application
	store  raftstore.Store
	eventC chan Event
	shardC chan shardCycle
	runner *task.Runner
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

func (h *beeStorage) Scan(start []byte, end []byte, limit uint64, skipPrefix int32) ([][]byte, error) {
	req := rpcpb.AcquireScanRequest()
	req.Start = start
	req.End = end
	req.Limit = limit
	req.Skip = skipPrefix

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

func (h *beeStorage) getStore(shard uint64) bhstorage.DataStorage {
	return h.store.DataStorage(shard)
}
