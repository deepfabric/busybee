package storage

import (
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	beehiveStorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	defaultRPCTimeout = time.Second * 10

	// EventQueueGroup queue group
	EventQueueGroup uint64 = 1
	// NotifyQueueGroup queue group
	NotifyQueueGroup uint64 = 2
)

// Storage storage
type Storage interface {
	// Start the storage
	Start() error
	// Close close the storage
	Close()
	// WatchInstance watch instance
	WatchEvent() chan Event
	// ExecCommand exec command
	ExecCommand(cmd interface{}) ([]byte, error)
	// CreateEventQueue create a queue to serve a workflow instance events.
	CreateEventQueue(id uint64) error
	// CreateNotifyQueue create a queue to serve a workflow instance notifies.
	CreateNotifyQueue(id uint64) error
	// QueueAdd add items to work flow instance queue
	QueueAdd(id uint64, group uint64, items ...[]byte) (uint64, error)
	// QueueFetch add items to work flow instance queue
	QueueFetch(id uint64, group uint64, afterOffset uint64, count uint64) (uint64, [][]byte, error)
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
}

type beeStorage struct {
	addr   string
	app    *server.Application
	store  raftstore.Store
	eventC chan Event
	shardC chan shardCycle
	runner *task.Runner
}

// NewStorage returns a beehive request handler
func NewStorage(addr string, dataPath string,
	metadataStorages []beehiveStorage.MetadataStorage,
	dataStorages []beehiveStorage.DataStorage) (Storage, error) {

	h := &beeStorage{
		addr:   addr,
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
		Addr:    h.addr,
		Store:   h.store,
		Handler: h,
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

func (h *beeStorage) ExecCommand(cmd interface{}) ([]byte, error) {
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *beeStorage) WatchEvent() chan Event {
	return h.eventC
}

func (h *beeStorage) CreateEventQueue(id uint64) error {
	return h.store.AddShard(metapb.Shard{
		Start:        goetty.Uint64ToBytes(id),
		End:          goetty.Uint64ToBytes(id + 1),
		DisableSplit: true,
		Group:        EventQueueGroup,
	})
}

func (h *beeStorage) CreateNotifyQueue(id uint64) error {
	return h.store.AddShard(metapb.Shard{
		Start:        goetty.Uint64ToBytes(id),
		End:          goetty.Uint64ToBytes(id + 1),
		DisableSplit: true,
		Group:        NotifyQueueGroup,
	})
}

func (h *beeStorage) QueueAdd(id uint64, group uint64, items ...[]byte) (uint64, error) {
	req := rpcpb.AcquireQueueAddRequest()
	req.Items = items
	req.Key = goetty.Uint64ToBytes(id)

	data, err := h.app.ExecWithGroup(req, group, defaultRPCTimeout)
	if err != nil {
		return 0, err
	}

	resp := rpcpb.AcquireQueueAddResponse()
	protoc.MustUnmarshal(resp, data)

	offset := resp.LastOffset
	rpcpb.ReleaseQueueAddResponse(resp)
	return offset, nil
}

func (h *beeStorage) QueueFetch(id uint64, group uint64, afterOffset uint64, count uint64) (uint64, [][]byte, error) {
	req := rpcpb.AcquireQueueFetchRequest()
	req.Count = count
	req.Key = goetty.Uint64ToBytes(id)
	req.AfterOffset = afterOffset
	req.Key = goetty.Uint64ToBytes(id)

	data, err := h.app.ExecWithGroup(req, group, defaultRPCTimeout)
	if err != nil {
		return 0, nil, err
	}

	resp := rpcpb.AcquireQueueFetchResponse()
	protoc.MustUnmarshal(resp, data)

	offset := resp.LastOffset
	items := resp.Items
	rpcpb.ReleaseQueueFetchResponse(resp)
	return offset, items, nil
}

func (h *beeStorage) RaftStore() raftstore.Store {
	return h.store
}
