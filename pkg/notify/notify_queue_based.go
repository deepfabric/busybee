package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	ttlValue = []byte{'1'}
)

type queueNotifier struct {
	store storage.Storage
	group metapb.Group
}

// NewQueueBasedNotifier create a notify based on raft queue
func NewQueueBasedNotifier(store storage.Storage) Notifier {
	return NewQueueBasedNotifierWithGroup(store, metapb.TenantOutputGroup)
}

// NewQueueBasedNotifierWithGroup create a notify based on raft queue
func NewQueueBasedNotifierWithGroup(store storage.Storage, group metapb.Group) Notifier {
	return &queueNotifier{
		store: store,
		group: group,
	}
}

func (n *queueNotifier) Notify(id uint64, buf *goetty.ByteBuf, notifies []metapb.Notify, cond *rpcpb.Condition, kvs ...[]byte) error {
	var items [][]byte
	for idx := range notifies {
		items = append(items, protoc.MustMarshal(&notifies[idx]))
	}

	return n.store.PutToQueueWithAllocAndKVAndCondition(id, n.group, items, cond, kvs...)
}
