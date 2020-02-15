package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
)

type queueNotifier struct {
	store storage.Storage
}

// NewQueueBasedNotifier create a notify based on raft queue
func NewQueueBasedNotifier(store storage.Storage) Notifier {
	return &queueNotifier{
		store: store,
	}
}

func (n *queueNotifier) Notify(id uint64, notifies ...metapb.Notify) error {
	var items [][]byte
	for _, nt := range notifies {
		items = append(items, protoc.MustMarshal(&nt))
	}

	return n.store.PutToQueue(id, 0, metapb.TenantOutputGroup, items...)
}
