package notify

import (
	"log"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	ttlValue = []byte{'1'}
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

func (n *queueNotifier) Notify(id uint64, buf *goetty.ByteBuf, notifies []metapb.Notify, kvs ...[]byte) error {
	var items [][]byte
	for idx := range notifies {
		if notifies[idx].TTL > 0 {
			err := n.addTTL(buf, &notifies[idx])
			if err != nil {
				return err
			}
		}

		items = append(items, protoc.MustMarshal(&notifies[idx]))
	}

	return n.store.PutToQueueWithKV(id, 0, metapb.TenantOutputGroup, items, kvs...)
}

func (n *queueNotifier) addTTL(buf *goetty.ByteBuf, nt *metapb.Notify) error {
	if nt.UserID > 0 {
		logger.Debugf("add %d TTL secs on workflow %d/%d step %s for user %d",
			nt.TTL,
			nt.WorkflowID,
			nt.InstanceID,
			nt.ToStep,
			nt.UserID)
		key := storage.WorkflowStepTTLKey(nt.WorkflowID, nt.UserID, nt.ToStep, buf)
		return n.store.SetWithTTL(key, ttlValue, int64(nt.TTL))
	}

	if len(nt.Crowd) == 0 {
		log.Fatalf("BUG: notify must has a user or a crowd")
	}

	bm := util.MustParseBM(nt.Crowd)
	itr := bm.Iterator()
	for {
		if !itr.HasNext() {
			break
		}

		buf.Clear()
		user := itr.Next()
		key := storage.WorkflowStepTTLKey(nt.WorkflowID, user, nt.ToStep, buf)
		err := n.store.SetWithTTL(key, ttlValue, int64(nt.TTL))
		if err != nil {
			return err
		}

		logger.Debugf("add %d TTL secs on workflow %d/%d step %s for user %d",
			nt.TTL,
			nt.WorkflowID,
			nt.InstanceID,
			nt.ToStep,
			user)
	}

	return nil
}
