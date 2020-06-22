package notify

import (
	"sync/atomic"
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/uuid"
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

func (n *queueNotifier) Notify(id uint64, notifies []metapb.Notify, cond *rpcpb.Condition, kvs ...[]byte) error {
	ts := time.Now().Unix()
	var items [][]byte
	var keys [][]byte
	for idx := range notifies {
		key := storage.OutputNotifyKey(id, ts, uuid.NewV4().Bytes())
		keys = append(keys, key)
		items = append(items, protoc.MustMarshal(&notifies[idx]))
	}

	ctx := &asyncSetCtx{}
	ctx.total = uint64(len(notifies))
	ctx.completed = uint64(0)
	ctx.c = make(chan struct{})
	ctx.store = n.store
	ctx.keys = keys
	ctx.items = items
	for idx := range notifies {
		req := rpcpb.AcquireSetRequest()
		req.Key = keys[idx]
		req.Value = items[idx]
		n.store.AsyncExecCommand(req, ctx.cb, idx)
	}

	ctx.wait()
	return n.store.PutToQueueWithAllocAndKVAndCondition(id, n.group, keys, cond, kvs...)
}

type asyncSetCtx struct {
	total     uint64
	completed uint64
	c         chan struct{}
	store     storage.Storage
	items     [][]byte
	keys      [][]byte
}

func (ctx *asyncSetCtx) cb(arg interface{}, value []byte, err error) {
	if err != nil {
		idx := arg.(int)
		req := rpcpb.AcquireSetRequest()
		req.Key = ctx.keys[idx]
		req.Value = ctx.items[idx]
		ctx.store.AsyncExecCommand(req, ctx.cb, arg)
		return
	}

	v := atomic.AddUint64(&ctx.completed, 1)
	if v == ctx.total {
		close(ctx.c)
	}
}

func (ctx *asyncSetCtx) wait() {
	<-ctx.c
}
