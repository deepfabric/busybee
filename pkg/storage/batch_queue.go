package storage

import (
	"math"
	"time"

	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	bhstorage "github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/busybee/pkg/metric"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/pb/rpcpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
)

func newQueueBatch() batchType {
	return &queueBatch{
		buf: goetty.NewByteBuf(256),
		pbs: make(map[uint32]*queuePartitionBatch),
	}
}

type queueBatch struct {
	tenant string
	buf    *goetty.ByteBuf

	// not reset
	loaded   bool
	pbs      map[uint32]*queuePartitionBatch
	ops      uint32
	metadata metapb.QueueState
}

func (qb *queueBatch) addReq(req *raftcmdpb.Request, resp *raftcmdpb.Response, b *batch, attrs map[string]interface{}) {
	switch rpcpb.Type(req.CustemType) {
	case rpcpb.QueueAdd:
		msg := getQueueAddRequest(attrs)
		protoc.MustUnmarshal(msg, req.Cmd)
		id := goetty.Byte2UInt64(msg.Key)
		target := goetty.Byte2UInt32(msg.Key[8:])
		prefix := req.Key[:len(req.Key)-len(msg.Key)]

		if !qb.loaded {
			key := QueueMetaKey(id, target)
			qb.buf.MarkWrite()
			qb.buf.Write(prefix)
			qb.buf.Write(key)
			value, err := b.bs.getStore(b.shard).Get(qb.buf.WrittenDataAfterMark().Data())
			if err != nil {
				log.Fatalf("load queue meta failed with %+v", err)
			}

			if len(value) == 0 {
				value := getUint64Response(attrs)
				value.Value = 0
				resp.Value = protoc.MustMarshal(value)
				return
			}

			protoc.MustUnmarshal(&qb.metadata, value)

			qb.tenant = string(format.UInt64ToString(id))
			qb.loaded = true
		}

		if !qb.matchCondition(msg, b, id, prefix) {
			value := getUint64Response(attrs)
			value.Value = 0
			resp.Value = protoc.MustMarshal(value)
			return
		}

		for _, item := range msg.Items {
			if msg.AllocPartition {
				target = qb.nextPartition()
			}

			pb, ok := qb.pbs[target]
			if !ok {
				pb = newPartitionBatch(b, qb, metapb.Group(req.Group), id, target, qb.buf)
				qb.pbs[target] = pb
			}

			pb.add(b, item)
		}

		n := len(msg.KVS) / 2
		for i := 0; i < n; i++ {
			b.wb.Set(queueKVKey(prefix, id, msg.KVS[2*i]), msg.KVS[2*i+1])
		}

		value := getUint64Response(attrs)
		value.Value = 0
		resp.Value = protoc.MustMarshal(value)
	default:
		log.Fatalf("BUG: not supoprt rpctype: %d", rpcpb.Type(req.CustemType))
	}
}

func (qb *queueBatch) exec(s bhstorage.DataStorage, b *batch) error {
	for _, pb := range qb.pbs {
		err := pb.exec(s, b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qb *queueBatch) support() []rpcpb.Type {
	return []rpcpb.Type{rpcpb.QueueAdd}
}

func (qb *queueBatch) reset() {
	for _, pb := range qb.pbs {
		pb.reset()
	}
	qb.buf.Clear()
}

func (qb *queueBatch) nextPartition() uint32 {
	v := qb.ops
	qb.ops++
	if qb.ops == qb.metadata.Partitions {
		qb.ops = 0
	}
	return v
}

func (qb *queueBatch) matchCondition(req *rpcpb.QueueAddRequest, b *batch, id uint64, prefix []byte) bool {
	cond := req.Condition
	if cond == nil {
		return true
	}

	value, err := b.bs.getStore(b.shard).Get(queueKVKey(prefix, id, cond.Key))
	if err != nil {
		log.Fatalf("load max queue offset failed with %+v", err)
	}

	return matchCondition(value, *req.Condition)
}

type queuePartitionBatch struct {
	loaded        bool
	maxOffset     uint64
	removedOffset uint64

	n                      int
	qb                     *queueBatch
	b                      *batch
	id                     uint64
	group                  metapb.Group
	buf                    *goetty.ByteBuf
	queueKey               []byte
	consumerStartKey       []byte
	consumerEndKey         []byte
	maxAndCleanOffsetKey   []byte
	maxAndCleanOffsetValue []byte
}

func newPartitionBatch(b *batch, qb *queueBatch, group metapb.Group, id uint64, partition uint32, buf *goetty.ByteBuf) *queuePartitionBatch {
	queueKey := raftstore.EncodeDataKey(uint64(group), PartitionKey(id, partition))

	pb := &queuePartitionBatch{
		maxAndCleanOffsetValue: make([]byte, 16, 16),
		buf:                    buf,
		qb:                     qb,
		b:                      b,
		id:                     id,
		group:                  group,
		queueKey:               queueKey,
		consumerStartKey:       consumerStartKey(queueKey),
		consumerEndKey:         consumerEndKey(queueKey),
		maxAndCleanOffsetKey:   maxAndCleanOffsetKey(queueKey),
	}

	return pb
}

func (qb *queuePartitionBatch) add(b *batch, item []byte) {
	if !qb.loaded {
		value, err := b.bs.getStore(b.shard).Get(qb.maxAndCleanOffsetKey)
		if err != nil {
			log.Fatalf("load max queue offset failed with %+v", err)
		}

		if len(value) > 0 {
			copy(qb.maxAndCleanOffsetValue, value)
			qb.maxOffset = goetty.Byte2UInt64(qb.maxAndCleanOffsetValue)
			qb.removedOffset = goetty.Byte2UInt64(qb.maxAndCleanOffsetValue[8:])
		}

		qb.loaded = true
	}

	qb.maxOffset++
	qb.b.keys = append(qb.b.keys, queueItemKey(qb.queueKey, qb.maxOffset, qb.buf))
	qb.b.values = append(qb.b.values, item)
	qb.n++
}

func (qb *queuePartitionBatch) exec(s bhstorage.DataStorage, b *batch) error {
	if qb.maxOffset > 0 {
		// clean [last clean offset, minimum committed offset in all consumers]
		if qb.maxOffset-qb.removedOffset >= qb.qb.metadata.CleanBatch {
			now := time.Now().Unix()
			low := uint64(math.MaxUint64)
			found := false
			err := s.Scan(qb.consumerStartKey, qb.consumerEndKey, func(key, value []byte) (bool, error) {
				v := goetty.Byte2UInt64(value)
				ts := goetty.Byte2Int64(value[8:])

				if (now-ts >= qb.qb.metadata.MaxAlive) && v < low {
					low = v
					found = true
				}

				return true, nil
			}, false)
			if err != nil {
				log.Fatalf("exec queue add batch failed with %+v", err)
			}

			if found && low > qb.removedOffset {
				from := QueueItemKey(qb.queueKey, qb.removedOffset)
				to := QueueItemKey(qb.queueKey, low+1)
				err = s.RangeDelete(from, to)
				if err != nil {
					log.Fatalf("exec queue add batch failed with %+v", err)
				}
				qb.removedOffset = low

				log.Infof("tenant %d truncate to offset %d of %s", qb.id, low, qb.group.String())
			}
		}

		goetty.Uint64ToBytesTo(qb.maxOffset, qb.maxAndCleanOffsetValue)
		goetty.Uint64ToBytesTo(qb.removedOffset, qb.maxAndCleanOffsetValue[8:])
		b.wb.Set(qb.maxAndCleanOffsetKey, qb.maxAndCleanOffsetValue)
	}

	metric.SetEventQueueSize(qb.maxOffset-qb.removedOffset, qb.qb.tenant, qb.group)
	metric.IncEventAdded(qb.n, qb.qb.tenant, qb.group)
	return nil
}

func (qb *queuePartitionBatch) reset() {
	qb.loaded = false
	qb.maxOffset = 0
	qb.removedOffset = 0
	qb.n = 0
}
