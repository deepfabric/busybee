package storage

import (
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

const (
	mappingIDPrefix       byte = 0
	mappingPrefix         byte = 1
	profilePrefix         byte = 2
	tenantMetadataPrefix  byte = 3
	workflowCurrentPrefix byte = 4
	workflowWorkerPrefix  byte = 5
	workflowHistoryPrefix byte = 6
	workflowStepTTLPrefix byte = 7
	workflowCrowdPrefix   byte = 8
	tempPrefix            byte = 9

	queueOffsetField    byte = 0
	queueItemField      byte = 1
	queueCommittedField byte = 2
	queueKVField        byte = 3
	queueStateField     byte = 4
	queueMetaField      byte = 5
)

// TempKey returns the temp key
func TempKey(key []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteByte(tempPrefix)
	buf.WriteInt64(time.Now().Unix())
	buf.Write(key)
	return buf.WrittenDataAfterMark()
}

// WorkflowStepTTLKey returns the ttl key for user on the step
func WorkflowStepTTLKey(wid uint64, uid uint32, name string, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteByte(workflowStepTTLPrefix)
	buf.WriteUInt64(wid)
	buf.WriteUInt32(uid)
	buf.WriteString(name)
	return buf.WrittenDataAfterMark()
}

// PartitionKey returns partition key
func PartitionKey(id uint64, partition uint32) []byte {
	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(id, key)
	goetty.Uint32ToBytesTo(partition, key[8:])
	return key
}

// PartitionKVKey returns partition kv key
func PartitionKVKey(id uint64, partition uint32, src []byte) []byte {
	prefixKey := PartitionKey(id, partition)
	n := len(prefixKey) + len(src) + 1
	key := make([]byte, n, n)
	copy(key, prefixKey)
	key[len(prefixKey)] = queueKVField
	copy(key[len(prefixKey)+1:], src)
	return key
}

// TenantMetadataKey returns queue metadata key
func TenantMetadataKey(id uint64) []byte {
	key := make([]byte, 9, 9)
	key[0] = tenantMetadataPrefix
	goetty.Uint64ToBytesTo(id, key[1:])
	return key
}

// WorkflowCurrentInstanceKey workflow current instance key
func WorkflowCurrentInstanceKey(id uint64) []byte {
	key := make([]byte, 9, 9)
	key[0] = workflowCurrentPrefix
	goetty.Uint64ToBytesTo(id, key[1:])
	return key
}

// WorkflowHistoryInstanceKey returns workflow instance history key
func WorkflowHistoryInstanceKey(wid, instanceID uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteByte(workflowHistoryPrefix)
	buf.WriteUInt64(wid)
	buf.WriteUInt64(instanceID)
	return buf.WrittenDataAfterMark()
}

// ShardBitmapKey returns bitmap shard key
func ShardBitmapKey(key []byte, index uint32, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(key)
	buf.WriteUInt32(index)
	return buf.WrittenDataAfterMark()
}

// WorkflowCrowdShardKey returns workflow instance crow shard key
func WorkflowCrowdShardKey(wid, instanceID uint64, version uint32, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteByte(workflowCrowdPrefix)
	buf.WriteUInt64(wid)
	buf.WriteUInt64(instanceID)
	buf.WriteUInt32(version)
	return buf.WrittenDataAfterMark()
}

// InstanceShardKey instance shard key
func InstanceShardKey(id uint64, index uint32) []byte {
	key := make([]byte, 13, 13)
	key[0] = workflowWorkerPrefix
	goetty.Uint64ToBytesTo(id, key[1:])
	goetty.Uint32ToBytesTo(index, key[9:])
	return key
}

// MappingIDKey returns a user id key,
func MappingIDKey(tenantID uint64, userID uint32) []byte {
	key := make([]byte, 13, 13)
	key[0] = mappingIDPrefix
	goetty.Uint64ToBytesTo(tenantID, key[1:])
	goetty.Uint32ToBytesTo(userID, key[9:])
	return key
}

// MappingKey returns a mapping key
func MappingKey(tenantID uint64, from metapb.IDValue, to string) []byte {
	size := 9 + len(from.Value) + len(from.Type) + len(to)
	key := make([]byte, size, size)
	key[0] = mappingPrefix
	goetty.Uint64ToBytesTo(tenantID, key[1:])
	idx := 9
	copy(key[idx:], hack.StringToSlice(from.Type))
	idx += len(from.Type)
	copy(key[idx:], hack.StringToSlice(to))
	idx += len(to)
	copy(key[idx:], hack.StringToSlice(from.Value))
	return key
}

// ProfileKey returns a profile key
func ProfileKey(tenantID uint64, uid uint32) []byte {
	key := make([]byte, 13, 13)
	key[0] = profilePrefix
	goetty.Uint64ToBytesTo(tenantID, key[1:])
	goetty.Uint32ToBytesTo(uid, key[9:])
	return key
}

// maxAndCleanOffsetKey store the max offset and already clean offset of the current queue
func maxAndCleanOffsetKey(src []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueOffsetField)
	return buf.WrittenDataAfterMark()
}

// QueueItemKey store the item at the offset in the queue
func QueueItemKey(src []byte, offset uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueItemField)
	buf.WriteUint64(offset)
	return buf.WrittenDataAfterMark()
}

// ConcurrencyQueueMetaKey returns concurrency queue meta key
func ConcurrencyQueueMetaKey(id uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteUInt64(id)
	buf.WriteByte(queueMetaField)
	return buf.WrittenDataAfterMark()
}

// ConcurrencyQueueStateKey returns concurrency queue key
func ConcurrencyQueueStateKey(id uint64, group []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteUInt64(id)
	buf.WriteByte(queueStateField)
	buf.Write(group)
	return buf.WrittenDataAfterMark()
}

func queueMetaKey(src []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueMetaField)
	return buf.WrittenDataAfterMark()
}

func queueStateKey(src []byte, group []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueStateField)
	buf.Write(group)
	return buf.WrittenDataAfterMark()
}

func committedOffsetKey(src []byte, consumer []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueCommittedField)
	buf.Write(consumer)
	return buf.WrittenDataAfterMark()
}

func queueKVKey(src []byte, key []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueKVField)
	buf.Write(key)
	return buf.WrittenDataAfterMark()
}

func consumerStartKey(src []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueCommittedField)
	return buf.WrittenDataAfterMark()
}

func consumerEndKey(src []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueCommittedField + 1)
	return buf.WrittenDataAfterMark()
}

func copyKey(key []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(key)
	return buf.WrittenDataAfterMark()
}
