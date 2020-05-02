package storage

import (
	"time"

	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

const (
	mappingIDPrefix            byte = 0
	mappingPrefix              byte = 1
	profilePrefix              byte = 2
	tenantMetadataPrefix       byte = 3
	tenantRunnerPrefix         byte = 4
	tenantRunnerMetadataPrefix byte = 5
	tenantRunnerWorkerPrefix   byte = 6
	workflowCurrentPrefix      byte = 7
	workflowHistoryPrefix      byte = 8
	workflowCrowdPrefix        byte = 9
	tempPrefix                 byte = 10

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

// PartitionKey returns partition key
func PartitionKey(id uint64, partition uint32) []byte {
	key := make([]byte, 12, 12)
	goetty.Uint64ToBytesTo(id, key)
	goetty.Uint32ToBytesTo(partition, key[8:])
	return key
}

// QueueKVKey returns partition kv key
func QueueKVKey(id uint64, src []byte) []byte {
	n := 9 + len(src)
	key := make([]byte, n, n)
	goetty.Uint64ToBytesTo(id, key)
	key[8] = queueKVField
	copy(key[9:], src)
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

// TenantRunnerKey tenant runner key
func TenantRunnerKey(tid uint64, runner uint64) []byte {
	key := make([]byte, 17, 17)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	return key
}

// TenantRunnerMetadataKey tenant runner metadata key
func TenantRunnerMetadataKey(tid uint64, runner uint64) []byte {
	key := make([]byte, 18, 18)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerMetadataPrefix
	return key
}

// TenantRunnerWorkerMinKey tenant runner worker min key
func TenantRunnerWorkerMinKey(tid uint64, runner uint64) []byte {
	key := make([]byte, 18, 18)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerWorkerPrefix
	return key
}

// TenantRunnerWorkerMaxKey tenant runner worker max key
func TenantRunnerWorkerMaxKey(tid uint64, runner uint64) []byte {
	key := make([]byte, 18, 18)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerWorkerPrefix + 1
	return key
}

// TenantRunnerWorkerKey tenant runner worker key
func TenantRunnerWorkerKey(tid uint64, runner uint64, wid uint64, worker uint32) []byte {
	key := make([]byte, 30, 30)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerWorkerPrefix
	goetty.Uint64ToBytesTo(wid, key[18:])
	goetty.Uint32ToBytesTo(worker, key[26:])
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

// QueueMetaKey returns concurrency queue meta key
func QueueMetaKey(id uint64, partition uint32, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteUInt64(id)
	buf.WriteUInt32(partition)
	buf.WriteByte(queueMetaField)
	return buf.WrittenDataAfterMark()
}

// QueueStateKey returns concurrency queue key
func QueueStateKey(id uint64, group []byte, buf *goetty.ByteBuf) []byte {
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

func queueKVKey(prefix []byte, id uint64, key []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(prefix)
	buf.WriteUInt64(id)
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

func copyKey(src []byte) []byte {
	dst := make([]byte, len(src), len(src))
	copy(dst, src)
	return dst
}
