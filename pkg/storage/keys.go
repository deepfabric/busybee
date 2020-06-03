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

	queueMaxOffsetField     byte = 0
	queueRemovedOffsetField byte = 1
	queueItemField          byte = 2
	queueCommittedField     byte = 3
	queueKVField            byte = 4
	queueStateField         byte = 5
	queueMetaField          byte = 6
)

// TempKey returns the temp key
func TempKey(value []byte) []byte {
	n := 1 + 8 + len(value)
	key := make([]byte, n, n)
	key[0] = tempPrefix
	goetty.Int64ToBytesTo(time.Now().Unix(), key[1:])
	copy(key[9:], value)
	return key
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
func WorkflowHistoryInstanceKey(wid, instanceID uint64) []byte {
	key := make([]byte, 17, 17)
	key[0] = workflowHistoryPrefix
	goetty.Uint64ToBytesTo(wid, key[1:])
	goetty.Uint64ToBytesTo(instanceID, key[9:])
	return key
}

// ShardBitmapKey returns bitmap shard key
func ShardBitmapKey(value []byte, index uint32) []byte {
	n := len(value)
	key := make([]byte, n+4, n+4)
	copy(key, value)
	goetty.Uint32ToBytesTo(index, key[n:])
	return key
}

// WorkflowCrowdShardKey returns workflow instance crow shard key
func WorkflowCrowdShardKey(wid, instanceID uint64, version uint32) []byte {
	key := make([]byte, 21, 21)
	key[0] = workflowCrowdPrefix
	goetty.Uint64ToBytesTo(wid, key[1:])
	goetty.Uint64ToBytesTo(instanceID, key[9:])
	goetty.Uint32ToBytesTo(version, key[17:])
	return key
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

// TenantRunnerWorkerInstanceMinKey tenant runner worker min key
func TenantRunnerWorkerInstanceMinKey(tid uint64, runner uint64, wid uint64) []byte {
	key := make([]byte, 26, 26)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerWorkerPrefix
	goetty.Uint64ToBytesTo(wid, key[18:])
	return key
}

// TenantRunnerWorkerInstanceMaxKey tenant runner worker min key
func TenantRunnerWorkerInstanceMaxKey(tid uint64, runner uint64, wid uint64) []byte {
	key := make([]byte, 26, 26)
	key[0] = tenantRunnerPrefix
	goetty.Uint64ToBytesTo(tid, key[1:])
	goetty.Uint64ToBytesTo(runner, key[9:])
	key[17] = tenantRunnerWorkerPrefix
	goetty.Uint64ToBytesTo(wid+1, key[18:])
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

func maxOffsetKey(src []byte) []byte {
	n := len(src)
	key := make([]byte, n+1, n+1)
	copy(key, src)
	key[n] = queueMaxOffsetField
	return key
}

func removedOffsetKey(src []byte) []byte {
	n := len(src)
	key := make([]byte, n+1, n+1)
	copy(key, src)
	key[n] = queueRemovedOffsetField
	return key
}

// QueueItemKey store the item at the offset in the queue
func QueueItemKey(src []byte, offset uint64) []byte {
	n := len(src)
	key := make([]byte, n+9, n+9)
	copy(key, src)
	key[n] = queueItemField
	goetty.Uint64ToBytesTo(offset, key[n+1:])
	return key
}

func queueItemKey(src []byte, offset uint64, buf *goetty.ByteBuf) goetty.Slice {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueItemField)
	buf.WriteUint64(offset)
	return buf.WrittenDataAfterMark()
}

// QueueMetaKey returns concurrency queue meta key
func QueueMetaKey(id uint64, partition uint32) []byte {
	key := make([]byte, 13, 13)
	goetty.Uint64ToBytesTo(id, key)
	goetty.Uint32ToBytesTo(partition, key[8:])
	key[12] = queueMetaField
	return key
}

// QueueStateKey returns concurrency queue key
func QueueStateKey(id uint64, group []byte) []byte {
	n := len(group)
	key := make([]byte, n+9, n+9)
	goetty.Uint64ToBytesTo(id, key)
	key[8] = queueStateField
	copy(key[9:], group)
	return key
}

func queueMetaKey(src []byte) []byte {
	n := len(src)
	key := make([]byte, n+1, n+1)
	copy(key, src)
	key[n] = queueMetaField
	return key
}

func queueStateKey(src []byte, group []byte) []byte {
	n1 := len(src)
	n2 := len(group)

	key := make([]byte, n1+n2+1, n1+n2+1)
	copy(key, src)
	key[n1] = queueStateField
	copy(key[n1+1:], group)
	return key
}

func committedOffsetKey(src []byte, consumer []byte) []byte {
	n1 := len(src)
	n2 := len(consumer)

	key := make([]byte, n1+n2+1, n1+n2+1)
	copy(key, src)
	key[n1] = queueCommittedField
	copy(key[n1+1:], consumer)
	return key
}

func decodeConsumerFromCommittedOffsetKey(src []byte) []byte {
	return src[13:]
}

func queueKVKey(prefix []byte, id uint64, src []byte) []byte {
	n1 := len(prefix)
	n2 := len(src)

	key := make([]byte, n1+n2+9, n1+n2+9)
	copy(key, prefix)
	goetty.Uint64ToBytesTo(id, key[n1:])
	key[n1+8] = queueKVField
	copy(key[n1+9:], src)
	return key
}

func consumerStartKey(src []byte) []byte {
	n := len(src)
	key := make([]byte, n+1, n+1)
	copy(key, src)
	key[n] = queueCommittedField
	return key
}

func consumerEndKey(src []byte) []byte {
	n := len(src)
	key := make([]byte, n+1, n+1)
	copy(key, src)
	key[n] = queueCommittedField + 1
	return key
}
