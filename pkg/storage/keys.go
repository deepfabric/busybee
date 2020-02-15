package storage

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
)

const (
	mappingIDPrefix      byte = 0
	mappingPrefix        byte = 1
	profilePrefix        byte = 2
	queueMetadataPrefix  byte = 3
	workflowMetataPrefix byte = 4

	instance      byte = 1
	instanceShard byte = 2

	queueOffsetField    byte = 0
	queueItemField      byte = 1
	queueCommittedField byte = 2
)

// PartitionKey returns partition key
func PartitionKey(id, partition uint64) []byte {
	key := make([]byte, 16, 16)
	goetty.Uint64ToBytesTo(id, key)
	goetty.Uint64ToBytesTo(partition, key[8:])
	return key
}

// QueueMetadataKey returns queue metadata key
func QueueMetadataKey(id uint64, group metapb.Group) []byte {
	key := make([]byte, 13, 13)
	key[0] = queueMetadataPrefix
	goetty.Uint64ToBytesTo(id, key[1:])
	goetty.Uint32ToBytesTo(uint32(group), key[9:])
	return key
}

// StartedInstanceKey instance key
func StartedInstanceKey(id uint64) []byte {
	key := make([]byte, 10, 10)
	key[0] = workflowMetataPrefix
	key[1] = instance
	goetty.Uint64ToBytesTo(id, key[2:])
	return key
}

// InstanceShardKey instance shard key
func InstanceShardKey(id uint64, index uint32) []byte {
	key := make([]byte, 14, 14)
	key[0] = workflowMetataPrefix
	key[1] = instanceShard
	goetty.Uint64ToBytesTo(id, key[2:])
	goetty.Uint32ToBytesTo(index, key[10:])
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

// itemKey store the item at the offset in the queue
func itemKey(src []byte, offset uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueItemField)
	buf.WriteUint64(offset)
	return buf.WrittenDataAfterMark()
}

// committedOffsetKey store the commttied offset per consumer
func committedOffsetKey(src []byte, consumer []byte, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.Write(src)
	buf.WriteByte(queueCommittedField)
	buf.Write(consumer)
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
