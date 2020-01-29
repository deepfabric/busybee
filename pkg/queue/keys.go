package queue

import (
	"github.com/fagongzi/goetty"
)

// PartitionKey returns partition key
func PartitionKey(id, partition uint64) []byte {
	key := make([]byte, 16, 16)
	goetty.Uint64ToBytesTo(id, key)
	goetty.Uint64ToBytesTo(partition, key[8:])
	return key
}
