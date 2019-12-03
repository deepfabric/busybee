package util

import (
	"hash/crc64"
)

var (
	table = crc64.MakeTable(crc64.ECMA)
)

// HashToUint64 hash to uint64
func HashToUint64(value []byte) uint64 {
	return crc64.Checksum(value, table)
}
