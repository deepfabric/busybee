package storage

import (
	"github.com/fagongzi/goetty"
)

var (
	kvType               byte = 0x00
	instanceStartingType byte = 0x01
	instanceStartedType  byte = 0x02
	instanceStoppingType byte = 0x03
	instanceStoppedType  byte = 0x04

	runingStateType  byte = 0x05
	stoppedStateType byte = 0x06
)

func appendValuePrefix(buf *goetty.ByteBuf, value []byte, prefix byte) []byte {
	idx := buf.GetWriteIndex()
	buf.WriteByte(prefix)
	buf.Write(value)
	return buf.RawBuf()[idx:buf.GetWriteIndex()]
}

// OriginInstanceStatePBValue returns origin instance state pb value
func OriginInstanceStatePBValue(value []byte) []byte {
	return value[8:]
}
