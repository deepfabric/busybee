package storage

import (
	"time"

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

func consumerCommittedValue(offset uint64, buf *goetty.ByteBuf) []byte {
	idx := buf.GetWriteIndex()
	buf.WriteUint64(offset)
	buf.WriteInt64(time.Now().Unix())
	return buf.RawBuf()[idx:buf.GetWriteIndex()]
}

func int64Value(value int64, buf *goetty.ByteBuf) []byte {
	idx := buf.GetWriteIndex()
	buf.WriteInt64(value)
	return buf.RawBuf()[idx:buf.GetWriteIndex()]
}

func uint64Value(value uint64, buf *goetty.ByteBuf) []byte {
	idx := buf.GetWriteIndex()
	buf.WriteUint64(value)
	return buf.RawBuf()[idx:buf.GetWriteIndex()]
}
