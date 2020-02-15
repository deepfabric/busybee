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

	runningStateType byte = 0x05
	stoppedStateType byte = 0x06
)

func appendValuePrefix(buf *goetty.ByteBuf, value []byte, prefix byte) []byte {
	buf.WrittenDataAfterMark()
	buf.WriteByte(prefix)
	buf.Write(value)
	return buf.WrittenDataAfterMark()
}

// RunningInstanceState returns the  is running state key
func RunningInstanceState(value []byte) bool {
	return value[0] == runningStateType
}

// OriginInstanceStatePBValue returns origin instance state pb value
func OriginInstanceStatePBValue(value []byte) []byte {
	return value[8:]
}

func consumerCommittedValue(offset uint64, buf *goetty.ByteBuf) []byte {
	buf.WrittenDataAfterMark()
	buf.WriteUint64(offset)
	buf.WriteInt64(time.Now().Unix())
	return buf.WrittenDataAfterMark()
}

func int64Value(value int64, buf *goetty.ByteBuf) []byte {
	buf.WrittenDataAfterMark()
	buf.WriteInt64(value)
	return buf.WrittenDataAfterMark()
}

func uint64Value(value uint64, buf *goetty.ByteBuf) []byte {
	buf.WrittenDataAfterMark()
	buf.WriteUint64(value)
	return buf.WrittenDataAfterMark()
}
