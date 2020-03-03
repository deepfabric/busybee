package storage

import (
	"time"

	"github.com/fagongzi/goetty"
)

func consumerCommittedValue(offset uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteUint64(offset)
	buf.WriteInt64(time.Now().Unix())
	return buf.WrittenDataAfterMark()
}

func int64Value(value int64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteInt64(value)
	return buf.WrittenDataAfterMark()
}

func uint64Value(value uint64, buf *goetty.ByteBuf) []byte {
	buf.MarkWrite()
	buf.WriteUint64(value)
	return buf.WrittenDataAfterMark()
}
