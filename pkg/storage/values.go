package storage

var (
	kvType       byte = 0x00
	instanceType byte = 0x01
	stateType    byte = 0x02
)

func appendPrefix(value []byte, prefix byte) []byte {
	buf := acquireBuf()
	buf.WriteByte(prefix)
	buf.Write(value)
	_, data, _ := buf.ReadBytes(buf.Readable())
	releaseBuf(buf)
	return data
}
