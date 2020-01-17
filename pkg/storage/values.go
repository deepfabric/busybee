package storage

var (
	kvType               byte = 0x00
	instanceStartingType byte = 0x01
	instanceStartedType  byte = 0x02
	instanceStoppedType  byte = 0x03
	stateType            byte = 0x04
)

func appendPrefix(value []byte, prefix byte) []byte {
	buf := acquireBuf()
	buf.WriteByte(prefix)
	buf.Write(value)
	_, data, _ := buf.ReadBytes(buf.Readable())
	releaseBuf(buf)
	return data
}
