package crowd

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
)

var (
	memPool = goetty.NewSyncPool(4*goetty.MB, 1284*goetty.MB, 2)
)

type ckLoader struct {
	queryURL string
	address  string
	name     string
	password string
	table    []byte
}

// NewCKLoader returns a loader that load bitmap from clickhouse
func NewCKLoader(address, name, password string) Loader {
	table := make([]byte, 255, 255)
	table['0'] = 0
	table['r'] = 13
	table['n'] = 10
	table['\\'] = 92
	table['\''] = 39
	table['b'] = 8
	table['f'] = 12
	table['t'] = 9
	table['N'] = 0

	return &ckLoader{
		queryURL: fmt.Sprintf("%s?user=%s&password=%s&query=",
			address, name, password),
		address:  address,
		name:     name,
		password: password,
		table:    table,
	}
}

func (l *ckLoader) Get(data []byte) (*roaring.Bitmap, error) {
	sql := string(data)
	data, err := l.load(sql)
	if err != nil {
		return nil, err
	}

	var bm *roaring.Bitmap
	buf := goetty.WrapBytes(data)
	v, _ := buf.ReadByte()
	if v == 0 {
		bm = roaring.BitmapOf(l.readArray(buf)...)
	} else if v == 1 {
		l.readSize(buf)
		bm = util.MustParseBM(data[buf.GetReaderIndex():])
	}

	logger.Infof("load %d crowd from clickhouse<%s> with sql<%s>",
		bm.GetCardinality(), l.address, sql)
	return bm, nil
}

func (l *ckLoader) Set(key []byte, data []byte) (uint64, uint32, error) {
	return 0, 0, fmt.Errorf("Clickhouse loader not support Set")
}

func (l *ckLoader) load(sql string) ([]byte, error) {
	logger.Infof("send sql<%s> to clickhouse<%s>", sql, l.address)

	s := time.Now().Unix()
	resp, err := http.Get(fmt.Sprintf("%s%s", l.queryURL, url.QueryEscape(sql)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	logger.Infof("load %d bytes from clickhouse<%s>, cost %d sec",
		len(data), l.address, time.Now().Unix()-s)

	data = l.unescape(data)
	logger.Infof("unescape %d bytes", len(data))
	return data, nil
}

func (l *ckLoader) unescape(data []byte) []byte {
	pos := 0
	n := len(data)
	for i := 0; i < n; i++ {
		v := data[i]
		if v == 92 {
			i++
			data[pos] = l.table[data[i]]
			pos++
		} else if v == 10 {
			// pass
		} else {
			data[pos] = v
			pos++
		}
	}

	return data[:pos]
}

func (l *ckLoader) readArray(buf *goetty.ByteBuf) []uint32 {
	size := l.readSize(buf)
	value := make([]uint32, size, size)
	for i := uint64(0); i < size; i++ {
		value[i] = l.readUint32(buf)
	}

	return value
}

func (l *ckLoader) readUint32(buf *goetty.ByteBuf) uint32 {
	idx := buf.GetReaderIndex()
	value := buf.RawBuf()[idx:]
	v := binary.LittleEndian.Uint32(value)
	buf.SetReaderIndex(idx + 4)
	return v
}

func (l *ckLoader) readSize(buf *goetty.ByteBuf) uint64 {
	size, _ := binary.ReadUvarint(buf)
	return size
}
