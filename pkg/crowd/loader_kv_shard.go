package crowd

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	bbutil "github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

type putCtx struct {
	key   []byte
	index uint32
	ttl   uint32
	data  []byte
	wg    *sync.WaitGroup
}

type loadCtx struct {
	key   []byte
	index uint32
	cb    *combinationBytes
}

type kvShardLoader struct {
	store   storage.Storage
	runner  *task.Runner
	workerC []chan interface{}
	workers int
	op      uint64
	retry   time.Duration
	cache   *bbutil.Cache
}

// NewKVShardLoader returns a KV shard bitmap loader
func NewKVShardLoader(store storage.Storage, runner *task.Runner, workers int, retry time.Duration, maxCached uint64) Loader {
	l := &kvShardLoader{
		store:   store,
		runner:  runner,
		workers: workers,
		retry:   retry,
		cache:   bbutil.NewLRUCache(maxCached),
	}

	l.initWorkers()
	return l
}

func (l *kvShardLoader) Get(meta []byte) (*roaring.Bitmap, error) {
	var loadMeta metapb.ShardBitmapLoadMeta
	protoc.MustUnmarshal(&loadMeta, meta)

	key := loadMeta.String()
	if bm, ok := l.cache.Get(key); ok {
		return bm, nil
	}

	cb := newCombinationBytes(int(loadMeta.Shards))
	for i := uint32(0); i < loadMeta.Shards; i++ {
		cb.wg.Add(1)
		l.nextC() <- loadCtx{loadMeta.Key, i, cb}
	}

	cb.wg.Wait()
	if loadMeta.Total != cb.total {
		return nil, fmt.Errorf("Total size not match, expect %d but %d", loadMeta.Total, cb.total)
	}

	bm := bbutil.AcquireBitmap()
	_, err := bm.ReadFrom(cb)
	if err != nil {
		return nil, err
	}

	l.cache.Add(key, bm, loadMeta.Total)
	return bm, nil
}

func (l *kvShardLoader) Set(meta []byte, data []byte) (uint64, uint32, error) {
	var putMeta metapb.ShardBitmapPutMeta
	protoc.MustUnmarshal(&putMeta, meta)

	n := len(data)
	var wg sync.WaitGroup
	pos := 0
	next := 0
	index := uint32(0)
	for {
		next = pos + int(putMeta.BytesPerShard)
		if next > n {
			next = n
		}

		wg.Add(1)
		l.nextC() <- putCtx{putMeta.Key, index, putMeta.TTL, data[pos:next], &wg}
		index++
		if next >= n {
			break
		}

		pos = next
	}

	wg.Wait()
	return uint64(n), index, nil
}

func (l *kvShardLoader) initWorkers() {
	for i := 0; i < l.workers; i++ {
		c := make(chan interface{}, 128)
		l.workerC = append(l.workerC, c)
		l.do(c)
	}
}

func (l *kvShardLoader) do(c chan interface{}) {
	l.runner.RunCancelableTask(func(ctx context.Context) {
		buf := goetty.NewByteBuf(32)
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-c:
				if !ok {
					return
				}

				if value, ok := data.(loadCtx); ok {
					l.doLoad(value, buf)
				} else if value, ok := data.(putCtx); ok {
					l.doPut(value, buf)
				}
			}
		}
	})
}

func (l *kvShardLoader) doPut(value putCtx, buf *goetty.ByteBuf) {
	logger.Infof("put bitmap shard %s",
		hex.EncodeToString(value.key))

	buf.Clear()
	key := storage.ShardBitmapKey(value.key, value.index, buf)
	err := l.store.SetWithTTL(key, value.data, int64(value.ttl))
	if err != nil {
		logger.Infof("put bitmap shard %s failed with %+v, retry later",
			hex.EncodeToString(value.key),
			err)
		util.DefaultTimeoutWheel().Schedule(l.retry, l.addToRetry, value)
		l.addToRetry(value)
		return
	}

	logger.Infof("put bitmap shard %s completed",
		hex.EncodeToString(value.key))
	value.wg.Done()
}

func (l *kvShardLoader) doLoad(value loadCtx, buf *goetty.ByteBuf) {
	logger.Infof("load bitmap shard %s",
		hex.EncodeToString(value.key))

	buf.Clear()
	key := storage.ShardBitmapKey(value.key, value.index, buf)
	data, err := l.store.Get(key)
	if err != nil {
		logger.Errorf("load bitmap shard %s failed with %+v, retry later",
			hex.EncodeToString(value.key),
			err)
		util.DefaultTimeoutWheel().Schedule(l.retry, l.addToRetry, value)
		l.addToRetry(value)
		return
	}

	logger.Infof("load bitmap shard %s completed",
		hex.EncodeToString(value.key))
	value.cb.add(int(value.index), data)
}

func (l *kvShardLoader) nextC() chan interface{} {
	return l.workerC[atomic.AddUint64(&l.op, 1)%uint64(len(l.workerC))]
}

func (l *kvShardLoader) addToRetry(arg interface{}) {
	l.nextC() <- arg
}

type combinationBytes struct {
	sync.Mutex

	wg     sync.WaitGroup
	values [][]byte
	readed int
	total  uint64
}

func newCombinationBytes(shards int) *combinationBytes {
	return &combinationBytes{
		values: make([][]byte, shards, shards),
	}
}

func (cb *combinationBytes) add(index int, data []byte) {
	cb.Lock()
	defer cb.Unlock()

	cb.values[index] = data
	cb.total += uint64(len(data))
	cb.wg.Done()
}

func (cb *combinationBytes) Read(p []byte) (int, error) {
	size := len(p)
	pos := 0
	read := 0
	for _, data := range cb.values {
		cs := len(data)
		pos += cs
		if cb.readed < pos {
			unreadIdx := cs - (pos - cb.readed)
			n := copy(p[read:], data[unreadIdx:])
			read += n
			cb.readed += n
			if read == size {
				return read, nil
			}
		}
	}

	return read, io.EOF
}
