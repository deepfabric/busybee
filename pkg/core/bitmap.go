package core

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/crowd"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/fagongzi/util/protoc"
)

func (eng *engine) initBMLoaders() {
	eng.loaders[metapb.KVLoader] = crowd.NewKVLoader(eng.store)
	eng.loaders[metapb.RawLoader] = crowd.NewRawLoader()
	eng.loaders[metapb.KVShardLoader] = crowd.NewKVShardLoader(eng.store, eng.runner,
		eng.opts.bitmapWorkers, eng.opts.retryInterval, eng.opts.bitmapMaxCacheBytes)
	eng.loaders[metapb.ClickhouseLoader] = crowd.NewCKLoader(eng.opts.clickhouseAddress,
		eng.opts.clickhouseUserName, eng.opts.clickhousePassword)
}

func (eng *engine) loadBM(loader metapb.BMLoader, meta []byte) (*roaring.Bitmap, error) {
	if l, ok := eng.loaders[loader]; ok {
		return l.Get(meta)
	}

	return nil, fmt.Errorf("not support bitmap loader %s", loader.String())
}

func (eng *engine) putBM(bm *roaring.Bitmap, key []byte, ttl uint32) (metapb.BMLoader, []byte, error) {
	data := util.MustMarshalBM(bm)
	if uint64(len(data)) <= eng.opts.shardBitmapBytes {
		return metapb.RawLoader, data, nil
	}

	total, shards, err := eng.putShardBM(metapb.KVShardLoader, &metapb.ShardBitmapPutMeta{
		Key:           key,
		TTL:           ttl,
		BytesPerShard: uint32(eng.opts.shardBitmapBytes),
	}, data)
	if err != nil {
		return metapb.RawLoader, nil, err
	}

	return metapb.KVShardLoader, protoc.MustMarshal(&metapb.ShardBitmapLoadMeta{
		Key:    key,
		Total:  total,
		Shards: shards,
	}), nil
}

func (eng *engine) putShardBM(loader metapb.BMLoader, meta *metapb.ShardBitmapPutMeta, data []byte) (uint64, uint32, error) {
	if l, ok := eng.loaders[loader]; ok {
		return l.Set(protoc.MustMarshal(meta), data)
	}

	return 0, 0, fmt.Errorf("not support bitmap loader %s", loader.String())
}
