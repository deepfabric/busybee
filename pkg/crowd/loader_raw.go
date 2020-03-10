package crowd

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/util"
)

type rawLoader struct {
}

// NewRawLoader returns raw loader
func NewRawLoader() Loader {
	return &rawLoader{}
}

func (l *rawLoader) Get(value []byte) (*roaring.Bitmap, error) {
	total := uint64(len(value))
	bm := util.AcquireBitmap()
	err := bm.UnmarshalBinary(value)
	if err != nil {
		return nil, err
	}

	if total < kb {
		logger.Debugf("load %d crowd from raw %d bytes",
			bm.GetCardinality(),
			total)
	} else if total < mb {
		logger.Debugf("load %d crowd from raw %d KB",
			bm.GetCardinality(),
			total/kb)
	} else {
		logger.Debugf("load %d crowd from raw %d MB",
			bm.GetCardinality(),
			total/mb)
	}

	return bm, nil
}

func (l *rawLoader) Set(key []byte, data []byte) (uint64, uint32, error) {
	return 0, 0, fmt.Errorf("Raw loader not support Set")
}
