package expr

import (
	"github.com/RoaringBitmap/roaring"
)

// ValueFetcher value fetcher
type ValueFetcher interface {
	Bitmap(key []byte) (*roaring.Bitmap, error)
}
