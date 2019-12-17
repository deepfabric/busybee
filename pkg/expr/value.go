package expr

import (
	"github.com/pilosa/pilosa/roaring"
)

// ValueFetcher value fetcher
type ValueFetcher interface {
	Bitmap(key []byte) (*roaring.Bitmap, error)
}
