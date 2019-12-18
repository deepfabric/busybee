package expr

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestString(t *testing.T) {
	expr := metapb.Expr{
		Sources: []string{"k1"},
		Op:      metapb.Empty,
		Func:    metapb.First,
		Cmp:     metapb.Equal,
		Expect:  "v1",
	}
	v, err := NewRuntime(expr, nil)
	assert.Nil(t, err, "TestString failed")

	rt := v.(*runtime)
	ok, value, err := rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.True(t, ok, "TestString failed")
	assert.Equal(t, "v1", value, "TestString failed")

	rt.expr.Expect = "v2"
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.False(t, ok, "TestString failed")
	assert.Equal(t, "v1", value, "TestString failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Max
	rt.expr.Expect = "v2"
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.True(t, ok, "TestString failed")
	assert.Equal(t, rt.expr.Expect, value, "TestString failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Min
	rt.expr.Expect = "v1"
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.True(t, ok, "TestString failed")
	assert.Equal(t, rt.expr.Expect, value, "TestString failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Count
	rt.expr.Expect = "v1"
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.False(t, ok, "TestString failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Avg
	rt.expr.Expect = "v1"
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("v1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("v2"),
			},
		},
	})
	assert.NoError(t, err, "TestString failed")
	assert.False(t, ok, "TestString failed")
}

func TestNumber(t *testing.T) {
	expr := metapb.Expr{
		Sources: []string{"k1"},
		Op:      metapb.Empty,
		Func:    metapb.First,
		Cmp:     metapb.Equal,
		Expect:  "1",
		Type:    metapb.Number,
	}
	v, err := NewRuntime(expr, nil)
	assert.Nil(t, err, "TestNumber failed")

	rt := v.(*runtime)
	ok, value, err := rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("2"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.True(t, ok, "TestNumber failed")
	assert.Equal(t, rt.expectUint64Value, value, "TestNumber failed")

	rt.expectUint64Value = 2
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("2"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.False(t, ok, "TestNumber failed")
	assert.Equal(t, uint64(1), value, "TestNumber failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Max
	rt.expectUint64Value = 2
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("2"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.True(t, ok, "TestNumber failed")
	assert.Equal(t, rt.expectUint64Value, value, "TestNumber failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Min
	rt.expectUint64Value = 1
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("2"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.True(t, ok, "TestNumber failed")
	assert.Equal(t, rt.expectUint64Value, value, "TestNumber failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Count
	rt.expectUint64Value = 1
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("2"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.False(t, ok, "TestNumber failed")

	rt.expr.Sources = []string{"k1", "k2"}
	rt.expr.Func = metapb.Avg
	rt.expectUint64Value = 2
	ok, value, err = rt.Exec(&metapb.Event{
		Data: []metapb.KV{
			metapb.KV{
				Key:   []byte("k1"),
				Value: []byte("1"),
			},
			metapb.KV{
				Key:   []byte("k2"),
				Value: []byte("3"),
			},
		},
	})
	assert.NoError(t, err, "TestNumber failed")
	assert.True(t, ok, "TestNumber failed")
	assert.Equal(t, rt.expectUint64Value, value, "TestNumber failed")
}

type fetcher struct {
	m map[string]*roaring.Bitmap
}

func (f *fetcher) Bitmap(key []byte) (*roaring.Bitmap, error) {
	return f.m[string(key)], nil
}

func TestBitmap(t *testing.T) {
	f := &fetcher{
		m: make(map[string]*roaring.Bitmap),
	}

	f.m["k1"] = roaring.NewBTreeBitmap(1, 2, 3, 4)
	f.m["k2"] = roaring.NewBTreeBitmap(4, 5, 6, 7, 8)

	expr := metapb.Expr{
		Sources: []string{"k1", "k2"},
		Op:      metapb.Empty,
		Func:    metapb.Count,
		Cmp:     metapb.Equal,
		Expect:  "9",
		Type:    metapb.Bitmap,
	}
	v, err := NewRuntime(expr, f)
	assert.Nil(t, err, "TestBitmap failed")

	rt := v.(*runtime)
	ok, value, err := rt.Exec(&metapb.Event{})
	assert.NoError(t, err, "TestBitmap failed")
	assert.True(t, ok, "TestBitmap failed")
	assert.Equal(t, rt.expectUint64Value, value, "TestBitmap failed")

	rt.expr.Op = metapb.BMAnd
	rt.expectUint64Value = 1
	ok, value, err = rt.Exec(&metapb.Event{})
	assert.NoError(t, err, "TestBitmap failed")
	assert.True(t, ok, "TestBitmap failed")
	assert.Equal(t, rt.expectUint64Value, value.(*roaring.Bitmap).Count(), "TestBitmap failed")

	rt.expr.Op = metapb.BMOr
	rt.expectUint64Value = 8
	ok, value, err = rt.Exec(&metapb.Event{})
	assert.NoError(t, err, "TestBitmap failed")
	assert.True(t, ok, "TestBitmap failed")
	assert.Equal(t, rt.expectUint64Value, value.(*roaring.Bitmap).Count(), "TestBitmap failed")

	rt.expr.Op = metapb.BMXor
	rt.expectUint64Value = 7
	ok, value, err = rt.Exec(&metapb.Event{})
	assert.NoError(t, err, "TestBitmap failed")
	assert.True(t, ok, "TestBitmap failed")
	assert.Equal(t, rt.expectUint64Value, value.(*roaring.Bitmap).Count(), "TestBitmap failed")

	rt.expr.Op = metapb.BMAndNot
	rt.expectUint64Value = 3
	ok, value, err = rt.Exec(&metapb.Event{})
	assert.NoError(t, err, "TestBitmap failed")
	assert.True(t, ok, "TestBitmap failed")
	assert.Equal(t, rt.expectUint64Value, value.(*roaring.Bitmap).Count(), "TestBitmap failed")
}
