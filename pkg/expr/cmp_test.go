package expr

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompareString(t *testing.T) {
	expr := metapb.Expr{}
	rt, err := NewRuntime(expr)
	assert.Nil(t, err, "test compare string value failed")

	src := rt.(*runtime)
	src.expr.Expect = "abc"

	src.expr.Cmp = metapb.Equal
	ok, err := compareString("abc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("1abc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")

	src.expr.Cmp = metapb.LT
	ok, err = compareString("abc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")
	ok, err = compareString("aac", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("bbc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")

	src.expr.Cmp = metapb.LE
	ok, err = compareString("abc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("aac", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("bbc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")

	src.expr.Cmp = metapb.GT
	ok, err = compareString("abc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")
	ok, err = compareString("acc", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("aac", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")

	expr = metapb.Expr{}
	expr.Cmp = metapb.Match
	expr.Expect = `^\d+$`
	rt, err = NewRuntime(expr)
	assert.Nil(t, err, "test compare string value failed")

	src = rt.(*runtime)
	ok, err = compareString("aac", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.False(t, ok, "test compare string value failed")
	ok, err = compareString("1", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
	ok, err = compareString("123", src)
	assert.Nil(t, err, "test compare string value failed")
	assert.True(t, ok, "test compare string value failed")
}

func TestCompareUint64(t *testing.T) {
	expr := metapb.Expr{}
	expr.Expect = "10"
	expr.Type = metapb.Number
	rt, err := NewRuntime(expr)
	assert.Nil(t, err, "test compare uint64 value failed")

	src := rt.(*runtime)

	src.expr.Cmp = metapb.Equal
	ok, err := compareUint64(10, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.True(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(9, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.False(t, ok, "test compare uint64 value failed")

	src.expr.Cmp = metapb.LT
	ok, err = compareUint64(10, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.False(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(9, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.True(t, ok, "test compare uint64 value failed")

	src.expr.Cmp = metapb.LE
	ok, err = compareUint64(10, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.True(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(9, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.True(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(11, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.False(t, ok, "test compare uint64 value failed")

	src.expr.Cmp = metapb.GT
	ok, err = compareUint64(10, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.False(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(11, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.True(t, ok, "test compare uint64 value failed")
	ok, err = compareUint64(9, src)
	assert.Nil(t, err, "test compare uint64 value failed")
	assert.False(t, ok, "test compare uint64 value failed")

	expr = metapb.Expr{}
	expr.Cmp = metapb.Match
	expr.Expect = `^\d+$`
	rt, err = NewRuntime(expr)
	assert.Nil(t, err, "test compare uint64 value failed")

	src = rt.(*runtime)
	_, err = compareUint64(10, src)
	assert.NotNil(t, err, "test compare uint64 value failed")
}
