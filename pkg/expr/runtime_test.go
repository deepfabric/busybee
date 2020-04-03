package expr

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/util"
	"github.com/stretchr/testify/assert"
)

type testCtx struct {
	event *metapb.UserEvent
	kvs   map[string]string
}

func newTestCtx() *testCtx {
	return &testCtx{
		kvs:   make(map[string]string),
		event: &metapb.UserEvent{},
	}
}

func (c *testCtx) setEventKV(key, value string) {
	for idx := range c.event.Data {
		if string(c.event.Data[idx].Key) == key {
			c.event.Data[idx].Value = []byte(value)
			return
		}
	}

	c.event.Data = append(c.event.Data, metapb.KV{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (c *testCtx) Event() *metapb.UserEvent {
	return c.event
}

func (c *testCtx) Profile(key []byte) ([]byte, error) {
	return c.KV(key)
}

func (c *testCtx) KV(key []byte) ([]byte, error) {
	if v, ok := c.kvs[string(key)]; ok {
		return []byte(v), nil
	}

	return nil, nil
}

func (c *testCtx) TotalCrowd() *roaring.Bitmap {
	return nil
}

func (c *testCtx) StepCrowd() *roaring.Bitmap {
	return nil
}

func TestFetchFromEvent(t *testing.T) {
	ctx := newTestCtx()
	ctx.setEventKV("key1", "1")

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: event.key1}==1"),
	})
	assert.NoError(t, err, "TestFetchFromEvent failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestFetchFromEvent failed")
	assert.Nil(t, value, "TestFetchFromEvent failed")
	assert.True(t, ok, "TestFetchFromEvent failed")
}

func TestFetchFromKV(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}==1"),
	})
	assert.NoError(t, err, "TestFetchFromKV failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestFetchFromKV failed")
	assert.Nil(t, value, "TestFetchFromKV failed")
	assert.True(t, ok, "TestFetchFromKV failed")
}

func TestFetchFromProfile(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: profile.key1}==1"),
	})
	assert.NoError(t, err, "TestFetchFromProfile failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestFetchFromProfile failed")
	assert.Nil(t, value, "TestFetchFromProfile failed")
	assert.True(t, ok, "TestFetchFromProfile failed")
}

func TestAddWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}+{num: kv.key2}==3"),
	})
	assert.NoError(t, err, "TestAddWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestAddWithNumber failed")
	assert.Nil(t, value, "TestAddWithNumber failed")
	assert.True(t, ok, "TestAddWithNumber failed")
}

func TestAddWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}+{kv.key2}==12"),
	})
	assert.NoError(t, err, "TestAddWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestAddWithString failed")
	assert.Nil(t, value, "TestAddWithString failed")
	assert.True(t, ok, "TestAddWithString failed")
}

func TestInWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1} in [1,2]"),
	})
	assert.NoError(t, err, "TestInWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestInWithString failed")
	assert.Nil(t, value, "TestInWithString failed")
	assert.True(t, ok, "TestInWithString failed")

	ctx.kvs["key1"] = "3"
	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestInWithString failed")
	assert.Nil(t, value, "TestInWithString failed")
	assert.False(t, ok, "TestInWithString failed")
}

func TestInWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1} in [1,2]"),
	})
	assert.NoError(t, err, "TestInWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestInWithString failed")
	assert.Nil(t, value, "TestInWithString failed")
	assert.True(t, ok, "TestInWithString failed")

	ctx.kvs["key1"] = "3"
	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestInWithString failed")
	assert.Nil(t, value, "TestInWithString failed")
	assert.False(t, ok, "TestInWithString failed")
}

func TestNotInWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1} !in [1,2]"),
	})
	assert.NoError(t, err, "TestNotInWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestNotInWithString failed")
	assert.Nil(t, value, "TestNotInWithString failed")
	assert.False(t, ok, "TestNotInWithString failed")

	ctx.kvs["key1"] = "3"
	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestNotInWithString failed")
	assert.Nil(t, value, "TestNotInWithString failed")
	assert.True(t, ok, "TestNotInWithString failed")
}

func TestNotInWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1} !in [1,2]"),
	})
	assert.NoError(t, err, "TestNotInWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestNotInWithNumber failed")
	assert.Nil(t, value, "TestNotInWithNumber failed")
	assert.False(t, ok, "TestNotInWithNumber failed")

	ctx.kvs["key1"] = "3"
	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestNotInWithNumber failed")
	assert.Nil(t, value, "TestNotInWithNumber failed")
	assert.True(t, ok, "TestNotInWithNumber failed")
}

func TestMinusWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}-{num: kv.key2}==1"),
	})
	assert.NoError(t, err, "TestMinusWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestMinusWithNumber failed")
	assert.Nil(t, value, "TestMinusWithNumber failed")
	assert.True(t, ok, "TestMinusWithNumber failed")
}

func TestMinusWithBitmap(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2, 3)))
	ctx.kvs["key2"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2)))

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{bm: kv.key1}-{bm: kv.key2}"),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestMinusWithBitmap failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestMinusWithBitmap failed")
	assert.NotNil(t, value, "TestMinusWithBitmap failed")
	assert.True(t, ok, "TestMinusWithBitmap failed")
	assert.Equal(t, uint64(1), value.(*roaring.Bitmap).GetCardinality(), "TestMinusWithBitmap failed")

	ctx.kvs["key2"] = "1"
	rt, err = NewRuntime(metapb.Expr{
		Value: []byte("{bm:kv.key1}-{num:kv.key2}"),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestMinusWithBitmap failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestMinusWithBitmap failed")
	assert.NotNil(t, value, "TestMinusWithBitmap failed")
	assert.True(t, ok, "TestMinusWithBitmap failed")
	assert.Equal(t, uint64(2), value.(*roaring.Bitmap).GetCardinality(), "TestMinusWithBitmap failed")
}

func TestMultiplication(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}*{num: kv.key2}==2"),
	})
	assert.NoError(t, err, "TestMultiplication failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestMultiplication failed")
	assert.Nil(t, value, "TestMultiplication failed")
	assert.True(t, ok, "TestMultiplication failed")
}

func TestDivision(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}/{num: kv.key2}==2"),
	})
	assert.NoError(t, err, "TestDivision failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestDivision failed")
	assert.Nil(t, value, "TestDivision failed")
	assert.True(t, ok, "TestDivision failed")

	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"
	rt, err = NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}/{num: kv.key2}==0"),
	})
	assert.NoError(t, err, "TestDivision failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestDivision failed")
	assert.Nil(t, value, "TestDivision failed")
	assert.True(t, ok, "TestDivision failed")

}

func TestMod(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "3"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}%{num: kv.key2}==1"),
	})
	assert.NoError(t, err, "TestMod failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestMod failed")
	assert.Nil(t, value, "TestMod failed")
	assert.True(t, ok, "TestMod failed")

	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"
	rt, err = NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}/{num: kv.key2}==0"),
	})
	assert.NoError(t, err, "TestDivision failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestDivision failed")
	assert.Nil(t, value, "TestDivision failed")
	assert.True(t, ok, "TestDivision failed")

}

func TestLTWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}<{num: kv.key2}"),
	})
	assert.NoError(t, err, "TestLTWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLTWithNumber failed")
	assert.Nil(t, value, "TestLTWithNumber failed")
	assert.True(t, ok, "TestLTWithNumber failed")
}

func TestLTWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "11"
	ctx.kvs["key2"] = "12"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}<{kv.key2}"),
	})
	assert.NoError(t, err, "TestLTWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLTWithString failed")
	assert.Nil(t, value, "TestLTWithString failed")
	assert.True(t, ok, "TestLTWithString failed")
}

func TestLEWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}<={num: kv.key2}"),
	})
	assert.NoError(t, err, "TestLEWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLEWithNumber failed")
	assert.Nil(t, value, "TestLEWithNumber failed")
	assert.True(t, ok, "TestLEWithNumber failed")
}

func TestLEWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "12"
	ctx.kvs["key2"] = "12"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}<={kv.key2}"),
	})
	assert.NoError(t, err, "TestLEWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLEWithString failed")
	assert.Nil(t, value, "TestLEWithString failed")
	assert.True(t, ok, "TestLEWithString failed")
}

func TestGTWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}>{num: kv.key2}"),
	})
	assert.NoError(t, err, "TestGTWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestGTWithNumber failed")
	assert.Nil(t, value, "TestGTWithNumber failed")
	assert.True(t, ok, "TestGTWithNumber failed")
}

func TestGTWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "12"
	ctx.kvs["key2"] = "11"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}>{kv.key2}"),
	})
	assert.NoError(t, err, "TestGTWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestGTWithString failed")
	assert.Nil(t, value, "TestGTWithString failed")
	assert.True(t, ok, "TestGTWithString failed")
}

func TestGEWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "2"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num: kv.key1}>={num: kv.key2}"),
	})
	assert.NoError(t, err, "TestGEWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestGEWithNumber failed")
	assert.Nil(t, value, "TestGEWithNumber failed")
	assert.True(t, ok, "TestGEWithNumber failed")
}

func TestGEWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "12"
	ctx.kvs["key2"] = "12"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}>={kv.key2}"),
	})
	assert.NoError(t, err, "TestGEWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestGEWithString failed")
	assert.Nil(t, value, "TestGEWithString failed")
	assert.True(t, ok, "TestGEWithString failed")
}

func TestEqualWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num:kv.key1}=={num:kv.key2}"),
	})
	assert.NoError(t, err, "TestEqualWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestEqualWithNumber failed")
	assert.Nil(t, value, "TestEqualWithNumber failed")
	assert.True(t, ok, "TestEqualWithNumber failed")
}

func TestEqualWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}=={kv.key2}"),
	})
	assert.NoError(t, err, "TestEqualWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestEqualWithString failed")
	assert.Nil(t, value, "TestEqualWithString failed")
	assert.True(t, ok, "TestEqualWithString failed")
}

func TestNotEqualWithNumber(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"
	ctx.kvs["key2"] = "2"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{num:kv.key1}!={num:kv.key2}"),
	})
	assert.NoError(t, err, "TestNotEqualWithNumber failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestNotEqualWithNumber failed")
	assert.Nil(t, value, "TestNotEqualWithNumber failed")
	assert.True(t, ok, "TestNotEqualWithNumber failed")
}

func TestNotEqualWithString(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "12"
	ctx.kvs["key2"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{kv.key1}!={kv.key2}"),
	})
	assert.NoError(t, err, "TestNotEqualWithString failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestNotEqualWithString failed")
	assert.Nil(t, value, "TestNotEqualWithString failed")
	assert.True(t, ok, "TestNotEqualWithString failed")
}

func TestMatch(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "1"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{kv.key1} ~ "^[0-9]*$"`),
	})
	assert.NoError(t, err, "TestMatch failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestMatch failed")
	assert.Nil(t, value, "TestMatch failed")
	assert.True(t, ok, "TestMatch failed")

	rt, err = NewRuntime(metapb.Expr{
		Value: []byte(`{kv.key1} ~ |^[0-9]*$|`),
	})
	assert.NoError(t, err, "TestMatch failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestMatch failed")
	assert.Nil(t, value, "TestMatch failed")
	assert.True(t, ok, "TestMatch failed")
}

func TestNotMatch(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "abc"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{kv.key1} !~ "^[0-9]*$"`),
	})
	assert.NoError(t, err, "TestNotMatch failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestNotMatch failed")
	assert.Nil(t, value, "TestNotMatch failed")
	assert.True(t, ok, "TestNotMatch failed")
}

func TestLogicAnd(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "abc"
	ctx.kvs["key2"] = "123"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`({str:kv.key1} !~ "^[0-9]*$") && ({str:kv.key2} == 123) && ({str:kv.key2} ~ "^[\d]*$")`),
	})
	assert.NoError(t, err, "TestLogicAnd failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLogicAnd failed")
	assert.Nil(t, value, "TestLogicAnd failed")
	assert.True(t, ok, "TestLogicAnd failed")
}

func TestLogicOr(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "abc"
	ctx.kvs["key2"] = "123"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`({str:kv.key1} ~ "^[0-9]*$") || ({str:kv.key2} != 123) || ({str:kv.key2} ~ "^[\d]*$")`),
	})
	assert.NoError(t, err, "TestLogicOr failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLogicOr failed")
	assert.Nil(t, value, "TestLogicOr failed")
	assert.True(t, ok, "TestLogicOr failed")
}

func TestLogicAndWithBitmap(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2, 3)))
	ctx.kvs["key2"] = string(util.MustMarshalBM(roaring.BitmapOf(3, 4, 5)))

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{bm:kv.key1} && {bm:kv.key2}`),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestLogicAndWithBitmap failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLogicAndWithBitmap failed")
	assert.NotNil(t, value, "TestLogicAndWithBitmap failed")
	assert.True(t, ok, "TestLogicAndWithBitmap failed")
	assert.Equal(t, uint64(1), value.(*roaring.Bitmap).GetCardinality(), "TestLogicAndWithBitmap failed")
}

func TestLogicOrWithBitmap(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2, 3)))
	ctx.kvs["key2"] = string(util.MustMarshalBM(roaring.BitmapOf(3, 4, 5)))

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{bm:kv.key1} || {bm:kv.key2}`),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestLogicOrWithBitmap failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestLogicOrWithBitmap failed")
	assert.NotNil(t, value, "TestLogicOrWithBitmap failed")
	assert.True(t, ok, "TestLogicOrWithBitmap failed")
	assert.Equal(t, uint64(5), value.(*roaring.Bitmap).GetCardinality(), "TestLogicOrWithBitmap failed")
}

func TestAndnot(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2, 3)))
	ctx.kvs["key2"] = string(util.MustMarshalBM(roaring.BitmapOf(3, 4, 5)))

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{bm:kv.key1} !&& {bm:kv.key2}`),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestAndnot failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestAndnot failed")
	assert.NotNil(t, value, "TestAndnot failed")
	assert.True(t, ok, "TestAndnot failed")
	assert.Equal(t, uint64(2), value.(*roaring.Bitmap).GetCardinality(), "TestAndnot failed")
}

func TestXor(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = string(util.MustMarshalBM(roaring.BitmapOf(1, 2, 3)))
	ctx.kvs["key2"] = string(util.MustMarshalBM(roaring.BitmapOf(3, 4, 5)))

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{bm:kv.key1} ^| {bm:kv.key2}`),
		Type:  metapb.BMResult,
	})
	assert.NoError(t, err, "TestXor failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestXor failed")
	assert.NotNil(t, value, "TestXor failed")
	assert.True(t, ok, "TestXor failed")
	assert.Equal(t, uint64(4), value.(*roaring.Bitmap).GetCardinality(), "TestXor failed")
}

func TestFuncVar(t *testing.T) {
	ctx := newTestCtx()

	now := time.Now()
	year := now.Year()
	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(fmt.Sprintf("{func.year}==%d", year)),
	})
	assert.NoError(t, err, "TestFuncVar failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestFuncVar failed")
	assert.Nil(t, value, "TestFuncVar failed")
	assert.True(t, ok, "TestFuncVar failed")

	month := now.Month()
	rt, err = NewRuntime(metapb.Expr{
		Value: []byte(fmt.Sprintf("{func.month}==%d", month)),
	})
	assert.NoError(t, err, "TestFuncVar failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestFuncVar failed")
	assert.Nil(t, value, "TestFuncVar failed")
	assert.True(t, ok, "TestFuncVar failed")

	day := now.Day()
	rt, err = NewRuntime(metapb.Expr{
		Value: []byte(fmt.Sprintf("{func.day}==%d", day)),
	})
	assert.NoError(t, err, "TestFuncVar failed")

	ok, value, err = rt.Exec(ctx)
	assert.NoError(t, err, "TestFuncVar failed")
	assert.Nil(t, value, "TestFuncVar failed")
	assert.True(t, ok, "TestFuncVar failed")
}

func TestDynamicVar(t *testing.T) {
	now := time.Now()
	ctx := newTestCtx()
	ctx.kvs[fmt.Sprintf("cur_year_%d", now.Year())] = "123"
	ctx.kvs[fmt.Sprintf("cur_month_%d", now.Month())] = "456"
	ctx.kvs[fmt.Sprintf("cur_day_%d", now.Day())] = "789"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("({dyna.cur_year_%d.year}==123) && ({dyna.cur_month_%d.month}==456) && ({dyna.cur_day_%d.day}==789)"),
	})
	assert.NoError(t, err, "TestDynamicVar failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestDynamicVar failed")
	assert.Nil(t, value, "TestDynamicVar failed")
	assert.True(t, ok, "TestDynamicVar failed")
}

func TestDynamicVarWithEvent(t *testing.T) {
	ctx := newTestCtx()
	ctx.setEventKV("key1", "event-key1")
	ctx.kvs["prev_event-key1"] = "abc"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{dyna.prev_%s.event.key1} == abc"),
	})
	assert.NoError(t, err, "TestDynamicVarWithEvent failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestDynamicVarWithEvent failed")
	assert.Nil(t, value, "TestDynamicVarWithEvent failed")
	assert.True(t, ok, "TestDynamicVarWithEvent failed")
}

func TestDynamicVarWithProfile(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "profile"
	ctx.kvs["profile_profile"] = "abc"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{dyna.profile_%s.profile.key1} == abc"),
	})
	assert.NoError(t, err, "TestDynamicVarWithProfile failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestDynamicVarWithProfile failed")
	assert.Nil(t, value, "TestDynamicVarWithProfile failed")
	assert.True(t, ok, "TestDynamicVarWithProfile failed")
}

func TestDynamicVarWithKV(t *testing.T) {
	ctx := newTestCtx()
	ctx.kvs["key1"] = "profile"
	ctx.kvs["kv_profile"] = "abc"

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte("{dyna.kv_%s.kv.key1} == abc"),
	})
	assert.NoError(t, err, "TestDynamicVarWithKV failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestDynamicVarWithKV failed")
	assert.Nil(t, value, "TestDynamicVarWithKV failed")
	assert.True(t, ok, "TestDynamicVarWithKV failed")
}

func TestEmptyWithKV(t *testing.T) {
	ctx := newTestCtx()

	rt, err := NewRuntime(metapb.Expr{
		Value: []byte(`{kv.key1} == ""`),
	})
	assert.NoError(t, err, "TestEmptyWithKV failed")

	ok, value, err := rt.Exec(ctx)
	assert.NoError(t, err, "TestEmptyWithKV failed")
	assert.Nil(t, value, "TestEmptyWithKV failed")
	assert.True(t, ok, "TestEmptyWithKV failed")
}
