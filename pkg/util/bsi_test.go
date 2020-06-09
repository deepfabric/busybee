package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBSIKV(t *testing.T) {
	b := NewBSI(64)
	b.Set(1, 100)
	b.Set(2, 200)

	v, ok := b.Get(1)
	assert.True(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(100), v, "TestBSIKV failed")

	v, ok = b.Get(2)
	assert.True(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(200), v, "TestBSIKV failed")

	v, ok = b.Get(3)
	assert.False(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(-1), v, "TestBSIKV failed")

	assert.NoError(t, b.Del(1), "TestBSIKV failed")
	v, ok = b.Get(1)
	assert.False(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(-1), v, "TestBSIKV failed")
}

func TestBSIMashal(t *testing.T) {
	b := NewBSI(64)
	b.Set(1, 100)
	b.Set(2, 200)

	data, err := b.Marshal()
	assert.NoError(t, err, "TestBSIMashal failed")

	b = NewBSI(64)
	assert.NoError(t, b.Unmarshal(data), "TestBSIMashal failed")

	v, ok := b.Get(1)
	assert.True(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(100), v, "TestBSIKV failed")

	v, ok = b.Get(2)
	assert.True(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(200), v, "TestBSIKV failed")

	v, ok = b.Get(3)
	assert.False(t, ok, "TestBSIKV failed")
	assert.Equal(t, int64(-1), v, "TestBSIKV failed")

}
