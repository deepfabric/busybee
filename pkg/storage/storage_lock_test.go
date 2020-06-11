package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	store, deferFunc := NewTestStorage(t, true)
	defer deferFunc()

	key := []byte("test-lock-key")
	expect1 := []byte("expect-1")
	expect2 := []byte("expect-2")

	ok, err := store.Lock(key, expect1, 1, time.Millisecond*200, true)
	assert.NoError(t, err, true, "TestLock failed")
	assert.True(t, ok, "TestLock failed")

	ok, err = store.Lock(key, expect2, 1, time.Millisecond*200, false)
	assert.NoError(t, err, true, "TestLock failed")
	assert.False(t, ok, "TestLock failed")

	time.Sleep(time.Second * 2)

	ok, err = store.Lock(key, expect1, 1, time.Millisecond*200, true)
	assert.NoError(t, err, true, "TestLock failed")
	assert.True(t, ok, "TestLock failed")

	ok, err = store.Lock(key, expect2, 1, time.Millisecond*200, false)
	assert.NoError(t, err, true, "TestLock failed")
	assert.False(t, ok, "TestLock failed")

	err = store.Unlock(key, expect1)
	assert.NoError(t, err, true, "TestLock failed")

	ok, err = store.Lock(key, expect2, 1, time.Millisecond*200, false)
	assert.NoError(t, err, true, "TestLock failed")
	assert.True(t, ok, "TestLock failed")
}
