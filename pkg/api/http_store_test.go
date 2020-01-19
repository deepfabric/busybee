package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeAndDecodeUint32(t *testing.T) {
	value := []uint32{1, 2, 3}
	data := EncodeUint32Slice(value)
	assert.Equal(t, 4*len(value), len(data), "TestEncodeAndDecodeUint32 failed")
	value2, err := DecodeUint32Slice(data)
	assert.NoError(t, err, "TestEncodeAndDecodeUint32 failed")
	assert.Equal(t, value, value2, "TestEncodeAndDecodeUint32 failed")

}
