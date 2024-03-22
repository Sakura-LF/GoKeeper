package index

import (
	"GoKeeper/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	// key = nil
	key1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 114514,
	})
	assert.True(t, key1)

	// key == word
	key2 := bt.Put([]byte("S"), &data.LogRecordPos{
		Fid:    2,
		Offset: 2000,
	})
	assert.True(t, key2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	// key = nil
	key1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 114514,
	})
	assert.True(t, key1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(114514), pos1.Offset)

	// key == word
	key2 := bt.Put([]byte("S"), &data.LogRecordPos{
		Fid:    2,
		Offset: 2000,
	})
	assert.True(t, key2)

	pos2 := bt.Get([]byte("S"))
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(2000), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	// key = nil
	key1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 114514,
	})
	assert.True(t, key1)

	del1 := bt.Delete(nil)
	assert.True(t, del1)

	// key == word
	key2 := bt.Put([]byte("S"), &data.LogRecordPos{
		Fid:    2,
		Offset: 2000,
	})
	assert.True(t, key2)

	del2 := bt.Delete([]byte("S"))
	assert.True(t, del2)
}
