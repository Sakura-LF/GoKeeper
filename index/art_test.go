package index

import (
	"GoKeeper/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	assert.NotNil(t, art)
	art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("abd"), &data.LogRecordPos{Fid: 2, Offset: 12})
	art.Put([]byte("ab"), &data.LogRecordPos{Fid: 3, Offset: 12})

}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	put := art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, put)

	// 1.get存在的数据
	val := art.Get([]byte("abc"))
	assert.NotNil(t, val)
	assert.Equal(t, uint32(1), val.Fid)
	assert.Equal(t, int64(12), val.Offset)

	// 2.get 不存在的数据
	val = art.Get([]byte("abcd"))
	assert.Nil(t, val)

	// 3.重复put数据,查看值是否更新
	put = art.Put([]byte("abc"), &data.LogRecordPos{Fid: 2, Offset: 13})
	assert.True(t, put)
	val = art.Get([]byte("abc"))
	assert.NotNil(t, val)
	assert.Equal(t, uint32(2), val.Fid)
	assert.Equal(t, int64(13), val.Offset)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	assert.NotNil(t, art)
	// 1.删除一个不存在的 key
	deleted := art.Delete([]byte("abc"))
	assert.False(t, deleted)

	// 2.删除一个存在的 key
	put := art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, put)
	deleted = art.Delete([]byte("abc"))
	assert.True(t, deleted)
	assert.Nil(t, art.Get([]byte("abc")))

	// 3.删除一个存在的 key, 再次删除
	put = art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.NotNil(t, art)

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("abd"), &data.LogRecordPos{Fid: 2, Offset: 12})

	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	assert.NotNil(t, art)

	art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("abd"), &data.LogRecordPos{Fid: 2, Offset: 22})
	art.Put([]byte("ab"), &data.LogRecordPos{Fid: 3, Offset: 32})
	art.Put([]byte("abcd"), &data.LogRecordPos{Fid: 4, Offset: 42})
	iterator := art.Iterator(true)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log(
			"Key:",
			string(iterator.Key()),
			"Value:",
			iterator.Value(),
		)
	}
}
