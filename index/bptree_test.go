package index

import (
	"GoKeeper/data"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	path := filepath.Join("../tmp")
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			log.Println(err)
			return
		}
	}()
	tree := NewBPlusTree(path, false)
	assert.NotNil(t, tree)

	put := tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, put)
	put = tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 2, Offset: 20})
	assert.True(t, put)
	put = tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 3, Offset: 30})
	assert.True(t, put)
	tree.tree.Close()
}

func TestNewBPTree_Get(t *testing.T) {
	path := filepath.Join("../tmp")
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			log.Println(err)
			return
		}
	}()
	tree := NewBPlusTree(path, false)
	assert.NotNil(t, tree)

	// 一: Get 一个不存在的 key
	value := tree.Get([]byte("sdfa"))
	assert.Nil(t, value)

	// 二: Get 一个存在的 key
	put := tree.Put([]byte("1"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, put)
	value = tree.Get([]byte("1"))
	assert.NotNil(t, value)
	assert.Equal(t, uint32(1), value.Fid)
	assert.Equal(t, int64(10), value.Offset)

	// 三: 重复 Put 再 Get
	put = tree.Put([]byte("1"), &data.LogRecordPos{Fid: 2, Offset: 20})
	assert.True(t, put)
	value = tree.Get([]byte("1"))
	assert.NotNil(t, value)
	assert.Equal(t, uint32(2), value.Fid)
	assert.Equal(t, int64(20), value.Offset)
}

func TestNewBPTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			log.Println(err)
			return
		}
	}()
	tree := NewBPlusTree(path, false)
	assert.NotNil(t, tree)
	//assert.Equal(t, 0, tree.Size())

	// 1. 删除一个不存在的 key
	ok := tree.Delete([]byte("saf"))
	assert.False(t, ok)

	// 2. 删除一个存在的 key
	tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 10})
	ok = tree.Delete([]byte("hello"))
	assert.True(t, ok)
	value := tree.Get([]byte("hello"))
	assert.Nil(t, value)
	//assert.Nil(t, err)

	tree.tree.Close()
}

func TestNewBPTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			log.Println(err)
			return
		}
	}()
	tree := NewBPlusTree(path, false)
	assert.NotNil(t, tree)
	// 1. 初始化时，tree 的 size 为 0
	assert.Equal(t, 0, tree.Size())

	tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Equal(t, 1, tree.Size())
	tree.Put([]byte("hello2"), &data.LogRecordPos{Fid: 2, Offset: 20})
	assert.Equal(t, 2, tree.Size())

	tree.tree.Close()
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join("../tmp")
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			log.Println(err)
			return
		}
	}()
	tree := NewBPlusTree(path, false)
	assert.NotNil(t, tree)

	tree.Put([]byte("hello1"), &data.LogRecordPos{Fid: 1, Offset: 10})
	tree.Put([]byte("hello2"), &data.LogRecordPos{Fid: 2, Offset: 20})
	tree.Put([]byte("hello3"), &data.LogRecordPos{Fid: 3, Offset: 30})

	iter := tree.Iterator(true)
	assert.NotNil(t, iter)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		log.Println("key:", string(iter.Key()), "value:", iter.Value())
	}

	iter.Close()
}
