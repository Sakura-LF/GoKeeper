package GoKeeper

import (
	"GoKeeper/util"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	// 传入配置文件,启动数据库
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 创建迭代器
	iterator := db.NewIterator(DefaultIteratorOption)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_OneValue(t *testing.T) {
	// 传入配置文件,启动数据库
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)

	// 创建迭代器
	iterator := db.NewIterator(DefaultIteratorOption)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, util.GetRandomKey(1), iterator.Key())
	value, err := iterator.Value()
	assert.Nil(t, err)
	t.Log(value)
}

func TestDB_Iterator_MutiValue(t *testing.T) {
	// 传入配置文件,启动数据库
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入10个数据
	for i := 0; i < 10; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(10))
		assert.Nil(t, err)
	}

	// 创建迭代器
	iterator := db.NewIterator(DefaultIteratorOption)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
	}
	iterator.Rewind()
	for iterator.Seek([]byte("GoKeeper-key-4")); iterator.Valid(); iterator.Next() {
		t.Log(string(iterator.Key()))
	}

	// 创建反向迭代
	iterator2 := db.NewIterator(IteratorOption{
		Reverse: true,
		Prefix:  nil,
	})

	for iterator2.Rewind(); iterator2.Valid(); iterator2.Next() {
		assert.NotNil(t, iterator2.Key())
	}
}

func TestDB_Iterator_Prefix(t *testing.T) {
	// 传入配置文件,启动数据库
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 插入10个数据
	db.Put([]byte("Sakurass"), util.GetRandomValue(10))
	db.Put([]byte("GoKeeper-key-1"), util.GetRandomValue(10))
	db.Put([]byte("Sakuras"), util.GetRandomValue(10))
	db.Put([]byte("Saki"), util.GetRandomValue(10))
	db.Put([]byte("GoKeeper-key-2"), util.GetRandomValue(10))

	// 创建无前缀的迭代器
	iterator := db.NewIterator(DefaultIteratorOption)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log(string(iterator.Key()))
	}
	fmt.Println("--------------")
	// 创建指定前缀的迭代器
	iterator = db.NewIterator(IteratorOption{
		Prefix:  []byte("GoKeeper"),
		Reverse: false,
	})
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log(string(iterator.Key()))
	}
}
