package GoKeeper

import (
	"GoKeeper/util"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		for _, of := range db.olderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	fmt.Println("v1:", string(val1))

	// 2.重复 Put key 相同的数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
	val2, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	assert.NotEqual(t, val1, val2) // 确保值不相同
}
