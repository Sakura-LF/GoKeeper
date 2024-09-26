package GoKeeper

import (
	"GoKeeper/util"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	// 一:写数据后未进行提交
	err = wb.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(util.GetRandomKey(2))
	assert.Nil(t, err)

	_, err = db.Get(util.GetRandomKey(1))
	assert.ErrorIs(t, ErrKeyNotFound, err)

	// 二:提交数据后,读取数据
	err = wb.Commit()
	assert.Nil(t, err)
	val2, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 三:删除数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb2.Delete(util.GetRandomKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	val, err := db.Get(util.GetRandomKey(1))
	assert.ErrorIs(t, ErrKeyNotFound, err)
	t.Log(val)
	assert.Nil(t, val)
}

func TestWriteBatch_Reboot(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "goKeeper-batch")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(util.GetRandomKey(2), util.GetRandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(util.GetRandomKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	// 测试事务序列号
	err = wb.Put(util.GetRandomKey(11), util.GetRandomValue(10))
	err = wb.Commit()
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), db.transactionSeq)

	// 重启数据库
	err = db.Close()
	assert.Nil(t, err)

	//_, err = Open(opts)
	//assert.Nil(t, err)
	////
	////val, err := db2.Get(util.GetRandomKey(1))
	////t.Log(val)
	////assert.ErrorIs(t, ErrKeyNotFound, err)
	////assert.Nil(t, val)
}

func TestWriteBatch_Commit(t *testing.T) {
	opts := DefaultOptions
	//dir, _ := os.MkdirTemp("./tmp/", "goKeeper-batch")
	dir := "./tmp/goKeeper-batch"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	//opts2 := DefaultWriteBatchOptions
	//wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	//for i := 0; i < 500000; i++ {
	//	err = wb.Put(util.GetRandomKey(i), util.GetRandomValue(10))
	//	assert.Nil(t, err)
	//}
	//err = wb.Commit()
	//assert.Nil(t, err)
	//
	err = db.Close()
	//assert.Nil(t, err)
}
