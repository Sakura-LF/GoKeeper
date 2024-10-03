package GoKeeper

import (
	"GoKeeper/util"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		for _, file := range db.olderFiles {
			if file != nil {
				_ = file.Close()
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
	//dir, _ := os.TempDir()
	//opts.DirPath = dir
	db, err := Open(opts)
	defer func(db *DB) {
		err = db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	t.Log(db.options.DirPath)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, err := os.MkdirTemp("./tmp/", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
	val2, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	assert.NotEqual(t, val1, val2) // 确保值不相同

	// 3. key 为空
	err = db.Put(nil, util.GetRandomValue(10))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4. value 为空
	err = db.Put(util.GetRandomKey(10), nil)
	assert.Nil(t, err)
	val3, err := db.Get(util.GetRandomKey(10))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5. 写到数据文件进行转换
	for i := 0; i < 1000000; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(128))
		assert.Nil(t, err)
	}
	// 确保数据文件被转换, 旧数据文件数量大于2
	assert.GreaterOrEqual(t, len(db.olderFiles), 2)

	// 6.重启后再次 Put 一条数据
	if db.activeFile != nil {
		_ = db.activeFile.Close()
	}
	for _, of := range db.olderFiles {
		if of != nil {
			_ = of.Close()
		}
	}
	// 重启数据库
	db2, err := Open(opts)
	//defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := util.GetRandomValue(128)
	err = db2.Put(util.GetRandomKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(util.GetRandomKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 正常读取一条数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
	val, err := db.Get(util.GetRandomKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	// 2. 读取一个不存在的 Key
	val2, err := db.Get(util.GetRandomKey(111))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复put后读取
	err = db.Put(util.GetRandomKey(22), util.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Put(util.GetRandomKey(22), util.GetRandomValue(10))
	assert.Nil(t, err)

	val3, err := db.Get(util.GetRandomKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4. 值被删除后 再次 Get
	err = db.Put(util.GetRandomKey(100), util.GetRandomValue(10))
	assert.Nil(t, err)
	err = db.Delete(util.GetRandomKey(100))
	assert.Nil(t, err)
	val4, err := db.Get(util.GetRandomKey(100))
	assert.Equal(t, 0, len(val4))
	//assert.Equal(t, ErrKeyNotFound, err)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// 5.转换为旧的数据文件,从旧的数据文件上获取 value
	for i := 0; i < 1000000; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(128))
		assert.Nil(t, err)
	}
	assert.GreaterOrEqual(t, len(db.olderFiles), 2)
	val5, err := db.Get(util.GetRandomKey(1000))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6. 重启后前面写入的数据都能拿到
	if db.activeFile != nil {
		_ = db.activeFile.Close()
	}
	for _, of := range db.olderFiles {
		if of != nil {
			_ = of.Close()
		}
	}
	// 重启数据库
	//db2, err := Open(opts)
	////defer destroyDB(db2)
	//val6, err := db.Get(util.GetRandomKey(1))
	//val7, err := db.Get(util.GetRandomKey(2))
	//val8, err := db.Get(util.GetRandomKey(3))
	//
	//assert.Nil(t, err)
	//assert.Equal(t, val4, val5)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(util.GetRandomKey(11), util.GetRandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(util.GetRandomKey(11))
	assert.Nil(t, err)
	_, err = db.Get(util.GetRandomKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(util.GetRandomKey(22), util.GetRandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(util.GetRandomKey(22))
	assert.Nil(t, err)
	err = db.Put(util.GetRandomKey(22), util.GetRandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetRandomKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)
	// 重启之后
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// 只有一条数据
	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(128))
	keys2 := db.ListKeys()
	for _, value := range keys2 {
		fmt.Println(string(value))
	}
	assert.Equal(t, 1, len(keys2))

	// 多条数据
	for i := 0; i < 10; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(128))
		assert.Nil(t, err)
	}

	keys3 := db.ListKeys()
	assert.Equal(t, 10, len(keys3))
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写10条数据
	for i := 0; i < 10; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(10))
		assert.Nil(t, err)
	}

	err = db.Fold(func(key []byte, value []byte) bool {
		//t.Log("Key:", string(key), " Value:", string(value))
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetRandomKey(1), util.GetRandomValue(10))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	db2, err := Open(opts)
	t.Log(db2)
	t.Log(err)
	assert.Nil(t, db2)
	assert.ErrorIs(t, err, ErrDatabaseIsUsing)
}

func TestDB_FileLock2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./tmp/", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 关闭之后再打开
	db.Close()

	db2, err := Open(opts)
	defer db2.Close()

	assert.NotNil(t, db2)
	assert.Nil(t, err)
}

// 测试开始 mmap 和 非 mmap 时数据库启动的耗时
func TestDB_Open2(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/goKeeper-open"
	opts.MMapStartup = true
	now := time.Now()
	db, err := Open(opts)
	t.Log("open time:", time.Since(now))
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Put 数据
	for i := 0; i < 1000000; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(128))
		assert.Nil(t, err)
	}
}

// 测试数据库的累计失效数据量是否正常
func TestOpen2(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/goKeeper-open"
	opts.MMapStartup = true
	db, err := Open(opts)
	defer destroyDB(db)
	defer db.Close()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//写100 条数据
	for i := 100; i < 10000; i++ {
		err = db.Put(util.GetRandomKey(i), util.GetRandomValue(128))
		assert.Nil(t, err)
	}
	stat := db.Stat()
	// 1. 测试 keyNum 数量
	assert.Equal(t, uint(9900), stat.KeyNum)

	// 删除 100 条数据
	//for i := 100; i < 10000; i++ {
	//	err = db.Delete(util.GetRandomKey(i))
	//	assert.Nil(t, err)
	//}
	//err = db.Put([]byte("key"), []byte("value"))
	//assert.Nil(t, err)
	//db.Delete([]byte("key"))
	stat = db.Stat()
	t.Logf("%+v", stat)

}
