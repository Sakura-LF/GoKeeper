package GoKeeper

import (
	"GoKeeper/data"
	"GoKeeper/index"
	"sync"
)

// DB bitcask 存储引擎实现
type DB struct {
	// 用户配置选项
	options Options

	// 当前活跃数据文件,可以用于写入
	activeFile *data.DataFile

	// 旧的数据文件,只能用于读
	olderFiles map[uint32]*data.DataFile

	// 内存索引
	index index.Index
	mu    *sync.RWMutex
}

// Put 写入key/value数据,key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件, id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileID == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrDataCountDeleted
	}

	return logRecord.Value, nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在,因为数据库在没有写入的时候是没有文件生成的
	// 活跃文件为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDateFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据编码
	encodeRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入数据已经到达了活跃文件的阈值,则关闭活跃文件,并打开新文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件,保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDateFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodeRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDateFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileID + 1
	}
	// 打开最新的数据文件
	file, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = file
	return nil
}
