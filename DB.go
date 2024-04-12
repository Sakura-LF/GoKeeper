package GoKeeper

import (
	"GoKeeper/data"
	"GoKeeper/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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

	// 文件id,在加载索引的时候用
	fileids []int

	// 内存索引
	index index.Index
	mu    *sync.RWMutex
}

func Open(options Options) (*DB, error) {
	// 对用户传入的配置进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 如果目录不存在,则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModeDir); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件,找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据文件可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序,从小到大,一次加载
	sort.Ints(fileIds)
	db.fileids = fileIds

	// 遍历每个文件id,打开对应的数据文件
	for i, fid := range fileIds {
		datafile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = datafile
		} else {
			db.olderFiles[uint32(fid)] = datafile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录,并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件,说明数据库是空的
	if len(db.fileids) == 0 {
		return nil
	}

	// 遍历所有的文件id, 处理文件中的记录
	for i, fid := range db.fileids {
		var fileiId = uint32(fid)
		var dataFile *data.DataFile
		if fileiId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileiId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileiId,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增 offset,下一次从新的位置开始
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 Write0ff
		if i == len(db.fileids)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("db dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database datafile must >= 0")
	}
	return nil
}
