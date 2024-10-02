package GoKeeper

import (
	"GoKeeper/data"
	"GoKeeper/index"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实现
type DB struct {
	options         Options                   // 用户配置选项
	activeFile      *data.DataFile            // 当前活跃数据文件,可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件,只能用于读
	fileids         []int                     // 文件id,在加载索引的时候用
	index           index.Index               // 内存索引
	transactionSeq  uint64                    // 事务序列号, 全局递增
	isMerging       bool                      // 是否正在 merge
	seqNoFileExists bool                      // 存储事务序列号的文件是否存在
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              // 文件锁:确保多个进程之间的互斥
	byteWrite       uint                      // 表示数据库已经写入的字节数
	lock            *sync.RWMutex
}

// Open 启动数据库
// 流程:
// 1. 校验数据库配置
// 2. 加载数据目录中的文件
// 3. 遍历数据文件中的内容构建内存索引
func Open(options Options) (*DB, error) {
	// 对用户传入的配置进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在,如果目录不存在,则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		lock:       new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	// 加载 merge 数据目录
	if err = db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err = db.loadDataFile(); err != nil {
		return nil, err
	}

	// B+树不需要从数据文件中加载索引
	if options.IndexType != BPlusTree {
		//  从 hint 索引文件中加载 内存索引
		if err = db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		// 从数据文件中加载索引
		if err = db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}
	// 取出当前的事务序列号
	if options.IndexType == BPlusTree {
		if err = db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close 方法
func (db *DB) Close() error {
	defer func() {
		err := db.fileLock.Unlock()
		if err != nil {
			panic(fmt.Sprintln("failed to unlock the directory", err))
		}
	}()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	// 保存当前的事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	defer seqNoFile.Close()
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.transactionSeq, 10)),
	}
	logRecord, _ := data.EncodeLogRecord(record)
	if err = seqNoFile.Write(logRecord); err != nil {
		return err
	}
	if err = seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭活跃数据文件
	if err = db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err = file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 持久化数据文件到磁盘
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.activeFile.Sync()
}

// 加载数据文件
func (db *DB) loadDataFile() error {
	// 读取目录中的所有文件
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件,找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		// 判断文件是否以 .data 结尾
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
// todo db.fileids 可以不需要,直接传入 loadIndexFromDataFiles 方法也可以
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件,说明数据库是空的
	if len(db.fileids) == 0 {
		return nil
	}

	// 查看是否发生过 Merge
	isMerge, nonMergeFileId := false, uint32(0)
	mergerFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergerFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		isMerge = true
		nonMergeFileId = fid
	}

	updateMemoryIndex := func(key []byte, recordType data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if recordType == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index start")
		}
	}

	// 暂存事务数据
	tansactionRecord := make(map[uint64][]*data.TransactionRecord)
	//
	var currentSeq = nonTransactionKey

	// 遍历所有的文件id, 处理文件中的记录
	for i, fid := range db.fileids {
		var fileId = uint32(fid)

		// 如果比最近未参与 merge 的文件 id 更小,则说明已经从 Hint 文件中加载索引了
		if isMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		// 判断文件是否是活跃文件
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		// 偏移量
		var offset int64 = 0
		for {
			// 读取日志记录,返回的日志记录和记录大小
			// todo 重点理解这块儿
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 从 LogRecord 中获取 序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// 非事务提交的记录,直接更新内存索引
			if seqNo == nonTransactionKey {
				updateMemoryIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 如果是事务完成的记录
				// 更新内存索引
				if logRecord.Type == data.LogRecordFinished {
					for _, txRecord := range tansactionRecord[seqNo] {
						updateMemoryIndex(txRecord.Record.Key, txRecord.Record.Type, txRecord.Pos)
					}
					delete(tansactionRecord, seqNo)
				} else {
					// 如果不是事务完成的记录,暂存,知道读取到 事务完成的记录
					logRecord.Key = realKey
					tansactionRecord[seqNo] = append(tansactionRecord[seqNo], &data.TransactionRecord{
						Pos:    logRecordPos,
						Record: logRecord,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeq {
				currentSeq = seqNo
			}

			// 递增 offset,下一次从新的位置开始
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 Write0ff
		if i == len(db.fileids)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	// 更新事务序列号
	db.transactionSeq = currentSeq
	return nil
}

// Put 写入key/value数据,key不能为空
// 流程:
//  1. 首先判断 key 是否有效
//     1.1 如为空,返回自定义错误
//  2. 构造日志记录结构体 LogRecord
//  3. 将日志记录追加写入到当前文件中 appendLogRecord(LogRecord)
//  4. 更新内存索引(向内存索引执行Put)
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionKey),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		log.Println(err)
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
	db.lock.Lock()
	defer db.lock.Unlock()

	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	db.lock.RLock()
	defer db.lock.RUnlock()

	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	idx := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据,并且执行用户指定的操作, 函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// 遍历内存索引
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		// 根据value的位置信息,从数据文件中获取数据
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 根据位置信息获取数据
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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

// Delete 删除数据
func (db *DB) Delete(key []byte) error {
	// 检查key是否合法
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查key是否存在
	if pos := db.index.Get(key); pos == nil {
		return ErrKeyNotFound
	}

	// 构造 LogRecord, 标识类型是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionKey),
		Type: data.LogRecordDeleted,
	}
	// 写入到数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 从内存索引中删除对应的 Key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写数据到活跃文件中
// 流程:
//  1. 判断数据库活跃文件是否为空(数据库刚启动)
//     1.1 若为空初始化活跃文件 initActiveFile
//  2. 将要写入的数据进行编码
//     2.1 编码:将结构体编码为一串字节数组
//  3. 判断活跃文件+编码完成的长度是否大于数据库设定的存储阈值
//     2.1 若大于,保存活跃文件,然后打开新的活跃文件
//  4. 向活跃文件中写入内容 Write()
//  5. 返回内存索引
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在,因为数据库在没有写入的时候是没有文件生成的
	// 活跃文件为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
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
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodeRecord); err != nil {
		return nil, err
	}

	db.byteWrite += uint(size)
	// 根据用户配置决定是否每次写入持久化
	// 如果打开了每次写入持久化, 则根据写入字节的持久化策略就失效
	needSync := db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.byteWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.byteWrite > 0 {
			db.byteWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
	}
	return pos, nil
}

// setActiveDataFile 设置当前活跃文件
// 两种情况需要调用这个方法
//
//	1.数据库目录为空,没有活跃文件
//	2.活跃文件写满了,转换为旧的数据文件,重新开启一个活跃文件
//
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		// 活跃文件的 id 为数据库目前的活跃文件 id + 1
		initialFileId = db.activeFile.FileID + 1
	}
	// 打开最新的数据文件
	// 传入数据库配置中的路径,已经刚才初始化好的文件id
	file, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = file
	return nil
}

// checkOptions 检查DB的Options是否正确
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("db dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database datafileSize must >= 0")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.transactionSeq = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}
