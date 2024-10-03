package GoKeeper

import (
	"GoKeeper/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// 辨别非事务的操作
const nonTransactionKey uint64 = 0

// 暂存事务完成标识的 key
var txnFinkey = []byte("txn-fin")

// WriteBatch 原子批量写数据,保证原子性
type WriteBatch struct {
	// 数据库对象
	db *DB

	// 缓存数据
	pendingWrites map[string]*data.LogRecord

	// 配置项
	options WriteBatchOptions

	lock *sync.Mutex
}

// NewWriteBatch 创建一个 WriteBatch
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	// 只要当索引类型是B+Tree,并且存储序列号不存在,并且不是第一次加载
	// 就禁用当前的 WriteBatch功能
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot user write batch, seq no file not exists")
	}
	return &WriteBatch{
		db:            db,
		options:       options,
		lock:          new(sync.Mutex),
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 写入数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.lock.Lock()
	defer wb.lock.Unlock()
	// 构建 LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Type:  data.LogRecordNormal,
		Value: value,
	}
	// 暂存到内存中
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.lock.Lock()
	defer wb.lock.Unlock()
	// 数据不存在直接返回
	if pos := wb.db.index.Get(key); pos == nil {
		// 删除暂存区数据
		// 因为有可能数据还在暂存区中,而没有 commit
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 构建 LogRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	// 暂存到内存中
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务
func (wb *WriteBatch) Commit() error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	// 暂存区没有数据,直接返回
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	// 检查是否超过最大批量写入数量
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
		return ErrExceedMaxBatchNum
	}
	// 加锁保证事务的串行化
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.transactionSeq, 1)

	// 开始写数据到数据文件中
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		// 写入数据到数据文件中
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = pos
	}
	// 写一条标识数据完成的数据
	finish := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinkey, seqNo),
		Type: data.LogRecordFinished,
	}
	if _, err := wb.db.appendLogRecord(finish); err != nil {
		return err
	}

	// 根据配置决定是否进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		// 判断数据类型是否为删除, 如果是删除则删除索引
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size) // 删除数据这条记录的大小,也是需要记录的
		}
	}

	// 清空暂存区
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 将 SeqNum 转换为字节流
// TODO 感觉还可以优化
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	resultKey := make([]byte, n+len(key))
	// 先拷贝序列号, 再拷贝 key
	// key: 序列化+key
	copy(resultKey[:n], seq[:n])
	copy(resultKey[n:], key)

	return resultKey
}

// 解析 LogRecord 中的 key 和序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, i := binary.Uvarint(key)
	return key[i:], seqNo
}
