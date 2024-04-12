package data

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录
// 之所以叫做日志,是因为数据文件中的数据是追加写入的,类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存所索引,描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id,表示数据在哪个文件上
	Offset int64  // 表示数据在这个文件中的偏移量
}

// EncodeLogRecord 对 LogRecord 进行编码,返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
