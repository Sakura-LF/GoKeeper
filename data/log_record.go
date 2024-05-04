package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// 日志记录(Header)的结构:
// crc type keySize valueSize
// 4    1     5       5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫做日志,是因为数据文件中的数据是追加写入的,类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordHeader LogRecord 的头部信息
type LogRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存的索引,描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id,表示数据在哪个文件上
	Offset int64  // 表示数据在这个文件中的偏移量
}

// EncodeLogRecord 对 LogRecord 进行编码,返回字节数组以及长度
// crc校验值  type     keySize,     valueSize     key     value
//
//	4        1     变长(最大5)    变长(最大5)    变长       变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个直接存储 Type
	header[4] = byte(logRecord.Type)
	var index = 5

	// 之后存储 key 和 value 的长度信息
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// 计算logRecord的长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// 根据长度创建一个byte切片
	resultBytes := make([]byte, size)

	// 将 header 部分拷贝过来
	copy(resultBytes[:index], header[:index])
	// key 和 value 也拷贝过来
	copy(resultBytes[index:], logRecord.Key)
	copy(resultBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 的数据就行crc校验
	// 从索引 4 开始
	// 0 1 2 3 表示crc校验的知
	crc := crc32.ChecksumIEEE(resultBytes[4:])
	binary.LittleEndian.PutUint32(resultBytes, crc)

	return resultBytes, int64(size)
}

// DecodeLogRecordHead 对 LogRecord 进行解码，返回
func DecodeLogRecordHead(buf []byte) (*LogRecordHeader, int64) {
	return &LogRecordHeader{
		crc:        0,
		recordType: 0,
		keySize:    0,
		valueSize:  0,
	}, 0
}

// 获取LogRecordCRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
