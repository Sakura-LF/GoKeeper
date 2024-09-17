package data

import (
	"GoKeeper/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var (
	ErrInvalidCRC = errors.New("invalid crc")
)

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileID    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := fmt.Sprintf("%s%09d%s", dirPath, fileId, DataFileNameSuffix)
	fmt.Println("FileName:", fileName)

	// 初始化 IOManager管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, err
}

// ReadLogRecord 读取日志记录
// 返回日志记录,日志记录长度,error
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取文件大小
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// 对 Head 进行解码:
	// 返回解码后的 Header 和 HeaderSize
	header, headerSize := DecodeLogRecordHead(headerBuf)
	// 这两个条件标识读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}
	// 读取用户实际存储的 key/value长度
	if keySize > 0 || valueSize > 0 {
		kvbuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解出key和value
		logRecord.Key = kvbuf[:keySize]
		logRecord.Value = kvbuf[keySize:]
	}

	// 校验数据的有效性
	// 与存储在数据文件中的 CRC 进行比较
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	_, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return b, err
	}
	return b, err
}
