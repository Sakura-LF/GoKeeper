package data

import (
	"GoKeeper/fio"
	"fmt"
	"io"
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

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 读取 Header 信息
	headerBuf, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}
	// 对Head进行解码:
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

	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return b, err
	}
	return b, err
}
