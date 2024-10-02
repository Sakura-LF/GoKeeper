package data

import (
	"GoKeeper/fio"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataFile_Open(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	//t.Log(os.TempDir())
	file2, err := OpenDataFile("../tmp/", 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	file3, err := OpenDataFile("../tmp/", 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}

func TestDataFile_Write(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("Sakura"))
	assert.Nil(t, err)

	err = file.Write([]byte("LF"))
	assert.Nil(t, err)

	err = file.Write([]byte("A2"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 123, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("Sakura"))
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 456, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("AAA"))
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 789, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	//defer file.Close()

	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("sakura"),
		Type:  LogRecordNormal,
	}
	bytesRecord, size1 := EncodeLogRecord(record1)
	err = file.Write(bytesRecord)
	assert.Nil(t, err)

	readRecord, readSize, err := file.ReadLogRecord(0)
	assert.Nil(t, err)
	// 判断读取的数据是否正确
	assert.Equal(t, readSize, size1)
	assert.Equal(t, readRecord.Key, record1.Key)
	assert.Equal(t, readRecord.Value, record1.Value)
	assert.Equal(t, readRecord.Type, record1.Type)

	// 多条 LogRecord
	record2 := &LogRecord{
		Key:   []byte("age"),
		Value: []byte("18"),
		Type:  LogRecordNormal,
	}
	bytesRecord2, size2 := EncodeLogRecord(record2)
	err = file.Write(bytesRecord2)
	assert.Nil(t, err)

	// 从上一条日志记录的位置开始读取
	readRecord2, readSize2, err := file.ReadLogRecord(readSize)
	assert.Nil(t, err)
	assert.Equal(t, readSize2, size2)
	assert.Equal(t, readRecord2.Key, record2.Key)
	assert.Equal(t, readRecord2.Value, record2.Value)
	assert.Equal(t, readRecord2.Type, record2.Type)

	// 被删除的数据在数据文件中的末尾
	record3 := &LogRecord{
		Key:   []byte("test"),
		Value: []byte("tt"),
		Type:  LogRecordDeleted,
	}
	bytesRecord3, size3 := EncodeLogRecord(record3)
	err = file.Write(bytesRecord3)
	assert.Nil(t, err)

	// 读取被删除的数据
	readRecord3, readSize3, err := file.ReadLogRecord(readSize + readSize2)
	assert.Nil(t, err)
	assert.Equal(t, readSize3, size3)
	assert.Equal(t, readRecord3.Key, record3.Key)
	assert.Equal(t, readRecord3.Value, record3.Value)
	assert.Equal(t, readRecord3.Type, record3.Type)
}
