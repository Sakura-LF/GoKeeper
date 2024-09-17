package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 1.正常情况
	fmt.Println("----------一: 正常情况---------------")
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("Sakura"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(logRecord)
	t.Log(res1)
	assert.NotNil(t, res1)
	// 测试 logRecord 长度一定大于 5 字节
	//assert.Greater(t, n1, int64(5))
	assert.Equal(t, n1, int64(17))

	// 2.测试 value 为空
	fmt.Println("----------二: value 为空---------------")
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, logRecordHead2 := EncodeLogRecord(logRecord2)
	t.Log(res2)
	assert.NotNil(t, res2)
	// 测试 head 长度一定大于 5 字节
	assert.Greater(t, logRecordHead2, int64(5))

	// 3.测试 Type 为 DELETED
	fmt.Println("----------三: Type 为 DELETED ---------------")
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("Sakurasss"),
		Type:  LogRecordDeleted,
	}
	res3, logRecordHead3 := EncodeLogRecord(logRecord3)
	t.Log(res3)
	assert.NotNil(t, res3)
	// 测试 head 长度一定大于 5 字节
	assert.Greater(t, logRecordHead3, int64(5))
}

func TestDecodeLogRecordHead(t *testing.T) {
	// 1.正常情况
	fmt.Println("----------一: 正常情况---------------")
	// 使用上面解码测试的 Header
	headerBuf := []byte{16, 97, 221, 162, 0, 8, 12}
	header, size := DecodeLogRecordHead(headerBuf)
	t.Logf("%+v\n", header)
	t.Log(size)
	// 测试 header 不为 nil
	assert.NotNil(t, header)
	// 测试 header 头字节长度
	assert.Equal(t, int64(7), size)
	// 测试 crc 的值
	assert.Equal(t, uint32(2732417296), header.crc)
	// 测试 LogRecord 类型
	assert.Equal(t, LogRecordNormal, header.recordType)
	// 测试 keySize
	assert.Equal(t, uint32(4), header.keySize)
	// 测试 valueSize
	assert.Equal(t, uint32(6), header.valueSize)

	// 2. value 为空
	fmt.Println("----------二: value 为空---------------")
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	head2, size2 := DecodeLogRecordHead(headerBuf2)
	t.Logf("%+v\n", head2)
	t.Log(size2)
	assert.NotNil(t, head2)
	// 测试 header 头字节长度
	assert.Equal(t, int64(7), size2)
	// 测试 crc 的值
	assert.Equal(t, uint32(240712713), head2.crc)
	// 测试 LogRecord 类型
	assert.Equal(t, LogRecordNormal, head2.recordType)
	// 测试 keySize
	assert.Equal(t, uint32(4), head2.keySize)
	// 测试 valueSize
	assert.Equal(t, uint32(0), head2.valueSize)

	// 3. DELETED 为空
	fmt.Println("----------三: DELETED ---------------")
	headerBuf3 := []byte{136, 170, 11, 255, 1, 8, 18}
	head3, size3 := DecodeLogRecordHead(headerBuf3)
	t.Logf("%+v\n", head3)
	t.Log(size3)
	assert.NotNil(t, head3)
	// 测试 header 头字节长度
	assert.Equal(t, int64(7), size3)
	// 测试 crc 的值
	assert.Equal(t, uint32(4278954632), head3.crc)
	// 测试 LogRecord 类型
	assert.Equal(t, LogRecordDeleted, head3.recordType)
	// 测试 keySize
	assert.Equal(t, uint32(4), head3.keySize)
	// 测试 valueSize
	assert.Equal(t, uint32(9), head3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	// 正常情况
	fmt.Println("----------一: 正常情况---------------")
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("Sakura"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{16, 97, 221, 162, 0, 8, 12}
	crc := getLogRecordCRC(logRecord, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2732417296), crc)

	fmt.Println("----------二: 空 value 情况---------------")
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(logRecord2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	fmt.Println("----------三: DETETED ---------------")
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("Sakurasss"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{136, 170, 11, 255, 1, 8, 18}
	crc3 := getLogRecordCRC(logRecord3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(4278954632), crc3)
}
