package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// 在测试结束后删除文件
func RemoveFile(filename string) {
	// RemoveAll 删除这个路径下的所有文件
	// Remove 删除单个文件
	if err := os.RemoveAll(filename); err != nil {
		panic(err)
	}
}

func TestNewFileIO(t *testing.T) {
	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)
	defer file.Close()

	assert.Nil(t, err)
	assert.NotNil(t, file)
}

func TestFileIo_Write(t *testing.T) {
	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)
	defer file.Close()

	assert.Nil(t, err)
	assert.NotNil(t, file)

	// 写文件
	n, err := file.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("Sakura"))
	assert.Equal(t, 6, n)
	assert.Nil(t, err)
}

func TestFileIo_Read(t *testing.T) {
	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)
	defer file.Close()

	assert.Nil(t, err)
	assert.NotNil(t, file)

	// 写文件
	n, err := file.Write([]byte("LF"))
	assert.Equal(t, 2, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("Sakura"))
	assert.Equal(t, 6, n)
	assert.Nil(t, err)

	// 读文件
	b1 := make([]byte, 2)
	read, err := file.Read(b1, 0)
	assert.Equal(t, 2, read)
	assert.Equal(t, []byte("LF"), b1)
}

func TestFileIo_Sync(t *testing.T) {
	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)
	defer file.Close()

	err = file.Sync()
	assert.Nil(t, err)
}

func TestFileIo_Close(t *testing.T) {

	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)

	err = file.Close()
	assert.Nil(t, err)
}
