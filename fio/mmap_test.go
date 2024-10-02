package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("../tmp", "mmap-a.data")

	mmapIO, err := NewMMapIOManager(path)
	//defer destroyFile(path)

	assert.NotNil(t, mmapIO)

	// 1. 文件为空
	b1 := make([]byte, 10)
	read, err := mmapIO.Read(b1, 0)
	assert.ErrorIs(t, io.EOF, err)
	assert.Equal(t, 0, read)

	// 2. 追加数据,再读取
	// 使用标准文件IO 追加数据
	fileIO, err := NewFileIO(path)
	assert.Nil(t, err)
	write, err := fileIO.Write([]byte("hello world"))
	assert.Nil(t, err)
	assert.Equal(t, 11, write)

	// 重新载 mmap
	mmapIO, err = NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equalf(t, int64(11), size, "mmap size should be 11")

	i, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 10, i)
	assert.Equal(t, "hello worl", string(b1))
}
