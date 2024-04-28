package fio

import (
	"os"
)

// FileIo 对标准文件系统的封装
type FileIo struct {
	fd *os.File
}

// NewFileIO 初始化标准文件IO
func NewFileIO(filename string) (*FileIo, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &FileIo{fd: file}, nil
}

func (f *FileIo) Read(bytes []byte, i int64) (int, error) {
	return f.fd.ReadAt(bytes, i)
}

func (f *FileIo) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f *FileIo) Sync() error {
	return f.fd.Sync()
}

func (f *FileIo) Close() error {
	return f.fd.Close()
}
