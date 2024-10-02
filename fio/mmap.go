package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO, 内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (mmap *MMap) Read(bytes []byte, i int64) (int, error) {
	return mmap.readerAt.ReadAt(bytes, i)
}

func (mmap *MMap) Write(bytes []byte) (int, error) {
	panic("not implement")
}

func (mmap *MMap) Sync() error {
	panic("not implement")
}

func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
