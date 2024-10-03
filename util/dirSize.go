package util

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// DirSize 获取目录大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘有效空间
func AvailableDiskSize() (uint64, error) {
	_, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	//var stat syscall.Statfs_t
	//
	//if err = syscall.Statfs(wd, &stat); err != nil {
	//	return 0, err
	//}

	//return stat.Bavail * uint64(stat.Bsize), nil
	return uint64(10000000), nil
}
