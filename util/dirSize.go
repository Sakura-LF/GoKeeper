package util

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

// CopyDir 拷贝数据目录
// src 数据目录
// dst 目标目录
// exclude 排除文件
func CopyDir(src, dst string, exclude []string) error {
	// 如果目标文件夹不存在，则创建
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err = os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	// 循环遍历源路径,
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}
		for _, file := range exclude {
			matched, err := filepath.Match(file, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
	})
}
