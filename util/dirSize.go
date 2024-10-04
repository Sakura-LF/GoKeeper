package util

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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
func CopyDir(src, dst string, excludes []string) error {
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err = os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}
	task := make(chan string, 100)

	wg := &sync.WaitGroup{}

	if err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if path == src {
			return nil
		}
		fileName := filepath.Base(path)

		// 排除文件
		for _, excludeFile := range excludes {
			if fileName == excludeFile {
				return nil
			}
		}

		// 向队列中添加文件名
		task <- fileName
		return nil
	}); err != nil {
		return err
	}

	for i := 0; i < len(task); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := Worker(src, dst, task)
			if err != nil {
				return
			}
		}()
	}

	wg.Wait()
	return nil
}

func Worker(src, dst string, task chan string) error {
	fileName := <-task

	srcFile, err := os.Open(filepath.Join(src, fileName))
	if err != nil {
		return err
	}
	defer func(srcFile *os.File) {
		err := srcFile.Close()
		if err != nil {
			return
		}
	}(srcFile)

	dstFile, err := os.Create(filepath.Join(dst, fileName))
	if err != nil {
		return err
	}
	defer func(dstFile *os.File) {
		err := dstFile.Close()
		if err != nil {
			return
		}
	}(dstFile)

	_, err = io.Copy(dstFile, srcFile)
	return err
}
