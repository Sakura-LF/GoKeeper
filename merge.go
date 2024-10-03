package GoKeeper

import (
	"GoKeeper/data"
	"GoKeeper/util"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeSuffixName   = "-merge"
	mergerFinishedKey = "merge.finished"
)

// Merge 清理无效数据生成 Hint 文件
func (db *DB) Merge() error {
	// 活跃文件为空,直接返回
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	if db.isMerging {
		// 正在 merge 中
		// 释放锁
		db.lock.Unlock()
		return ErrMergeIsRunning
	}

	// 查看可以 merge 的数据量是否达到了阈值
	size, err := util.DirSize(db.options.DirPath)
	if err != nil {
		db.lock.Unlock()
		return err
	}
	if float32(size)/float32(db.options.DataFileSize) < db.options.MergeThreshold {
		// 数据量未达到阈值,直接返回
		db.lock.Unlock()
		return ErrMergeNotExceedThreshold
	}

	// 查看剩余空间容量是否可以容纳 merge 之后的数据量
	// todo 获取剩余磁盘空间的方法没法跨平台,暂时不是先
	//availableDiskSize, err := util.AvailableDiskSize()
	//if err != nil {
	//	db.lock.Unlock()
	//	return err
	//}
	//if uint64(size-db.reclaimSize) >= availableDiskSize {
	//	db.lock.Unlock()
	//	return ErrDiskSpaceNotEnough
	//}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 正式开始 merge 流程

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	// 将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		return err
	}
	// 记录最近没有参与 merge 的文件id
	nonMergeFileId := db.activeFile.FileID

	// 取出所有需要 merge 的文件
	mergeFiles := make([]*data.DataFile, 0, 10) // 预先分配空间
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 释放锁, 现在可以用户可以进行 Put,Get,Delete 操作
	db.lock.Unlock()

	// 将 merge 的文件从小到大进行排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	// 获取 mergePath
	mergePath := db.getMergePath()
	// 如果目录存在,说明发生过 merge, 删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建merge 目录
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时的 bitcask 实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	// 打开每次都 Sync, merge 速度会下降
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 打开 Hint 文件,存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历每个数据文件,读取每一条记录
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			record, n, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 Key
			realKey, _ := parseLogRecordKey(record.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存中的索引位置
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileID &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				record.Key = logRecordKeyWithSeq(realKey, nonTransactionKey)
				// 将数据重写到数据文件中
				pos, err := mergeDB.appendLogRecord(record)
				if err != nil {
					return err
				}
				// 将位置索引写到 hint 文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 移动到下一条记录
			offset += n
		}
	}

	// 持久化 hint 文件和数据文件
	if err = hintFile.Sync(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishFile, err := data.OpenFinishedFileName(mergePath)
	if err != nil {
		return err
	}
	mergeFinishRecord := &data.LogRecord{
		Key:   []byte(mergerFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encodeRecord, _ := data.EncodeLogRecord(mergeFinishRecord)
	if err = mergeFinishFile.Write(encodeRecord); err != nil {
		return err
	}

	if err = mergeFinishFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 拿到数据目录的路径
// eg. 数据目录 /tmp/goKeeper
//
//	merge目录 /tmp/gooKeeper-merge
func (db *DB) getMergePath() string {
	// 1.首先清理路径中的冗余部分
	// 2.其次获取数据目录的父级路径
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(dir)
	return filepath.Join(dir, base, mergeSuffixName)
}

// 加载 merge 目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// 判断目录是否存在,不存在则直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	// 读取目录中的所有文件
	dir, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找 merge 完成的文件,判断 merge 是否处理完毕
	var isMergeFinished bool
	mergeFileNames := make([]string, 0, 20)
	for _, entry := range dir {
		if entry.Name() == data.MergeFinishedFileName {
			isMergeFinished = true
		}
		// 没必要拿到事务序列号文件
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	// merge没有完成则直接返回
	// todo 这里之后可以优化在控制台打印数据,或将数据写入日志文件
	if !isMergeFinished {
		return nil
	}
	// merge 完成的处理
	// 将旧的数据文件删除,用merge目录下的文件替代

	// 获取没有参与 Merge 的文件ID
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除它文件id小的文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err = os.Stat(fileName); err == nil {
			if err = os.Remove(fileName); err != nil {
				return err
			}
		} else if os.IsNotExist(err) {
			return err
		}

	}

	// 将新的数据文件移动到数据目录
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err = os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

// 获取没有参与 Merge 的文件ID
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishFile, err := data.OpenFinishedFileName(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	// 打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	// 加载索引
	var offset int64 = 0
	for {
		record, n, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 拿到位置索引
		pos := data.DecodeLogRecordPos(record.Value)
		// 加入到内存索引中
		db.index.Put(record.Key, pos)
		// 移动到下一条记录
		offset += n
	}
	return nil
}
