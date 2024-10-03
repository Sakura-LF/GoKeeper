package GoKeeper

import (
	"errors"
)

// 定义 DataErr 类型作为 error 接口的别名

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataCountDeleted       = errors.New("data has deleted")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch num")
	ErrDatabaseIsUsing        = errors.New("database is using by another process")
)

// Merge Error
var (
	ErrMergeIsRunning          = errors.New("merge is running, try again later")
	ErrMergeNotExceedThreshold = errors.New("the amount of data does not exceed the threshold")
	ErrDiskSpaceNotEnough      = errors.New("disk space is not enough")
)
