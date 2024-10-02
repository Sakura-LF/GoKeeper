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
	ErrMergeIsRunning         = errors.New("merge is running, try again later")
	ErrDatabaseIsUsing        = errors.New("database is using by another process")
)

//type Error interface {
//	error
//}
//
//type OptionErr struct {
//	DirPath string
//}
//
//func (e *OptionErr) Error() string {
//
//}

//func Test() {
//	errors.New()
//}
