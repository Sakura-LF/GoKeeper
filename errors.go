package GoKeeper

import (
	"errors"
)

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataCountDeleted       = errors.New("data has deleted")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
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
