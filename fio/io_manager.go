package fio

// DataFilePerm 数据文件权限
const DataFilePerm = 0644

// IOManager 抽象IO管理接口,可以接入不同的IO类型
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件Size
	Size() (int64, error)
}

func NewIOManager(filename string) (IOManager, error) {
	return NewFileIO(filename)
}
