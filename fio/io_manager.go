package fio

// DataFilePerm 数据文件权限
const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO ReadOnly 标准文件IO
	StandardFIO FileIOType = iota
	// MemoryMapFIO  内存映射文件IO
	MemoryMapFIO
)

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

// NewIOManager 根据文件名创建IOManager
func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIO(filename)
	case MemoryMapFIO:
		return NewMMapIOManager(filename)
	default:
		panic("unsupported io type")
	}
}
