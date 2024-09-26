package GoKeeper

var DefaultOptions = Options{
	//DirPath:      os.TempDir(),      // 系统临时目录
	DirPath:      "tmp/",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,             // 默认关闭每次操作进行同步
	IndexType:    Btree,
}

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写入数据后是否要对数据进行安全的持久化
	SyncWrites bool

	// 索引类型(Btree,ART....)
	IndexType IndexType
}

// IteratorOption 索引迭代器的配置项
type IteratorOption struct {
	// 遍历前缀为指定值的 Key,默认为空
	Prefix []byte

	// 是否反向迭代
	// 默认正向迭代
	Reverse bool
}

var DefaultIteratorOption = IteratorOption{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchSize uint
	// 每一次事务提交时是否持久化
	SyncWrites bool
}

// DefaultWriteBatchOptions 默认配置
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 1000000,
	SyncWrites:   true,
}

// IndexType 索引类型
// 数据库默认是 Btree
// 还可以自己实现跳表，自适应基数树索引
type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)
