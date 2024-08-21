package GoKeeper

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
