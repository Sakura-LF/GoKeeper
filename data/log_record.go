package data

// LogRecordPos 数据内存所索引,描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id,表示数据在哪个文件上
	Offset int64  // 表示数据在这个文件中的偏移量
}
