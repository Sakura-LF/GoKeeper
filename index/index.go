package index

import (
	"GoKeeper/data"
	"bytes"
	"github.com/google/btree"
)

// Index 抽象索引接口,后续增加对其他数据机构的支持
type Index interface {

	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 kye 去除对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool

	// Size 返回索引中存储的键值对数量
	Size() int

	// Iterator 返回迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

type IndexType = int8

// 可以实现多种数据结构的索引
const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树
	ART

	// BPTree B+Tree 索引
	BPTree
)

func NewIndexer(indexType IndexType, dirPath string, sync bool) Index {
	switch indexType {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
	return nil
}

// Item 因为BTree insert,get,delete需要Item,所以自己定义一个Item
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 自定义比较的规则
func (i Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

// Iterator  通用的所有迭代器的接口
type Iterator interface {
	// Rewind  重新回到迭代器的起点,即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于(或小于)等于的目标 key, 根据从这key开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效,即是否已经便利完了所有的 key,用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器,释放相应资源
	Close()
}
