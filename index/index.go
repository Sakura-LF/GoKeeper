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
