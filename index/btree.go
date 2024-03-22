package index

import (
	"GoKeeper/data"
	"github.com/google/btree"
	"sync"
)

// BTree http://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	// 因为BTree 写不是并发安全的,读是并发安全的
	mu sync.Mutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
	}
}

// Put 写入数据
// 因为 BTree insert 需要一个Item接口,所以需要自己定义Item结构体
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	if item := bt.tree.Get(it); item == nil {
		return nil
	} else {
		return item.(*Item).pos
	}
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	if item := bt.tree.Delete(it); item == nil {
		return false
	}
	return true
}
