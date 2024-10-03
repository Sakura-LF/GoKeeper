package index

import (
	"GoKeeper/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree http://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	// 因为BTree 写不是并发安全的,读是并发安全的
	lock sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
	}
}

// Put 写入数据
// 因为 BTree insert 需要一个Item接口,所以需要自己定义Item结构体
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	if item := bt.tree.Get(it); item == nil {
		return nil
	} else {
		return item.(*Item).pos
	}
}

// Delete 删除修改的代码
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBTreeIterator(bt.tree, reverse)
}

// BTreeIterator  索引迭代器
type BTreeIterator struct {
	// 记录遍历到了哪个位置
	currIndex int

	// 是否反向遍历
	reverse bool

	// key + 位置索引信息
	values []*Item
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有数据存放到数组中
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if reverse {
		// [Last, First] 为 BTree 中的每个元素执行上面的逻辑
		tree.Descend(saveValues)
	} else {
		// [First, Last]
		tree.Ascend(saveValues)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重置迭代器
func (bti *BTreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的key 查找到第一个大于(或小于)等于的目标 key, 根据从这key开始遍历
func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *BTreeIterator) Next() {
	bti.currIndex++
}

// Valid 判断是否还有下一个 key
func (bti *BTreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 返回当前 key
func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 返回当前 key 对应的 value
func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器
func (bti *BTreeIterator) Close() {
	bti.values = nil
}
