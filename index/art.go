package index

import (
	"GoKeeper/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
// https://github.com/plar/go-adaptive-radix-tree?tab=readme-ov-file
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// BTreeIterator  索引迭代器
type artIterator struct {
	// 记录遍历到了哪个位置
	currIndex int

	// 是否反向遍历
	reverse bool

	// key + 位置索引信息
	values []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	// 将所有数据存放到数组中
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	// 传入回调函数
	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重置迭代器
func (arti *artIterator) Rewind() {
	arti.currIndex = 0
}

// Seek 根据传入的key 查找到第一个大于(或小于)等于的目标 key, 根据从这key开始遍历
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (arti *artIterator) Next() {
	arti.currIndex++
}

// Valid 判断是否还有下一个 key
func (arti *artIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

// Key 返回当前 key
func (arti *artIterator) Key() []byte {
	return arti.values[arti.currIndex].key
}

// Value 返回当前 key 对应的 value
func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

// Close 关闭迭代器
func (arti *artIterator) Close() {
	arti.values = nil
}
