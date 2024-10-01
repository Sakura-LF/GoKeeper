package index

import (
	"GoKeeper/data"
	"go.etcd.io/bbolt"
	"log"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("goKeeper-index")

// BPlusTree B+ 树索引
// 对 go.etcd.io/bbolt 进行封装
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化 B+Tree 索引
func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		log.Println(err)
		panic("failed to open bptree")
	}
	// 创建对应的 bucket
	if err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		log.Println(err)
		panic("failed to create bucket in bptree")
	}
	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		err := bucket.Put(key, data.EncodeLogRecordPos(pos))
		return err
	}); err != nil {
		log.Println(err)
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		log.Println(err)
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		//err := bucket.Delete(key)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		log.Println(err)
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		log.Println(err)
		panic("failed to get key size")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBpTreeIterator(bpt.tree, reverse)
}

// B+树迭代器
type bpTreeIterator struct {
	tx           *bbolt.Tx
	cursor       *bbolt.Cursor
	reverse      bool
	currentKey   []byte
	currentValue []byte
}

// newBpTreeIterator 创建一个迭代器
func newBpTreeIterator(tree *bbolt.DB, reverse bool) *bpTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin transaction")
	}
	// 初始化
	bpi := &bpTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpTi *bpTreeIterator) Rewind() {
	if bpTi.reverse {
		bpTi.currentKey, bpTi.currentValue = bpTi.cursor.Last()
	} else {
		bpTi.currentKey, bpTi.currentValue = bpTi.cursor.First()
	}
}

func (bpTi *bpTreeIterator) Seek(key []byte) {
	bpTi.currentKey, bpTi.currentValue = bpTi.cursor.Seek(key)
}

func (bpTi *bpTreeIterator) Next() {
	if bpTi.reverse {
		bpTi.currentKey, bpTi.currentValue = bpTi.cursor.Prev()
	} else {
		bpTi.currentKey, bpTi.currentValue = bpTi.cursor.Next()
	}
}

func (bpTi *bpTreeIterator) Valid() bool {
	return len(bpTi.currentKey) != 0
}

func (bpTi *bpTreeIterator) Key() []byte {
	return bpTi.currentKey
}

func (bpTi *bpTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpTi.currentValue)
}

func (bpTi *bpTreeIterator) Close() {
	_ = bpTi.tx.Rollback()
}
