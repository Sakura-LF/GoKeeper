package GoKeeper

import (
	"GoKeeper/index"
	"bytes"
)

// Iterator 面向用户的接口
type Iterator struct {
	indexIter index.Iterator // 迭代器接口
	db        *DB
	options   IteratorOption
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(options IteratorOption) *Iterator {
	iterator := db.index.Iterator(options.Reverse)
	return &Iterator{
		indexIter: iterator,
		db:        db,
		options:   options,
	}
}

func (i *Iterator) Rewind() {
	i.indexIter.Rewind()
	i.skipToNext()
}

func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
	i.skipToNext()
}

func (i *Iterator) Next() {
	i.indexIter.Next()
	i.skipToNext()
}

func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

func (i *Iterator) Value() ([]byte, error) {
	logRecord := i.indexIter.Value()
	return i.db.getValueByPosition(logRecord)
}

func (i *Iterator) Close() {
	i.indexIter.Close()
}

func (i *Iterator) skipToNext() {
	prefixLen := len(i.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; i.indexIter.Valid(); i.indexIter.Next() {
		if bytes.HasPrefix(i.indexIter.Key(), i.options.Prefix) {
			break
		}
	}
}
