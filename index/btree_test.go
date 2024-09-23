package index

import (
	"GoKeeper/data"
	"bytes"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	// 初始化 BTree
	bt := NewBTree()
	bt.Put([]byte("FF14"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1,
	})
	// 测试参数
	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	// 测试用例
	tests := []struct {
		name   string
		fields *BTree
		args   args
		want   bool
	}{
		{
			name:   "Key=nil",
			fields: NewBTree(),
			args: args{
				key: nil,
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 114514,
				},
			},
			want: true,
		},
		{
			name:   "Key=Sakura",
			fields: NewBTree(),
			args: args{
				key: []byte("Sakura"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 114514,
				},
			},
			want: true,
		},
		// 对已经存在的key进行put
		{
			name:   "Key=FF14",
			fields: NewBTree(),
			args: args{
				key: []byte("FF14"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 114514,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if bytes.Equal(tt.args.key, []byte("FF14")) {
				bt.Put(tt.args.key, tt.args.pos)
				logRecordPos := bt.Get(tt.args.key)
				assert.Equalf(t, tt.args.pos, logRecordPos, "Put(%s, %v),Expect(%v),Actual(%v)", tt.args.key, tt.args.pos, tt.want, logRecordPos)
				return
			}
			put := bt.Put(tt.args.key, tt.args.pos)
			assert.Equalf(t, tt.want, put, "Put(%s, %v),Expect(%v),Actual(%v)", tt.args.key, tt.args.pos, tt.want, put)
		})
	}
}

func TestBTree_Get(t *testing.T) {
	// 初始化 BTree
	bt := NewBTree()
	bt.Put([]byte("FF14"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1,
	})
	// 测试参数
	type args struct {
		key []byte
	}
	// 参数用例
	tests := []struct {
		name string
		args args
		want *data.LogRecordPos
	}{
		// key 为 nil, 不存在
		{
			name: "Key=nil",
			args: args{
				key: nil,
			},
			want: nil,
		},
		// key 为 Sakura, 不存在
		{
			name: "Key=Sakura",
			args: args{
				key: []byte("Sakura"),
			},
			want: nil,
		},
		// key 为 FF14, 存在
		{
			name: "Key=FF14",
			args: args{
				key: []byte("FF14"),
			},
			want: &data.LogRecordPos{Fid: 1, Offset: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logRecordPos := bt.Get(tt.args.key)
			assert.Equalf(t, tt.want, logRecordPos, "Get(%s),Expect:(%v),Actual:(%v)", tt.args.key, tt.want, logRecordPos)
		})
	}
}

func TestBTree_Delete(t *testing.T) {
	// 初始化 BTree
	bt := NewBTree()
	bt.Put([]byte("FF14"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1,
	})
	// 测试参数
	type args struct {
		key []byte
	}
	// 参数用例
	tests := []struct {
		name string
		args args
		want bool
	}{
		// key 为 nil, 不存在
		{
			name: "Key=nil",
			args: args{
				key: nil,
			},
			want: false,
		},
		// key 为 Sakura, 不存在
		{
			name: "Key=Sakura",
			args: args{
				key: []byte("Sakura"),
			},
			want: false,
		},
		// key 为 FF14, 存在
		{
			name: "Key=FF14",
			args: args{
				key: []byte("FF14"),
			},
			want: true,
		},
	}

	// 遍历测试用例,进行测试
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			success := bt.Delete(tt.args.key)
			assert.Equalf(t, tt.want, success, "Delete(%s),Expect:(%v),Actual:(%v)", tt.args.key, tt.want, success)
		})
	}
}

func TestBTree_Iterator(t *testing.T) {
	bTree1 := NewBTree()
	// 1. BTree 为空的情况
	iterator := bTree1.Iterator(false)
	//t.Log(iterator.Valid())
	assert.Equal(t, false, iterator.Valid())

	// 2. BTree 不为空的情况
	bTree1.Put([]byte("1"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1,
	})
	iterator = bTree1.Iterator(false)
	assert.NotNil(t, iterator.Key())
	assert.NotNil(t, iterator.Value())
	iterator.Next()
	assert.Equal(t, false, iterator.Valid())

	// 有多条数据
	for i := 0; i < 5; i++ {
		var builder strings.Builder
		builder.WriteString(strconv.Itoa(i))
		bTree1.Put([]byte(builder.String()), &data.LogRecordPos{
			Fid:    1,
			Offset: int64(i),
		})
	}
	iterator = bTree1.Iterator(false)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
		assert.NotNil(t, iterator.Value())
		//t.Log("Key:", string(iterator.Key()), " Value: ", iterator.Value())
	}

	// 测试反向遍历
	iterator = bTree1.Iterator(true)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
		assert.NotNil(t, iterator.Value())
		//t.Log("Key:", string(iterator.Key()), " Value: ", iterator.Value())
	}

	// 4.测试 Seek
	iterator2 := bTree1.Iterator(false)
	for iterator2.Seek([]byte("3")); iterator2.Valid(); iterator2.Next() {
		t.Log(string(iterator2.Key()))
	}

	// 5.反向不便利测试 Seek
	iterator3 := bTree1.Iterator(true)
	for iterator3.Seek([]byte("1")); iterator3.Valid(); iterator3.Next() {
		t.Log(string(iterator3.Key()))
	}
}
