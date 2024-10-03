package index

import (
	"GoKeeper/data"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	// 初始化 BTree
	bt := NewBTree()
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
		want   *data.LogRecordPos
	}{
		{
			name:   "key为空",
			fields: NewBTree(),
			args: args{
				key: nil,
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 114514,
				},
			},
			want: nil,
		},
		{
			name:   "key正常",
			fields: NewBTree(),
			args: args{
				key: []byte("Sakura"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 114514,
				},
			},
			want: nil,
		},
		// 对已经存在的key进行put
		{
			name:   "已经存在的key",
			fields: NewBTree(),
			args: args{
				key: []byte("Sakura"),
				pos: &data.LogRecordPos{
					Fid:    2,
					Offset: 23456,
				},
			},
			want: &data.LogRecordPos{Fid: 1, Offset: 114514},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bt.Put(tt.args.key, tt.args.pos)
			assert.Equal(t, tt.want, got)
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
			name: "key为空,不存在",
			args: args{
				key: nil,
			},
			want: nil,
		},
		// key 为 Sakura, 不存在
		{
			name: "key不为空,不存在",
			args: args{
				key: []byte("Sakura"),
			},
			want: nil,
		},
		// key 为 FF14, 存在
		{
			name: "key不为空,存在",
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
	type WantArgs struct {
		got bool
		pos *data.LogRecordPos
	}
	// 参数用例
	tests := []struct {
		name     string
		args     args
		wantArgs WantArgs
	}{
		// key 为 nil, 不存在
		{
			name: "key为nil",
			args: args{
				key: nil,
			},
			wantArgs: WantArgs{
				got: false,
				pos: nil,
			},
		},
		// key 为 Sakura, 不存在
		{
			name: "key不存在",
			args: args{
				key: []byte("Sakura"),
			},
			wantArgs: WantArgs{
				got: false,
				pos: nil,
			},
		},
		// key 为 FF14, 存在
		{
			name: "key存在",
			args: args{
				key: []byte("FF14"),
			},
			wantArgs: WantArgs{
				got: true,
				pos: &data.LogRecordPos{Fid: 1, Offset: 1},
			},
		},
	}

	// 遍历测试用例,进行测试
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos, got := bt.Delete(tt.args.key)
			assert.Equal(t, tt.wantArgs.got, got)
			assert.Equal(t, tt.wantArgs.pos, pos)
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
