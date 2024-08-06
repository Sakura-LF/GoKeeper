package index

import (
	"GoKeeper/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {

	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields *BTree
		args   args
		want   bool
	}{
		{
			name:   "Put: key=nil",
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
			name:   "Put: key=Sakura",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
			}
			assert.Equalf(t, tt.want, bt.Put(tt.args.key, tt.args.pos), "Put(%s, %v)", tt.args.key, tt.args.pos)
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
			name: "Get: key = nil",
			args: args{
				key: nil,
			},
			want: nil,
		},
		// key 为 Sakura, 不存在
		{
			name: "Get: key = Sakura",
			args: args{
				key: []byte("Sakura"),
			},
			want: nil,
		},
		// key 为 FF14, 存在
		{
			name: "Get: key = FF14",
			args: args{
				key: []byte("FF14"),
			},
			want: &data.LogRecordPos{Fid: 1, Offset: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bt.Get(tt.args.key), "Get(%s, %v)", tt.args.key, tt.want)
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
			name: "Delete: key = nil",
			args: args{
				key: nil,
			},
			want: false,
		},
		// key 为 Sakura, 不存在
		{
			name: "Delete: key = Sakura",
			args: args{
				key: []byte("Sakura"),
			},
			want: false,
		},
		// key 为 FF14, 存在
		{
			name: "Delete: key = FF14",
			args: args{
				key: []byte("FF14"),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bt.Delete(tt.args.key), "Get(%s, %v)", tt.args.key, tt.want)
		})
	}
}
