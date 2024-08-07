package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// 在测试结束后删除文件
func RemoveFile(filename string) {
	// RemoveAll 删除这个路径下的所有文件
	// Remove 删除单个文件
	if err := os.RemoveAll(filename); err != nil {
		panic(err)
	}
}

func TestFileIo_Sync(t *testing.T) {
	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)
	defer file.Close()

	err = file.Sync()
	assert.Nil(t, err)
}

func TestFileIo_Close(t *testing.T) {

	filePath := filepath.Join("../tmp", "Test.data")
	file, err := NewFileIO(filePath)
	defer RemoveFile(filePath)

	err = file.Close()
	assert.Nil(t, err)
}

func TestNewFileIO(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		args    args
		want    *FileIo
		wantErr assert.ErrorAssertionFunc
	}{
		{name: "sakura.data", args: args{filename: "../testFile/sakura.data"}, want: &FileIo{}},
		{name: "test.data", args: args{filename: "../testFile/test.data"}, want: &FileIo{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			io, err := NewFileIO(tt.args.filename)
			assert.Nil(t, err)
			assert.NotNil(t, io)
		})
	}
}

func TestFileIo_Write(t *testing.T) {
	fileIo, _ := NewFileIO("../testFile/sakura.data")
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "[]byte",
			args: args{
				bytes: []byte("FF14 is very Good"),
			},
			want: 17,
		},
		{
			name: "empty",
			args: args{
				bytes: []byte(""),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fileIo.Write(tt.args.bytes)
			assert.Nil(t, err)
			assert.Equalf(t, tt.want, got, "Write(%v)", tt.args.bytes)
		})
	}
}

func TestFileIo_Read1(t *testing.T) {
	fileIO, _ := NewFileIO("../testFile/sakura.data")
	// 测试参数
	type args struct {
		bytes []byte
		i     int64
	}
	// 预期参数
	type wantArgs struct {
		wantN     int
		wantSlice []byte
	}
	// 测试用例
	tests := []struct {
		name string
		args args
		want wantArgs
	}{
		{
			name: "[17]byte()",
			args: args{
				bytes: make([]byte, 17),
				i:     0,
			},
			want: wantArgs{
				wantN:     17,
				wantSlice: []byte("FF14 is very Good"),
			},
		},
		{
			name: "[6]byte()",
			args: args{
				bytes: make([]byte, 6),
				i:     17,
			},
			want: wantArgs{
				wantN:     6,
				wantSlice: []byte("Sakura"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readN, err := fileIO.Read(tt.args.bytes, tt.args.i)
			assert.Equalf(t, tt.want.wantN, readN, "")
			assert.Nil(t, err)
			assert.Equalf(t, tt.want.wantSlice, tt.args.bytes, "Read(%v, %v)", tt.args.bytes, tt.args.i)
			t.Log(string(tt.args.bytes))
		})
	}
}
