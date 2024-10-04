package benchmark

import (
	"GoKeeper"
	"GoKeeper/util"
	"errors"
	"github.com/stretchr/testify/assert"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

var DB *GoKeeper.DB

func init() {
	// 初始化
	options := GoKeeper.DefaultOptions
	options.DirPath = filepath.Join(os.TempDir(), "goKeeperBench")

	var err error
	DB, err = GoKeeper.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := DB.Put(util.GetRandomKey(i), util.GetRandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := DB.Get(util.GetRandomKey(rand.Int()))
		if err != nil && !errors.Is(err, GoKeeper.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := DB.Delete(util.GetRandomKey(i))
		assert.Nil(b, err)
	}
}

//func TestDB_LIST(t *testing.T) {
//	//keys := DB.ListKeys()
//	//for i := 0; i < 3; i++ {
//	//	t.Log(string(string(keys[i])))
//	//}
//	//
//	//err := DB.Delete(util.GetRandomKey(2))
//	//assert.Nil(t, err)
//
//	stat := DB.Stat()
//	fmt.Printf("%+v", stat)
//}
