package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataFile_Open(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	//t.Log(os.TempDir())
	file2, err := OpenDataFile("../tmp/", 111)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	file3, err := OpenDataFile("../tmp/", 111)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}

func TestDataFile_Write(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("Sakura"))
	assert.Nil(t, err)

	err = file.Write([]byte("LF"))
	assert.Nil(t, err)

	err = file.Write([]byte("A2"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 123)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("Sakura"))
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDataFile("../tmp/", 456)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("AAA"))
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)
}
