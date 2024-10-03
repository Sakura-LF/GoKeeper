package util

import "testing"

func TestDirSize(t *testing.T) {
	size, err := DirSize("../tmp")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(size)
}
