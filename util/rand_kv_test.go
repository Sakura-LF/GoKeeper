package util

import (
	"fmt"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 5; i++ {
		fmt.Println(string(GetRandomKey(i)))
	}
}

func TestGetRandomValue(t *testing.T) {
	for i := 0; i < 5; i++ {
		fmt.Println(string(GetRandomValue(10)))
	}
}
