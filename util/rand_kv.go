package util

import (
	"math/rand/v2"
	"strconv"
	"strings"
)

var letter = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func GetRandomKey(i int) []byte {
	var builder strings.Builder
	builder.Write([]byte("GoKeeper-key-"))
	builder.Write([]byte(strconv.Itoa(i)))
	return []byte(builder.String())
}

func GetRandomValue(n int) []byte {
	randomValue := make([]byte, n)
	var builder strings.Builder
	for i := range randomValue {
		randomValue[i] = letter[rand.IntN(len(letter))]
	}
	builder.Write([]byte("GoKeep-value-"))
	builder.Write(randomValue)
	return []byte(builder.String())
}
