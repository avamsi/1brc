package main

import (
	"io"
	"testing"
)

func Benchmark108(b *testing.B) {
	for range b.N {
		processFile("resources/measurements_10_8.txt", io.Discard)
	}
}

func Benchmark109(b *testing.B) {
	for range b.N {
		processFile("resources/measurements_10_9.txt", io.Discard)
	}
}
