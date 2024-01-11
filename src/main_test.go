package main

import (
	"runtime"
	"testing"
)

func Benchmark(b *testing.B) {
	for n := 0; n < b.N; n++ {
		run(parallelCommand, "./measurements.txt", false, runtime.NumCPU()*2, 1024*1024)
	}
}
