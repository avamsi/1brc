package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"

	"github.com/avamsi/ergo/assert"
	"golang.org/x/exp/mmap"
)

func readInBatches() <-chan []string {
	const (
		chanBufferSize = 1_000
		batchSize      = 1_000
	)
	c := make(chan []string, chanBufferSize)
	go func() {
		defer close(c)
		var (
			f = assert.Ok(mmap.Open("resources/measurements_10_8.txt"))
			s = bufio.NewScanner(io.NewSectionReader(f, 0, int64(f.Len())))
		)
		lines := make([]string, 0, batchSize)
		for s.Scan() {
			lines = append(lines, s.Text())
			if len(lines) == cap(lines) {
				c <- lines
				lines = make([]string, 0, batchSize)
			}
		}
		c <- lines
	}()
	return c
}

type result struct {
	min, max, sum, count int64
}

func parseMeasurement(m string) int64 {
	negative := m[0] == '-'
	if negative {
		m = m[1:]
	}
	var p int64
	const (
		z11  = int64('0') * 11
		z111 = int64('0') * 111
	)
	switch len(m) {
	case 3:
		p = int64(m[0])*10 + int64(m[2]) - z11
	case 4:
		p = int64(m[0])*100 + int64(m[1])*10 + int64(m[3]) - z111
	default:
		panic(m)
	}
	if negative {
		return -p
	}
	return p
}

func updateResults(lines []string, results map[string]*result) {
	for _, line := range lines {
		name, m, ok := strings.Cut(line, ";")
		if !ok {
			panic(line)
		}
		p := parseMeasurement(m)
		if r, ok := results[name]; !ok {
			results[name] = &result{p, p, p, 1}
		} else {
			if p < r.min {
				r.min = p
			} else if p > r.max {
				r.max = p
			}
			r.sum, r.count = r.sum+p, r.count+10
		}
	}
}

func reduceResults(results []map[string]*result) *SkipList[string, *result] {
	var global SkipList[string, *result]
	for _, shard := range results {
		for n, r := range shard {
			gr, ok := global.Get(n)
			if !ok {
				global.Put(n, r)
				continue
			}
			if r.min < gr.min {
				gr.min = r.min
			}
			if r.max > gr.max {
				gr.max = r.max
			}
			gr.sum, gr.count = gr.sum+r.sum, gr.count+r.count
		}
	}
	return &global
}

func main() {
	assert.Nil(trace.Start(assert.Ok(os.Create("debug/trace.out"))))
	assert.Nil(pprof.StartCPUProfile(assert.Ok(os.Create("debug/cpu.prof"))))
	defer func() {
		trace.Stop()
		pprof.StopCPUProfile()

		runtime.GC()
		assert.Nil(pprof.Lookup("allocs").WriteTo(assert.Ok(os.Create("debug/mem.prof")), 0))
	}()

	var (
		batches = readInBatches()
		results = make([]map[string]*result, runtime.NumCPU()-2)
		g       sync.WaitGroup
	)
	const numStations = 10_000
	for i := range results {
		g.Add(1)
		go func() {
			results[i] = make(map[string]*result, numStations)
			for batch := range batches {
				updateResults(batch, results[i])
			}
			g.Done()
		}()
	}
	g.Wait()

	global := reduceResults(results)
	global.Items()(func(name string, r *result) bool {
		fmt.Printf("%s=%.1f/%.1f/%.1f\n",
			name, float64(r.min)/10, float64(r.sum)/float64(r.count), float64(r.max)/10)
		return true
	})
}
