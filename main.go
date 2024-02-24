package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"

	"github.com/avamsi/ergo/assert"
)

func readInBatches() <-chan []string {
	const (
		chanBufferSize = 1_000
		readBufferSize = 1_000_000
		batchSize      = 1_000
	)
	c := make(chan []string, chanBufferSize)
	go func() {
		defer close(c)
		s := bufio.NewScanner(assert.Ok(os.Open("resources/measurements_10_8.txt")))
		s.Buffer(make([]byte, 0, readBufferSize), readBufferSize)
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
	min, max, sum float64
	count         int
}

func updateResults(lines []string, results map[string]*result) {
	for _, line := range lines {
		name, m, ok := strings.Cut(line, ";")
		if !ok {
			panic(line)
		}
		p := assert.Ok(strconv.ParseFloat(m, 64))
		if r, ok := results[name]; !ok {
			results[name] = &result{p, p, p, 1}
		} else {
			if p < r.min {
				r.min = p
			} else if p > r.max {
				r.max = p
			}
			r.sum, r.count = r.sum+p, r.count+1
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
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", name, r.min, r.sum/float64(r.count), r.max)
		return true
	})
}
