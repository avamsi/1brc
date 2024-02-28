package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"syscall"
	"unsafe"

	"github.com/avamsi/ergo/assert"
)

type result struct {
	min, max, sum, count int64
}

func parseMeasurement(m []byte) int64 {
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
		panic(string(m))
	}
	if negative {
		return -p
	}
	return p
}

const numStations = 10_000

func processChunk(chunk []byte) map[string]*result {
	var (
		results = make(map[string]*result, numStations)
		line    []byte
	)
	line, chunk, _ = bytes.Cut(chunk, []byte{'\n'})
	for len(line) > 0 {
		name, m, ok := bytes.Cut(line, []byte{';'})
		if !ok {
			panic(string(line))
		}
		var (
			// TODO: I think unsafe.String should be okay here since we don't
			// really modify the underlying bytes, but need to understand mmap
			// better to be really sure.
			s = unsafe.String(unsafe.SliceData(name), len(name))
			p = parseMeasurement(m)
		)
		if r, ok := results[s]; !ok {
			results[s] = &result{p, p, p, 1}
		} else {
			if p < r.min {
				r.min = p
			} else if p > r.max {
				r.max = p
			}
			r.sum, r.count = r.sum+p, r.count+10
		}
		line, chunk, _ = bytes.Cut(chunk, []byte{'\n'})
	}
	return results
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
		f          = assert.Ok(os.Open("resources/measurements_10_8.txt"))
		fileSize   = int(assert.Ok(f.Stat()).Size())
		data       = assert.Ok(syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_PRIVATE))
		numWorkers = runtime.NumCPU() - 2
		chunkSize  = fileSize / numWorkers
		results    = make(chan map[string]*result, numWorkers+2)
		g          sync.WaitGroup
	)
	defer syscall.Munmap(data)

	for len(data) > chunkSize {
		var (
			i     = chunkSize + bytes.IndexByte(data[chunkSize:], '\n')
			chunk []byte
		)
		chunk, data = data[:i], data[i+1:]
		g.Add(1)
		go func() {
			results <- processChunk(chunk)
			g.Done()
		}()
	}
	go func() {
		results <- processChunk(data)
		g.Wait()
		close(results)
	}()

	var global SkipList[string, *result]
	for chunk := range results {
		for name, r := range chunk {
			gr, ok := global.Get(name)
			if !ok {
				global.Put(name, r)
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

	global.Items()(func(name string, r *result) bool {
		fmt.Printf("%s=%.1f/%.1f/%.1f\n",
			name, float64(r.min)/10, float64(r.sum)/float64(r.count), float64(r.max)/10)
		return true
	})
}
