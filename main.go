package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/avamsi/ergo/assert"
)

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

const numStations = 10_000

func processChunk(chunk string) map[string]*result {
	var (
		results = make(map[string]*result, numStations)
		line    string
	)
	line, chunk, _ = strings.Cut(chunk, "\n")
	for len(line) > 0 {
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
		line, chunk, _ = strings.Cut(chunk, "\n")
	}
	return results
}

func processFile(path string, out io.Writer) {
	var (
		f          = assert.Ok(os.Open(path))
		fileSize   = int(assert.Ok(f.Stat()).Size())
		dataBytes  = assert.Ok(syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_PRIVATE))
		numWorkers = runtime.NumCPU() - 2
		chunkSize  = fileSize / numWorkers
		results    = make(chan map[string]*result, numWorkers+2)
		g          sync.WaitGroup
	)
	defer func() {
		assert.Nil(f.Close())
		assert.Nil(syscall.Munmap(dataBytes))
	}()

	// TODO: I think unsafe.String should be okay here since we don't really
	// modify the underlying bytes, but need to understand mmap better to be
	// really sure.
	data := unsafe.String(unsafe.SliceData(dataBytes), fileSize)
	for len(data) > chunkSize {
		var (
			i     = chunkSize + strings.IndexByte(data[chunkSize:], '\n')
			chunk string
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

	var b bytes.Buffer
	b.Grow(numStations * 24)
	b.WriteByte('{')
	global.Items()(func(name string, r *result) bool {
		fmt.Fprintf(&b, "%s=%.1f/%.1f/%.1f, ",
			name, float64(r.min)/10, float64(r.sum)/float64(r.count), float64(r.max)/10)
		return true
	})
	b.Truncate(b.Len() - 2)
	b.WriteByte('}')
	assert.Ok(b.WriteTo(out))
}

func main() {
	if args := os.Args[1:]; len(args) == 1 {
		processFile(args[0], os.Stdout)
	} else {
		fmt.Fprintf(os.Stderr, "1brc: got %s, want exactly 1 argument\n", args)
	}
}
