package advanced

import (
	"bytes"
	"os"
	"runtime"

	"github.com/edsrzf/mmap-go"
)

const (
	// FNV-1 64-bit constants from hash/fnv.
	offset64   = 14695981039346656037
	prime64    = 1099511628211
	numBuckets = 1 << 17 // 2^17
)

type MemChunk struct {
	start int
	end   int
}

func splitMem(mem mmap.MMap, n int) []MemChunk {
	total := len(mem)
	chunkSize := total / n
	chunks := make([]MemChunk, n)

	chunks[0].start = 0
	for i := 1; i < n; i++ {
		for j := i * chunkSize; j < i*chunkSize+50; j++ {
			if mem[j] == '\n' {
				chunks[i-1].end = j
				chunks[i].start = j + 1
				break
			}
		}
	}
	chunks[n-1].end = total - 1
	return chunks
}

// Min returns the minimum of x and y.
func Min(x, y int) int {
	return y ^ ((x ^ y) & ((x - y) >> 63))
}

// Max returns the maximum of x and y.
func Max(x, y int) int {
	return x ^ ((x ^ y) & ((x - y) >> 63))
}

func Equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type item struct {
	key  []byte
	stat *Measurement
}

func readMemChunk(ch chan map[string]*Measurement, data mmap.MMap, start int, end int) {
	temperature := 0
	prev := start
	hash := uint64(offset64)
	items := make([]item, numBuckets) // hash buckets, linearly probed
	// size := 0                         // number of active items in items slice

	for i := start; i <= end; i++ {
		hash ^= uint64(data[i]) // FNV-1a is XOR then *
		hash *= prime64

		if data[i] == ';' {
			station_bytes := data[prev:i]
			temperature = 0
			i += 1
			negative := false

			for data[i] != '\n' {
				ch := data[i]
				if ch == '.' {
					i += 1
					continue
				}
				if ch == '-' {
					negative = true
					i += 1
					continue
				}
				ch -= '0'
				if ch > 9 {
					panic("Invalid character")
				}
				temperature = temperature*10 + int(ch)
				i += 1
			}

			if negative {
				temperature = -temperature
			}

			// Go to correct bucket in hash table.
			hashIndex := int(hash & uint64(numBuckets-1))
			for {
				if items[hashIndex].key == nil {
					// Found empty slot, add new item.
					items[hashIndex] = item{
						key: station_bytes,
						stat: &Measurement{
							Min:   temperature,
							Max:   temperature,
							Sum:   int64(temperature),
							Count: 1,
						},
					}
					// size++
					// if size > numBuckets/2 {
					// 	panic("too many items in hash table")
					// }
					break
				}
				if bytes.Equal(items[hashIndex].key, station_bytes) {
					// Found matching slot, add to existing stats.
					s := items[hashIndex].stat
					s.Min = Min(s.Min, temperature)
					s.Max = Max(s.Max, temperature)
					s.Sum += int64(temperature)
					s.Count++
					break
				}
				// Slot already holds another key, try next slot (linear probe).
				hashIndex++
				if hashIndex >= numBuckets {
					hashIndex = 0
				}
			}

			prev = i + 1
			temperature = 0
			hash = uint64(offset64)
		}
	}

	measurements := make(map[string]*Measurement)
	for _, item := range items {
		if item.key == nil {
			continue
		}
		measurements[string(item.key)] = item.stat
	}

	ch <- measurements
}

func ParallelMmap(dataFilePath string) {
	maxGoroutines := Min(runtime.NumCPU(), runtime.GOMAXPROCS(0))

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	data, err := mmap.Map(dataFile, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer data.Unmap()

	chunks := splitMem(data, maxGoroutines)
	totals := make(map[string]*Measurement)
	measurementChan := make(chan map[string]*Measurement)

	for i := 0; i < maxGoroutines; i++ {
		go readMemChunk(measurementChan, data, chunks[i].start, chunks[i].end)
	}

	for i := 0; i < maxGoroutines; i++ {
		measurements := <-measurementChan
		for station, measurement := range measurements {
			total := totals[station]
			if total == nil {
				totals[station] = measurement
			} else {
				total.Min = Min(total.Min, measurement.Min)
				total.Max = Max(total.Max, measurement.Max)
				total.Sum += measurement.Sum
				total.Count += measurement.Count
			}
		}
	}

	printResults(totals)
}
