package main

import (
	"bytes"
	"log"
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

// --------------------------------------------------------------------------
// Custom hashing (FNV-1a) stolen from https://benhoyt.com/writings/go-1brc/
// --------------------------------------------------------------------------
type item struct {
	key  []byte
	stat *Measurement
}

func readMemChunkCustomHash(ch chan map[string]*Measurement, m mmap.MMap, start int, end int) {
	temp := 0
	prev := start
	hash := uint64(offset64)
	buckets := make([]item, numBuckets) // hash buckets, linearly probed
	size := 0

	for i := start; i <= end; i++ {
		hash ^= uint64(m[i]) // FNV-1a is XOR then *
		hash *= prime64

		if m[i] == ';' {
			city_bytes := m[prev:i]
			temp = 0
			i += 1
			negative := false

			// Parse the temperature as an integer without decimal point
			for m[i] != '\n' {
				ch := m[i]
				// skip decimal point
				if ch == '.' {
					i += 1
					continue
				}
				// check for negative sign
				if ch == '-' {
					negative = true
					i += 1
					continue
				}
				// parse the digits - stole part of from strconv.Atoi
				ch -= '0'
				if ch > 9 {
					panic("Invalid character")
				}
				temp = temp*10 + int(ch)
				i += 1
			}

			if negative {
				temp = -temp
			}

			// Go to correct bucket in hash table.
			hashIndex := int(hash & uint64(numBuckets-1))
			for {
				if buckets[hashIndex].key == nil {
					// Found empty slot, add new item.
					// key := make([]byte, len(station))
					// copy(key, station) -- Not sure why Ben Hoyt did a copy here, but I didn't
					buckets[hashIndex] = item{
						key: city_bytes,
						stat: &Measurement{
							Min:   temp,
							Max:   temp,
							Sum:   int64(temp),
							Count: 1,
						},
					}
					size++
					if size > numBuckets/2 {
						panic("too many buckets in hash table")
					}
					break
				}
				if bytes.Equal(buckets[hashIndex].key, city_bytes) {
					// Found matching slot, add to existing stats.
					s := buckets[hashIndex].stat
					s.Min = min(s.Min, temp)
					s.Max = max(s.Max, temp)
					s.Sum += int64(temp)
					s.Count++
					break
				}
				// Slot already holds another key, try next slot (linear probe).
				hashIndex++
				if hashIndex >= numBuckets {
					hashIndex = 0
				}
			}

			// Reset for next line
			temp = 0
			prev = i + 1
			hash = uint64(offset64)
		}
	}
	result := make(map[string]*Measurement, size)
	for _, item := range buckets {
		if item.key == nil {
			continue
		}
		result[string(item.key)] = item.stat
	}
	ch <- result
}

func parallelMmapCustomHash() {
	maxGoroutines := min(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	f, err := os.Open(dataFilePath)
	if err != nil {
		log.Fatal(err)
	}
	// The file must be closed, even after calling Unmap.
	defer f.Close()

	// mapped acts as a writable slice of bytes that is a view into the open file, notes.txt.
	// It is sized to the file contents automatically.
	mapped, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Fatal(err)
	}
	// The Unmap method should be called when finished with it to avoid leaking memory
	// and to ensure that writes are flushed to disk.
	defer mapped.Unmap()

	chunks := splitMemory(mapped, maxGoroutines)
	totals := make(map[string]*Measurement)
	measurementChan := make(chan map[string]*Measurement)

	for i := 0; i < maxGoroutines; i++ {
		go readMemChunkCustomHash(measurementChan, mapped, chunks[i].start, chunks[i].end)
	}

	for i := 0; i < maxGoroutines; i++ {
		measurementMap := <-measurementChan
		for city, stats := range measurementMap {
			measurement := totals[city]
			if measurement == nil {
				totals[city] = stats
			} else {
				measurement.Min = min(measurement.Min, stats.Min)
				measurement.Max = max(measurement.Max, stats.Max)
				measurement.Sum += stats.Sum
				measurement.Count += stats.Count
			}
		}
	}

	printResultMap(totals)
}
