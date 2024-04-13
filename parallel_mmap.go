package main

import (
	"log"
	"os"
	"runtime"

	"github.com/edsrzf/mmap-go"
)

func readMemChunk(ch chan map[string]*Measurement, m mmap.MMap, start int, end int) {
	var city string
	var temp int
	var prev int = start
	var measurementMap = make(map[string]*Measurement)
	for i := start; i <= end; i++ {
		if m[i] == ';' {
			city = string(m[prev:i])
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

			measurement := measurementMap[city]
			if measurement == nil {
				measurementMap[city] = &Measurement{
					Min:   temp,
					Max:   temp,
					Sum:   int64(temp),
					Count: 1,
				}
			} else {
				measurement.Min = min(measurement.Min, temp)
				measurement.Max = max(measurement.Max, temp)
				measurement.Sum += int64(temp)
				measurement.Count += 1
			}

			temp = 0
			city = ""
			prev = i + 1
		}
	}
	ch <- measurementMap
}

func parallelMmap() {
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
		go readMemChunk(measurementChan, mapped, chunks[i].start, chunks[i].end)
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
