package advanced

import (
	"fmt"
	"os"
	"slices"

	"github.com/edsrzf/mmap-go"
)

type Measurement struct {
	Min   int
	Max   int
	Sum   int64
	Count int
}

func CustomMmap(dataFilePath string) {
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

	station := ""
	temperature := 0
	prev := 0
	total := len(data)
	measurements := make(map[string]*Measurement)
	for i := 0; i < total; i++ {
		if data[i] == ';' {
			station = string(data[prev:i])
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

			measurement := measurements[station]
			if measurement == nil {
				measurements[station] = &Measurement{
					Min:   temperature,
					Max:   temperature,
					Sum:   int64(temperature),
					Count: 1,
				}
			} else {
				measurement.Min = min(measurement.Min, temperature)
				measurement.Max = max(measurement.Max, temperature)
				measurement.Sum += int64(temperature)
				measurement.Count += 1
			}

			prev = i + 1
			station = ""
			temperature = 0
		}
	}
	printResults(measurements)
}

func printResults(results map[string]*Measurement) {
	// sort by station name
	stationNames := make([]string, 0, len(results))
	for stationName := range results {
		stationNames = append(stationNames, stationName)
	}

	slices.Sort(stationNames)

	fmt.Printf("{")
	for idx, stationName := range stationNames {
		measurement := results[stationName]
		mean := float64(measurement.Sum/10) / float64(measurement.Count)
		max := float64(measurement.Max) / 10
		min := float64(measurement.Min) / 10
		fmt.Printf("%s=%.1f/%.1f/%.1f", stationName, min, mean, max)
		if idx < len(stationNames)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}\n")
}
