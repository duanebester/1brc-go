package simple

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

type SimpleMeasurement struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

func Simple(dataFilePath string) {
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	measurements := make(map[string]*SimpleMeasurement)

	fileScanner := bufio.NewScanner(dataFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		rawString := fileScanner.Text()
		stationName, temperatureStr, found := strings.Cut(rawString, ";")
		if !found {
			continue
		}
		temperature, err := strconv.ParseFloat(temperatureStr, 32)
		if err != nil {
			panic(err)
		}

		measurement := measurements[stationName]
		if measurement == nil {
			measurements[stationName] = &SimpleMeasurement{
				Min:   temperature,
				Max:   temperature,
				Sum:   temperature,
				Count: 1,
			}
		} else {
			measurement.Min = min(measurement.Min, temperature)
			measurement.Max = max(measurement.Max, temperature)
			measurement.Sum += temperature
			measurement.Count += 1
		}
	}

	printSimpleResults(measurements)
}

func printSimpleResults(results map[string]*SimpleMeasurement) {
	// sort by station name
	stationNames := make([]string, 0, len(results))
	for stationName := range results {
		stationNames = append(stationNames, stationName)
	}

	slices.Sort(stationNames)

	fmt.Printf("{")
	for idx, stationName := range stationNames {
		measurement := results[stationName]
		mean := measurement.Sum / float64(measurement.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			stationName, measurement.Min, mean, measurement.Max)
		if idx < len(stationNames)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}\n")
}
