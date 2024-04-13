package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

type MeasurementSimple struct {
	Min   float64
	Max   float64
	Mean  float64
	Sum   float64
	Count int
}

func simple() {
	// Read file
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	measurementMap := make(map[string]*MeasurementSimple)

	fileScanner := bufio.NewScanner(dataFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		raw := fileScanner.Text()
		city, tempStr, found := strings.Cut(raw, ";")
		if !found {
			continue
		}
		temp, err := strconv.ParseFloat(tempStr, 32)
		if err != nil {
			panic(err)
		}

		measurement := measurementMap[city]

		if measurement == nil {
			measurementMap[city] = &MeasurementSimple{
				Min:   temp,
				Max:   temp,
				Sum:   temp,
				Count: 1,
			}
		} else {
			measurement.Min = min(measurement.Min, temp)
			measurement.Max = max(measurement.Max, temp)
			measurement.Sum += temp
			measurement.Count += 1
		}
	}

	// Sort by city name
	keys := make([]string, 0, len(measurementMap))
	for k := range measurementMap {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Print result
	total := len(keys)
	fmt.Print("{")
	for idx, k := range keys {
		measurement := measurementMap[k]
		measurement.Mean = measurement.Sum / float64(measurement.Count)
		if idx == total-1 {
			fmt.Printf("%s=%.1f/%.1f/%.1f", k, measurement.Min, measurement.Mean, measurement.Max)
		} else {
			fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, measurement.Min, measurement.Mean, measurement.Max)
		}
	}
	fmt.Println("}")
}
