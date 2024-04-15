package main

import (
	"github.com/duanebester/1brc-go/advanced"
)

const (
	dataFilePath = "../1brc/measurements.txt"
)

func main() {
	// f, err := os.Create("cpuprofile")
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	// 	os.Exit(1)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	advanced.ParallelMmap(dataFilePath)
}
