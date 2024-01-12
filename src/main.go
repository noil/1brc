package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	_chunkSize = 1024 * 1024 // 1MB
)

func main() {
	// populate()
	parallel(os.Args[1], true, runtime.NumCPU()*2, _chunkSize)
}

const (
	populateCommand string = "populate"
	parallelCommand string = "parallel"
)

func run(command string, filePath string, debug bool, countWorkers int, sizeChunk int) {
	switch command {
	case populateCommand:
		populate()
	case parallelCommand:
		parallel(filePath, debug, countWorkers, sizeChunk)
	default:
		panic("Unknown command")
	}

}

type StationStats struct {
	Min   int64
	Max   int64
	Total int64
	Count int64
}

func NewStationStats() *StationStats {
	return &StationStats{Min: 1<<63 - 1}
}
func parallel(filePath string, debug bool, countWorkers int, chunkSize int) {
	timeStart := time.Now()
	fd, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	fi, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	fSize := int(fi.Size())

	data, err := syscall.Mmap(int(fd.Fd()), 0, fSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			panic(err)
		}
	}()

	chunks := (fSize / chunkSize) + 1
	wCh := make(chan []byte)
	wg := sync.WaitGroup{}
	wg.Add(chunks)
	// ----------------------------------------
	stations := make(map[string]*StationStats, 512)
	names := make([]string, 0, 512)
	statsCh := make(chan map[string]*StationStats, chunks)
	statsWg := sync.WaitGroup{}
	statsWg.Add(chunks)
	go func() {
		for stats := range statsCh {
			for station, stats := range stats {
				cur := stations[station]
				if cur == nil {
					stations[station] = stats
					names = append(names, station)
					continue
				}
				cur.Min = min(cur.Min, stats.Min)
				cur.Max = max(cur.Max, stats.Max)
				cur.Total += stats.Total
				cur.Count += stats.Count
			}
			statsWg.Done()
		}
	}()

	// ----------------------------------------
	for i := 0; i < countWorkers; i++ {
		go func() {
			for data := range wCh {
				stations := make(map[string]*StationStats, 512)
				nameStartIndx := 0
				nameEndIndx := 0
				value := int64(0)
				ln := len(data)
				for j := 0; j < ln; {
					if data[j] == ';' {
						nameEndIndx = j
						if data[j+1] == '-' {
							// -1.2\n
							if data[j+5] == '\n' {
								value = -(int64(data[j+2]-'0')*10 + int64(data[j+4]-'0'))
								j += 5
							} else {
								// -12.3\n
								value = -(int64(data[j+2]-'0')*100 + int64(data[j+3]-'0')*10 + int64(data[j+5]-'0'))
								j += 6
							}
						} else {
							// 1.2\n
							if data[j+4] == '\n' {
								value = int64(data[j+1]-'0')*10 + int64(data[j+3]-'0')
								j += 4
							} else {
								// 12.3\n
								value = int64(data[j+1]-'0')*100 + int64(data[j+2]-'0')*10 + int64(data[j+4]-'0')
								j += 5
							}
						}

						key := unsafe.String(unsafe.SliceData(data[nameStartIndx:nameEndIndx]), len(data[nameStartIndx:nameEndIndx]))
						nameStartIndx = j + 1
						cur := stations[key]
						if cur == nil {
							stations[key] = &StationStats{Min: value, Max: value, Total: value, Count: 1}
							continue
						}
						cur.Min = min(cur.Min, value)
						cur.Max = max(cur.Max, value)
						cur.Total += value
						cur.Count++
					}
					j++
				}

				statsCh <- stations
				wg.Done()
			}
		}()
	}

	start := 0
	end := chunkSize
	for i := 0; i < chunks; i++ {
		if end > fSize {
			end = fSize - 1
		}
		for data[end] != '\n' {
			end--
		}
		wCh <- data[start : end+1]
		start = end + 1
		end += chunkSize
	}
	wg.Wait()
	close(wCh)

	statsWg.Wait()
	close(statsCh)
	// ----------------------------------------

	sort.Strings(names)
	result := strings.Builder{}
	result.Grow(1024 * 1024)
	for _, name := range names {
		result.WriteString(name)
		result.WriteString("=")
		result.WriteString(fmt.Sprintf("%.1f/%.1f/%.1f", float64(stations[name].Min)/10.0, float64(stations[name].Total)/float64(stations[name].Count*10), float64(stations[name].Max)/10.0))
		result.WriteString(", ")
	}

	if debug {
		fmt.Fprintf(os.Stdout, "{%s}\n", result.String()[:result.Len()-2])
		fmt.Println("Time taken to read file: ", time.Since(timeStart))
	}
}
