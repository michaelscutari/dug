package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	dir := flag.String("dir", ".", "Directory to probe")
	limit := flag.Int("limit", 200000, "Max entries to sample (0 = all)")
	workers := flag.Int("workers", 8, "Concurrent lstat workers")
	recursive := flag.Bool("recursive", false, "Walk recursively and sample files/dirs")
	shuffle := flag.Bool("shuffle", false, "Shuffle sampled paths")
	sampleSeed := flag.Int64("seed", 0, "Shuffle seed (0 = time-based)")
	inline := flag.Bool("inline", false, "Measure lstat during walk to avoid warming cache")
	flag.Parse()

	if *inline && *shuffle {
		fmt.Fprintln(os.Stderr, "warning: --shuffle is ignored with --inline")
		*shuffle = false
	}

	var paths []string
	readDirDur := time.Duration(0)
	start := time.Now()
	if *recursive {
		if !*inline {
			var seen int
			err := filepath.WalkDir(*dir, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return nil
				}
				if path == *dir {
					return nil
				}
				paths = append(paths, path)
				seen++
				if *limit > 0 && seen >= *limit {
					return filepath.SkipAll
				}
				return nil
			})
			readDirDur = time.Since(start)
			if err != nil {
				fmt.Fprintf(os.Stderr, "walk error: %v\n", err)
				os.Exit(1)
			}
		}
	} else {
		entries, err := os.ReadDir(*dir)
		readDirDur = time.Since(start)
		if err != nil {
			fmt.Fprintf(os.Stderr, "readdir error: %v\n", err)
			os.Exit(1)
		}

		if *limit > 0 && *limit < len(entries) {
			entries = entries[:*limit]
		}

		paths = make([]string, 0, len(entries))
		for _, de := range entries {
			paths = append(paths, filepath.Join(*dir, de.Name()))
		}
	}

	if *shuffle && len(paths) > 1 {
		seed := *sampleSeed
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
	}

	var idx int64
	var statCount int64
	var errCount int64
	var totalDur int64

	start = time.Now()
	var wg sync.WaitGroup
	var elapsed time.Duration

	if *inline && *recursive {
		pathCh := make(chan string, *workers*4)
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for path := range pathCh {
					t0 := time.Now()
					_, err := os.Lstat(path)
					atomic.AddInt64(&totalDur, time.Since(t0).Microseconds())
					atomic.AddInt64(&statCount, 1)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
				}
			}()
		}

		var seen int
		walkStart := time.Now()
		err := filepath.WalkDir(*dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if path == *dir {
				return nil
			}
			pathCh <- path
			seen++
			if *limit > 0 && seen >= *limit {
				return filepath.SkipAll
			}
			return nil
		})
		readDirDur = time.Since(walkStart)
		close(pathCh)
		wg.Wait()
		elapsed = time.Since(start)
		if err != nil {
			fmt.Fprintf(os.Stderr, "walk error: %v\n", err)
			os.Exit(1)
		}
	} else {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					n := int(atomic.AddInt64(&idx, 1)) - 1
					if n >= len(paths) {
						return
					}
					t0 := time.Now()
					_, err := os.Lstat(paths[n])
					atomic.AddInt64(&totalDur, time.Since(t0).Microseconds())
					atomic.AddInt64(&statCount, 1)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
					}
				}
			}()
		}
		wg.Wait()
		elapsed = time.Since(start)
	}

	avg := time.Duration(0)
	if statCount > 0 {
		avg = time.Duration(atomic.LoadInt64(&totalDur)/statCount) * time.Microsecond
	}

	fmt.Printf("dir=%s entries=%d workers=%d recursive=%t shuffle=%t inline=%t\n", *dir, int(statCount), *workers, *recursive, *shuffle, *inline)
	fmt.Printf("readdir: %v\n", readDirDur)
	fmt.Printf("lstat:   calls=%d avg=%v total=%v errors=%d\n", statCount, avg, elapsed, errCount)
	if elapsed.Seconds() > 0 {
		fmt.Printf("throughput: %.0f stats/sec\n", float64(statCount)/elapsed.Seconds())
	}
}
