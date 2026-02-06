package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/michaelscutari/dug/internal/scan"
	"github.com/michaelscutari/dug/internal/pathutil"
	"github.com/michaelscutari/dug/internal/snapshot"
	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"
)

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan a directory and create a database",
	Long:  `Scan a directory tree and store metadata in a SQLite database.`,
	RunE:  runScan,
}

var (
	scanRoot      string
	scanOut       string
	scanWorkers   int
	scanXdev      bool
	scanRetention int
	scanExclude   []string
	scanMaxErrors int
	scanVerbose   bool
	scanProgress  time.Duration
	scanIndexMode string
	scanSQLiteTmp string
)

func init() {
	scanCmd.Flags().StringVarP(&scanRoot, "root", "r", ".", "Root directory to scan")
	scanCmd.Flags().StringVarP(&scanOut, "out", "o", "./data", "Output directory for database")
	scanCmd.Flags().IntVarP(&scanWorkers, "workers", "w", 8, "Number of worker goroutines")
	scanCmd.Flags().BoolVar(&scanXdev, "xdev", true, "Don't cross filesystem boundaries")
	scanCmd.Flags().IntVar(&scanRetention, "retention", 5, "Number of snapshots to retain (0 = unlimited)")
	scanCmd.Flags().StringSliceVarP(&scanExclude, "exclude", "e", nil, "Regex patterns to exclude (can be repeated)")
	scanCmd.Flags().IntVar(&scanMaxErrors, "max-errors", 0, "Stop after N errors (0 = unlimited)")
	scanCmd.Flags().BoolVarP(&scanVerbose, "verbose", "v", false, "Enable verbose scan logging")
	scanCmd.Flags().DurationVar(&scanProgress, "progress-interval", 30*time.Second, "Emit progress lines to stderr at this interval when not a TTY (0 to disable)")
	scanCmd.Flags().StringVar(&scanIndexMode, "index-mode", "memory", "Index build mode: memory|disk|skip")
	scanCmd.Flags().StringVar(&scanSQLiteTmp, "sqlite-tmp-dir", "", "Directory for SQLite temp files during index build")
}

func runScan(cmd *cobra.Command, args []string) error {
	// Resolve paths
	root, err := filepath.Abs(scanRoot)
	if err != nil {
		return fmt.Errorf("failed to resolve root path: %w", err)
	}
	root = pathutil.Normalize(root)

	outDir, err := filepath.Abs(scanOut)
	if err != nil {
		return fmt.Errorf("failed to resolve output path: %w", err)
	}

	fmt.Printf("Scanning %s...\n", root)

	// Configure scanner
	opts := scan.DefaultOptions().
		WithWorkers(scanWorkers).
		WithXdev(scanXdev).
		WithMaxErrors(scanMaxErrors).
		WithVerbose(scanVerbose)

	for _, pattern := range scanExclude {
		if err := opts.AddExcludePattern(pattern); err != nil {
			return fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
		}
	}

	switch scanIndexMode {
	case "memory", "disk", "skip":
	default:
		return fmt.Errorf("invalid index mode %q (expected memory|disk|skip)", scanIndexMode)
	}

	// Use snapshot manager
	mgr := snapshot.NewManager(outDir, scanRetention)
	mgr.SetIndexMode(scanIndexMode)
	if scanSQLiteTmp != "" {
		mgr.SetSQLiteTmpDir(scanSQLiteTmp)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nCanceling... (press Ctrl+C again to force)")
		cancel()
		<-sigCh
		os.Exit(130)
	}()
	startTime := time.Now()

	// Set up progress display
	var lastFiles, lastDirs, lastErrors, lastBytes int64
	var spinnerIdx int
	isTTY := isTerminal()
	var stage atomic.Value
	stage.Store("scan")

	mgr.SetProgressFunc(func(files, dirs, errors int64, totalBytes int64) {
		atomic.StoreInt64(&lastFiles, files)
		atomic.StoreInt64(&lastDirs, dirs)
		atomic.StoreInt64(&lastErrors, errors)
		atomic.StoreInt64(&lastBytes, totalBytes)
	})
	mgr.SetStageFunc(func(s string) {
		if s == "" {
			return
		}
		stage.Store(s)
	})

	// Progress display goroutine
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		lastNonTTY := time.Now()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				if isTTY {
					stageVal := stage.Load()
					stageStr, _ := stageVal.(string)
					files := atomic.LoadInt64(&lastFiles)
					dirs := atomic.LoadInt64(&lastDirs)
					errors := atomic.LoadInt64(&lastErrors)
					bytes := atomic.LoadInt64(&lastBytes)
					elapsed := time.Since(startTime).Round(time.Millisecond)
					spinner := spinnerFrames[spinnerIdx%len(spinnerFrames)]
					spinnerIdx++

					if stageStr != "" && stageStr != "scan" {
						fmt.Fprintf(os.Stderr, "\r\033[K%s %s... | %s",
							spinner, stageStr, elapsed)
					} else {
						// Calculate rate (files per second)
						rate := float64(0)
						if elapsed.Seconds() > 0 {
							rate = float64(files+dirs) / elapsed.Seconds()
						}

						errStr := ""
						if errors > 0 {
							errStr = fmt.Sprintf(" | %d errors", errors)
						}

						fmt.Fprintf(os.Stderr, "\r\033[K%s Scanning... %d files | %d dirs | %s | %.0f/sec | %s%s",
							spinner, files, dirs, humanizeBytes(bytes), rate, elapsed, errStr)
					}
				} else if scanProgress > 0 && time.Since(lastNonTTY) >= scanProgress {
					stageVal := stage.Load()
					stageStr, _ := stageVal.(string)
					files := atomic.LoadInt64(&lastFiles)
					dirs := atomic.LoadInt64(&lastDirs)
					errors := atomic.LoadInt64(&lastErrors)
					bytes := atomic.LoadInt64(&lastBytes)
					elapsed := time.Since(startTime).Round(time.Millisecond)
					rate := float64(0)
					if elapsed.Seconds() > 0 {
						rate = float64(files+dirs) / elapsed.Seconds()
					}

					if stageStr != "" && stageStr != "scan" {
						fmt.Fprintf(os.Stderr, "PROGRESS stage=%s elapsed=%s\n", stageStr, elapsed)
					} else {
						fmt.Fprintf(os.Stderr, "PROGRESS files=%d dirs=%d bytes=%s rate=%.0f/sec elapsed=%s errors=%d\n",
							files, dirs, humanizeBytes(bytes), rate, elapsed, errors)
					}
					lastNonTTY = time.Now()
				}
			}
		}
	}()

	dbPath, err := mgr.RunScan(ctx, root, opts)
	close(progressDone)

	// Clear progress line
	if isTTY {
		fmt.Fprintf(os.Stderr, "\r\033[K")
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "Scan canceled.")
			return nil
		}
		return fmt.Errorf("scan failed: %w", err)
	}

	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Scan completed in %s\n", time.Since(startTime).Round(time.Millisecond))

	// Print summary
	database, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil // Non-fatal
	}
	defer database.Close()

	var fileCount, dirCount, totalSize, totalBlocks, errorCount int64
	database.QueryRow(`SELECT file_count, dir_count, total_size, total_blocks, error_count FROM scan_meta WHERE id = 1`).
		Scan(&fileCount, &dirCount, &totalSize, &totalBlocks, &errorCount)

	fmt.Printf("\nSummary:\n")
	fmt.Printf("  Files: %d\n", fileCount)
	fmt.Printf("  Directories: %d\n", dirCount)
	fmt.Printf("  Apparent size: %s\n", humanizeBytes(totalSize))
	fmt.Printf("  Disk usage: %s\n", humanizeBytes(totalBlocks))
	if errorCount > 0 {
		fmt.Printf("  Errors: %d\n", errorCount)
	}

	return nil
}

func humanizeBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
