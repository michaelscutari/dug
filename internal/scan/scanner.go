package scan

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/entry"
	"github.com/michaelscutari/dug/internal/rollup"
)

// Scanner coordinates the filesystem scan.
type Scanner struct {
	opts     *ScanOptions
	root     string
	rootDev  uint64
	database *sql.DB

	entryCh  chan entry.Entry
	errorCh  chan entry.ScanError
	dirCh    chan rollup.DirResult
	rollupCh chan entry.Rollup
	dirQueue chan dirWork

	inFlight int64

	wg        sync.WaitGroup
	closeOnce sync.Once

	ingester *db.Ingester
}

// NewScanner creates a new scanner.
func NewScanner(opts *ScanOptions) *Scanner {
	if opts == nil {
		opts = DefaultOptions()
	}
	// Much larger queue to avoid inline processing for directories with many subdirs
	queueSize := opts.Workers * 10000 // 80k with 8 workers
	if queueSize < 50000 {
		queueSize = 50000
	}
	// Entry channel sized at 10x batch size to handle bursts from inline processing
	entryChSize := opts.BatchSize * 10
	if entryChSize < 100000 {
		entryChSize = 100000
	}
	dirChSize := opts.Workers * 2048
	if dirChSize < 8192 {
		dirChSize = 8192
	}
	rollupChSize := opts.BatchSize * 2
	if rollupChSize < 10000 {
		rollupChSize = 10000
	}
	return &Scanner{
		opts:     opts,
		entryCh:  make(chan entry.Entry, entryChSize),
		errorCh:  make(chan entry.ScanError, 1000),
		dirCh:    make(chan rollup.DirResult, dirChSize),
		rollupCh: make(chan entry.Rollup, rollupChSize),
		dirQueue: make(chan dirWork, queueSize),
	}
}

// Run executes the scan starting from root and writes to the database.
func (s *Scanner) Run(ctx context.Context, root string, database *sql.DB) error {
	s.root = root
	s.database = database

	// Create cancellable context for max-errors abort
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get root device ID for cross-device check
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("failed to stat root: %w", err)
	}

	var rootInode uint64
	var rootBlocks int64
	if stat, ok := rootInfo.Sys().(*syscall.Stat_t); ok {
		s.rootDev = uint64(stat.Dev)
		rootInode = stat.Ino
		rootBlocks = stat.Blocks * 512
	}

	// Record scan start
	startTime := time.Now()
	if err := s.initScanMeta(startTime); err != nil {
		return err
	}

	// Emit root directory entry
	rootEntry := entry.Entry{
		Path:    root,
		Name:    rootInfo.Name(),
		Parent:  "",
		Kind:    entry.KindDir,
		Size:    rootInfo.Size(),
		Blocks:  rootBlocks,
		ModTime: rootInfo.ModTime(),
		Depth:   0,
		DevID:   s.rootDev,
		Inode:   rootInode,
	}
	s.entryCh <- rootEntry

	// Start ingester
	s.ingester = db.NewIngester(s.database, s.entryCh, s.rollupCh, s.errorCh, s.opts.BatchSize, s.opts.FlushIntervalMs, s.opts.MaxErrors, s.opts.Verbose, cancel)
	ingesterDone := make(chan error, 1)
	go func() {
		ingesterDone <- s.ingester.Run(ctx)
	}()

	// Start rollup aggregator
	agg := rollup.NewAggregator([]string{root})
	aggDone := make(chan error, 1)
	go func() {
		aggDone <- agg.Run(ctx, s.dirCh, s.rollupCh)
	}()

	// Start workers
	for i := 0; i < s.opts.Workers; i++ {
		worker := NewWorker(i, s.opts, s.root, s.rootDev, s.entryCh, s.errorCh, s.dirCh, s.dirQueue, &s.inFlight)
		s.wg.Add(1)
		go func(w *Worker) {
			defer s.wg.Done()
			w.Run(ctx)
		}(worker)
	}

	// Seed the queue with root
	atomic.AddInt64(&s.inFlight, 1)
	if s.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[SCANNER] SEEDED root=%s inFlight=1 queueSize=%d entryChSize=%d\n", root, cap(s.dirQueue), cap(s.entryCh))
	}
	select {
	case s.dirQueue <- dirWork{path: root, depth: 0}:
	case <-ctx.Done():
		atomic.AddInt64(&s.inFlight, -1)
	}

	// Monitor for completion or cancellation
	go s.monitorCompletion(ctx)

	// Wait for all in-flight directory processing to finish
	if s.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[SCANNER] WAITING for workers...\n")
	}
	s.wg.Wait()
	if s.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[SCANNER] ALL-WORKERS-DONE inFlight=%d queueLen=%d entryChLen=%d\n",
			atomic.LoadInt64(&s.inFlight), len(s.dirQueue), len(s.entryCh))
	}

	// Ensure queue is closed after workers exit (safe if already closed)
	s.closeDirQueue()

	// Close channels to signal completion
	if s.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[SCANNER] CLOSING entryCh and errorCh\n")
	}
	close(s.entryCh)
	close(s.errorCh)
	close(s.dirCh)

	// Wait for rollup aggregation to finish
	if err := <-aggDone; err != nil {
		return fmt.Errorf("rollup aggregation failed: %w", err)
	}

	// Wait for ingester to finish
	if err := <-ingesterDone; err != nil {
		return fmt.Errorf("ingester error: %w", err)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Update scan metadata with actual error count from ingester
	if err := s.finalizeScanMeta(s.ingester.ErrorCount()); err != nil {
		return err
	}

	return nil
}

type dirWork struct {
	path  string
	depth int
}

func (s *Scanner) monitorCompletion(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	checkCount := 0
	lastInFlight := int64(-1)
	stuckCount := 0

	for {
		select {
		case <-ctx.Done():
			if s.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[MONITOR] CTX-CANCELLED inFlight=%d queueLen=%d entryChLen=%d\n",
					atomic.LoadInt64(&s.inFlight), len(s.dirQueue), len(s.entryCh))
			}
			s.closeDirQueue()
			return
		case <-ticker.C:
			checkCount++
			inFlight := atomic.LoadInt64(&s.inFlight)
			queueLen := len(s.dirQueue)
			entryChLen := len(s.entryCh)

			// Log every second (20 ticks at 50ms)
			if s.opts.Verbose && checkCount%20 == 0 {
				fmt.Fprintf(os.Stderr, "[MONITOR] CHECK#%d inFlight=%d queueLen=%d entryChLen=%d\n",
					checkCount, inFlight, queueLen, entryChLen)
			}

			// Detect stuck state: inFlight unchanged for multiple checks
			if s.opts.Verbose && inFlight == lastInFlight && inFlight > 0 {
				stuckCount++
				if stuckCount >= 60 { // 3 seconds stuck
					fmt.Fprintf(os.Stderr, "[MONITOR] STUCK! inFlight=%d queueLen=%d entryChLen=%d stuckFor=%dms\n",
						inFlight, queueLen, entryChLen, stuckCount*50)
				}
			} else {
				stuckCount = 0
			}
			lastInFlight = inFlight

			if inFlight == 0 {
				if s.opts.Verbose {
					fmt.Fprintf(os.Stderr, "[MONITOR] COMPLETE inFlight=0 queueLen=%d entryChLen=%d - closing queue\n",
						queueLen, entryChLen)
				}
				s.closeDirQueue()
				return
			}
		}
	}
}

func (s *Scanner) closeDirQueue() {
	s.closeOnce.Do(func() {
		close(s.dirQueue)
	})
}

func (s *Scanner) initScanMeta(startTime time.Time) error {
	_, err := s.database.Exec(
		`INSERT INTO scan_meta (id, root_path, start_time) VALUES (1, ?, ?)`,
		s.root, startTime.Unix(),
	)
	return err
}

// Progress returns current scan progress (safe for concurrent access).
// Returns nil if scan hasn't started.
func (s *Scanner) Progress() *db.Progress {
	if s.ingester == nil {
		return nil
	}
	p := s.ingester.Progress()
	return &p
}

func (s *Scanner) finalizeScanMeta(errorCount int64) error {
	// Get counts from the database
	var fileCount, dirCount, totalSize, totalBlocks int64
	row := s.database.QueryRow(`SELECT COUNT(*) FROM entries WHERE kind = 0`)
	row.Scan(&fileCount)

	row = s.database.QueryRow(`SELECT COUNT(*) FROM entries WHERE kind = 1`)
	row.Scan(&dirCount)

	row = s.database.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM entries WHERE kind = 0`)
	row.Scan(&totalSize)

	row = s.database.QueryRow(`SELECT COALESCE(SUM(blocks), 0) FROM entries WHERE kind = 0`)
	row.Scan(&totalBlocks)

	_, err := s.database.Exec(
		`UPDATE scan_meta SET end_time = ?, total_size = ?, total_blocks = ?, file_count = ?, dir_count = ?, error_count = ? WHERE id = 1`,
		time.Now().Unix(), totalSize, totalBlocks, fileCount, dirCount, errorCount,
	)
	return err
}
