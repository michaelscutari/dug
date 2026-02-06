package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
)

// DEBUG: Controlled by scan verbosity.

const insertDirSQL = `INSERT OR REPLACE INTO dirs (id, path, name, parent_id, depth) VALUES (?, ?, ?, ?, ?)`
const insertEntrySQL = `INSERT OR REPLACE INTO entries (parent_id, name, kind, size, blocks, mtime, dev_id, inode) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
const insertRollupSQL = `INSERT OR REPLACE INTO rollups (dir_id, total_size, total_blocks, total_files, total_dirs) VALUES (?, ?, ?, ?, ?)`
const insertErrorSQL = `INSERT INTO scan_errors (path, message) VALUES (?, ?)`

const maxErrorsSampled = 1000

// Ingester batches entries and writes them to the database.
type Ingester struct {
	db              *sql.DB
	entryCh         <-chan entry.Entry
	dirCh           <-chan entry.Dir
	rollupCh        <-chan entry.Rollup
	errorCh         <-chan entry.ScanError
	batchSize       int
	flushIntervalMs int
	maxErrors       int
	cancelFunc      context.CancelFunc

	entryBatch  []entry.Entry
	dirBatch    []entry.Dir
	rollupBatch []entry.Rollup
	errorBatch  []entry.ScanError
	errorCount  int64
	errorCapped bool

	// Progress tracking (atomic)
	fileCount  int64
	dirCount   int64
	totalBytes int64

	dirStmt    *sql.Stmt
	entryStmt  *sql.Stmt
	rollupStmt *sql.Stmt
	errorStmt  *sql.Stmt

	debug bool
}

// Progress holds current scan progress.
type Progress struct {
	Files      int64
	Dirs       int64
	Errors     int64
	TotalBytes int64
}

// NewIngester creates a new ingester.
func NewIngester(db *sql.DB, entryCh <-chan entry.Entry, dirCh <-chan entry.Dir, rollupCh <-chan entry.Rollup, errorCh <-chan entry.ScanError, batchSize, flushIntervalMs, maxErrors int, debug bool, cancelFunc context.CancelFunc) *Ingester {
	return &Ingester{
		db:              db,
		entryCh:         entryCh,
		dirCh:           dirCh,
		rollupCh:        rollupCh,
		errorCh:         errorCh,
		batchSize:       batchSize,
		flushIntervalMs: flushIntervalMs,
		maxErrors:       maxErrors,
		cancelFunc:      cancelFunc,
		entryBatch:      make([]entry.Entry, 0, batchSize),
		dirBatch:        make([]entry.Dir, 0, batchSize),
		rollupBatch:     make([]entry.Rollup, 0, batchSize),
		errorBatch:      make([]entry.ScanError, 0, 100),
		debug:           debug,
	}
}

// Run consumes entries from the channel and batches them to the database.
// It returns when the entry channel is closed.
func (ing *Ingester) Run(ctx context.Context) error {
	var err error
	ing.dirStmt, err = ing.db.Prepare(insertDirSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare dir statement: %w", err)
	}
	defer ing.dirStmt.Close()

	ing.entryStmt, err = ing.db.Prepare(insertEntrySQL)
	if err != nil {
		return fmt.Errorf("failed to prepare entry statement: %w", err)
	}
	defer ing.entryStmt.Close()

	ing.rollupStmt, err = ing.db.Prepare(insertRollupSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare rollup statement: %w", err)
	}
	defer ing.rollupStmt.Close()

	ing.errorStmt, err = ing.db.Prepare(insertErrorSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare error statement: %w", err)
	}
	defer ing.errorStmt.Close()

	ticker := time.NewTicker(time.Duration(ing.flushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	loopCount := 0
	if ing.debug {
		fmt.Fprintf(os.Stderr, "[INGESTER] STARTED batchSize=%d flushInterval=%dms\n", ing.batchSize, ing.flushIntervalMs)
	}

	entryCh := ing.entryCh
	dirCh := ing.dirCh
	rollupCh := ing.rollupCh
	errorCh := ing.errorCh

	for entryCh != nil || dirCh != nil || rollupCh != nil || errorCh != nil {
		loopCount++
		if ing.debug && loopCount%10000 == 0 {
			fmt.Fprintf(os.Stderr, "[INGESTER] LOOP#%d batchLen=%d files=%d dirs=%d\n",
				loopCount, len(ing.entryBatch), atomic.LoadInt64(&ing.fileCount), atomic.LoadInt64(&ing.dirCount))
		}

		select {
		case <-ctx.Done():
			if ing.debug {
				fmt.Fprintf(os.Stderr, "[INGESTER] CTX-CANCELLED batchLen=%d\n", len(ing.entryBatch))
			}
			return ing.flush()

		case e, ok := <-entryCh:
			if !ok {
				if ing.debug {
					fmt.Fprintf(os.Stderr, "[INGESTER] ENTRY-CH-CLOSED batchLen=%d\n", len(ing.entryBatch))
				}
				entryCh = nil
				continue
			}
			// Track progress
			if e.Kind == entry.KindFile {
				atomic.AddInt64(&ing.fileCount, 1)
				atomic.AddInt64(&ing.totalBytes, e.Blocks)
			}
			ing.entryBatch = append(ing.entryBatch, e)
			if len(ing.entryBatch) >= ing.batchSize {
				if err := ing.flushEntries(); err != nil {
					return err
				}
			}

		case d, ok := <-dirCh:
			if !ok {
				dirCh = nil
				continue
			}
			atomic.AddInt64(&ing.dirCount, 1)
			ing.dirBatch = append(ing.dirBatch, d)
			if len(ing.dirBatch) >= ing.batchSize {
				if err := ing.flushDirs(); err != nil {
					return err
				}
			}

		case r, ok := <-rollupCh:
			if !ok {
				rollupCh = nil
				continue
			}
			ing.rollupBatch = append(ing.rollupBatch, r)
			if len(ing.rollupBatch) >= ing.batchSize {
				if err := ing.flushRollups(); err != nil {
					return err
				}
			}

		case e, ok := <-errorCh:
			if !ok {
				errorCh = nil
				continue
			}
			ing.errorCount++
			// Check if max errors exceeded
			if ing.maxErrors > 0 && ing.errorCount >= int64(ing.maxErrors) {
				if ing.cancelFunc != nil {
					ing.cancelFunc() // Signal scan to stop
				}
			}
			// Only sample first N errors to bound memory
			if !ing.errorCapped {
				ing.errorBatch = append(ing.errorBatch, e)
				if len(ing.errorBatch) >= maxErrorsSampled {
					ing.errorCapped = true
					if err := ing.flushErrors(); err != nil {
						return err
					}
				}
			}

		case <-ticker.C:
			if ing.debug && len(ing.entryBatch) > 0 {
				fmt.Fprintf(os.Stderr, "[INGESTER] TICK-FLUSH batchLen=%d\n", len(ing.entryBatch))
			}
			if err := ing.flush(); err != nil {
				return err
			}
		}
	}

	if ing.debug {
		fmt.Fprintf(os.Stderr, "[INGESTER] INPUTS-CLOSED - flushing remaining batches\n")
	}
	return ing.flush()
}

func (ing *Ingester) flush() error {
	if err := ing.flushDirs(); err != nil {
		return err
	}
	if err := ing.flushEntries(); err != nil {
		return err
	}
	if err := ing.flushRollups(); err != nil {
		return err
	}
	return ing.flushErrors()
}

func (ing *Ingester) flushEntries() error {
	if len(ing.entryBatch) == 0 {
		return nil
	}

	batchLen := len(ing.entryBatch)
	flushStart := time.Now()
	if ing.debug {
		fmt.Fprintf(os.Stderr, "[INGESTER] FLUSH-START entries=%d files=%d dirs=%d\n",
			batchLen, atomic.LoadInt64(&ing.fileCount), atomic.LoadInt64(&ing.dirCount))
	}

	tx, err := ing.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt := tx.Stmt(ing.entryStmt)
	for _, e := range ing.entryBatch {
		_, err := stmt.Exec(e.ParentID, e.Name, e.Kind, e.Size, e.Blocks, e.ModTime.Unix(), e.DevID, e.Inode)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert entry %q: %w", e.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if ing.debug {
		fmt.Fprintf(os.Stderr, "[INGESTER] FLUSH-DONE entries=%d took=%v\n", batchLen, time.Since(flushStart))
	}

	ing.entryBatch = ing.entryBatch[:0]
	return nil
}

func (ing *Ingester) flushRollups() error {
	if len(ing.rollupBatch) == 0 {
		return nil
	}

	tx, err := ing.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin rollup transaction: %w", err)
	}

	stmt := tx.Stmt(ing.rollupStmt)
	for _, r := range ing.rollupBatch {
		_, err := stmt.Exec(r.DirID, r.TotalSize, r.TotalBlocks, r.TotalFiles, r.TotalDirs)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert rollup %d: %w", r.DirID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollup transaction: %w", err)
	}

	ing.rollupBatch = ing.rollupBatch[:0]
	return nil
}

// ErrorCount returns the total number of errors encountered.
func (ing *Ingester) ErrorCount() int64 {
	return atomic.LoadInt64(&ing.errorCount)
}

// Progress returns current scan progress (safe for concurrent access).
func (ing *Ingester) Progress() Progress {
	return Progress{
		Files:      atomic.LoadInt64(&ing.fileCount),
		Dirs:       atomic.LoadInt64(&ing.dirCount),
		Errors:     atomic.LoadInt64(&ing.errorCount),
		TotalBytes: atomic.LoadInt64(&ing.totalBytes),
	}
}

func (ing *Ingester) flushErrors() error {
	if len(ing.errorBatch) == 0 {
		return nil
	}

	tx, err := ing.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin error transaction: %w", err)
	}

	stmt := tx.Stmt(ing.errorStmt)
	for _, e := range ing.errorBatch {
		_, err := stmt.Exec(e.Path, e.Message)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert error for %q: %w", e.Path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit error transaction: %w", err)
	}

	ing.errorBatch = ing.errorBatch[:0]
	return nil
}

func (ing *Ingester) flushDirs() error {
	if len(ing.dirBatch) == 0 {
		return nil
	}

	tx, err := ing.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin dir transaction: %w", err)
	}

	stmt := tx.Stmt(ing.dirStmt)
	for _, d := range ing.dirBatch {
		_, err := stmt.Exec(d.ID, d.Path, d.Name, d.ParentID, d.Depth)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert dir %q: %w", d.Path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit dir transaction: %w", err)
	}

	ing.dirBatch = ing.dirBatch[:0]
	return nil
}
