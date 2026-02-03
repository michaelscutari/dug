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

// DEBUG: Set to true to print ingester details - REMOVE before production
const debugIngester = true // ENABLED for deadlock debugging

const insertEntrySQL = `INSERT OR REPLACE INTO entries (path, name, parent, kind, size, blocks, mtime, depth, dev_id, inode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
const insertErrorSQL = `INSERT INTO scan_errors (path, message) VALUES (?, ?)`

const maxErrorsSampled = 1000

// Ingester batches entries and writes them to the database.
type Ingester struct {
	db              *sql.DB
	entryCh         <-chan entry.Entry
	errorCh         <-chan entry.ScanError
	batchSize       int
	flushIntervalMs int
	maxErrors       int
	cancelFunc      context.CancelFunc

	entryBatch  []entry.Entry
	errorBatch  []entry.ScanError
	errorCount  int64
	errorCapped bool

	// Progress tracking (atomic)
	fileCount  int64
	dirCount   int64
	totalBytes int64

	entryStmt *sql.Stmt
	errorStmt *sql.Stmt
}

// Progress holds current scan progress.
type Progress struct {
	Files      int64
	Dirs       int64
	Errors     int64
	TotalBytes int64
}

// NewIngester creates a new ingester.
func NewIngester(db *sql.DB, entryCh <-chan entry.Entry, errorCh <-chan entry.ScanError, batchSize, flushIntervalMs, maxErrors int, cancelFunc context.CancelFunc) *Ingester {
	return &Ingester{
		db:              db,
		entryCh:         entryCh,
		errorCh:         errorCh,
		batchSize:       batchSize,
		flushIntervalMs: flushIntervalMs,
		maxErrors:       maxErrors,
		cancelFunc:      cancelFunc,
		entryBatch:      make([]entry.Entry, 0, batchSize),
		errorBatch:      make([]entry.ScanError, 0, 100),
	}
}

// Run consumes entries from the channel and batches them to the database.
// It returns when the entry channel is closed.
func (ing *Ingester) Run(ctx context.Context) error {
	var err error
	ing.entryStmt, err = ing.db.Prepare(insertEntrySQL)
	if err != nil {
		return fmt.Errorf("failed to prepare entry statement: %w", err)
	}
	defer ing.entryStmt.Close()

	ing.errorStmt, err = ing.db.Prepare(insertErrorSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare error statement: %w", err)
	}
	defer ing.errorStmt.Close()

	ticker := time.NewTicker(time.Duration(ing.flushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	loopCount := 0
	if debugIngester {
		fmt.Fprintf(os.Stderr, "[INGESTER] STARTED batchSize=%d flushInterval=%dms\n", ing.batchSize, ing.flushIntervalMs)
	}

	for {
		loopCount++
		if debugIngester && loopCount%10000 == 0 {
			fmt.Fprintf(os.Stderr, "[INGESTER] LOOP#%d batchLen=%d files=%d dirs=%d\n",
				loopCount, len(ing.entryBatch), atomic.LoadInt64(&ing.fileCount), atomic.LoadInt64(&ing.dirCount))
		}

		select {
		case <-ctx.Done():
			if debugIngester {
				fmt.Fprintf(os.Stderr, "[INGESTER] CTX-CANCELLED batchLen=%d\n", len(ing.entryBatch))
			}
			return ing.flush()

		case e, ok := <-ing.entryCh:
			if !ok {
				if debugIngester {
					fmt.Fprintf(os.Stderr, "[INGESTER] ENTRY-CH-CLOSED batchLen=%d - draining errors and flushing\n", len(ing.entryBatch))
				}
				ing.drainErrors()
				return ing.flush()
			}
			// Track progress
			if e.Kind == entry.KindFile {
				atomic.AddInt64(&ing.fileCount, 1)
				atomic.AddInt64(&ing.totalBytes, e.Blocks)
			} else if e.Kind == entry.KindDir {
				atomic.AddInt64(&ing.dirCount, 1)
			}
			ing.entryBatch = append(ing.entryBatch, e)
			if len(ing.entryBatch) >= ing.batchSize {
				if err := ing.flushEntries(); err != nil {
					return err
				}
			}

		case e, ok := <-ing.errorCh:
			if ok {
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
			}

		case <-ticker.C:
			if debugIngester && len(ing.entryBatch) > 0 {
				fmt.Fprintf(os.Stderr, "[INGESTER] TICK-FLUSH batchLen=%d\n", len(ing.entryBatch))
			}
			if err := ing.flush(); err != nil {
				return err
			}
		}
	}
}

func (ing *Ingester) drainErrors() {
	for {
		select {
		case e, ok := <-ing.errorCh:
			if !ok {
				return
			}
			ing.errorCount++
			if !ing.errorCapped {
				ing.errorBatch = append(ing.errorBatch, e)
				if len(ing.errorBatch) >= maxErrorsSampled {
					ing.errorCapped = true
				}
			}
		default:
			return
		}
	}
}

func (ing *Ingester) flush() error {
	if err := ing.flushEntries(); err != nil {
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
	if debugIngester {
		fmt.Fprintf(os.Stderr, "[INGESTER] FLUSH-START entries=%d files=%d dirs=%d\n",
			batchLen, atomic.LoadInt64(&ing.fileCount), atomic.LoadInt64(&ing.dirCount))
	}

	tx, err := ing.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt := tx.Stmt(ing.entryStmt)
	for _, e := range ing.entryBatch {
		_, err := stmt.Exec(e.Path, e.Name, e.Parent, e.Kind, e.Size, e.Blocks, e.ModTime.Unix(), e.Depth, e.DevID, e.Inode)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert entry %q: %w", e.Path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if debugIngester {
		fmt.Fprintf(os.Stderr, "[INGESTER] FLUSH-DONE entries=%d took=%v\n", batchLen, time.Since(flushStart))
	}

	ing.entryBatch = ing.entryBatch[:0]
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
