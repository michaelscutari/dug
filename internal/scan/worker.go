package scan

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
	"github.com/michaelscutari/dug/internal/rollup"
)

// DEBUG: Use scan options to control verbosity.
const slowOpThreshold = 200 * time.Millisecond

// Worker processes directories and emits entries.
type Worker struct {
	id       int
	opts     *ScanOptions
	root     string
	rootDev  uint64
	entryCh  chan<- entry.Entry
	dirCh    chan<- entry.Dir
	errorCh  chan<- entry.ScanError
	dirResCh chan<- rollup.DirResult
	dirQueue chan dirWork
	inFlight *int64
	stack    []dirWork
	dirIDSeq *int64
}

// NewWorker creates a new worker.
func NewWorker(id int, opts *ScanOptions, root string, rootDev uint64, entryCh chan<- entry.Entry, dirCh chan<- entry.Dir, errorCh chan<- entry.ScanError, dirResCh chan<- rollup.DirResult, dirQueue chan dirWork, inFlight *int64, dirIDSeq *int64) *Worker {
	return &Worker{
		id:       id,
		opts:     opts,
		root:     root,
		rootDev:  rootDev,
		entryCh:  entryCh,
		dirCh:    dirCh,
		errorCh:  errorCh,
		dirResCh: dirResCh,
		dirQueue: dirQueue,
		inFlight: inFlight,
		dirIDSeq: dirIDSeq,
	}
}

// Run processes directory work until the queue is closed.
func (w *Worker) Run(ctx context.Context) {
	if w.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[W%d] STARTED inFlight=%d queueLen=%d\n", w.id, atomic.LoadInt64(w.inFlight), len(w.dirQueue))
	}
	defer func() {
		if w.opts.Verbose {
			fmt.Fprintf(os.Stderr, "[W%d] EXITING inFlight=%d queueLen=%d stackLen=%d\n", w.id, atomic.LoadInt64(w.inFlight), len(w.dirQueue), len(w.stack))
		}
	}()

	loopCount := 0
	for {
		loopCount++
		inFlight := atomic.LoadInt64(w.inFlight)

		// Periodic status every 1000 loops
		if w.opts.Verbose && loopCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "[W%d] LOOP#%d inFlight=%d queueLen=%d stackLen=%d\n", w.id, loopCount, inFlight, len(w.dirQueue), len(w.stack))
		}

		if len(w.stack) > 0 {
			work := w.stack[len(w.stack)-1]
			w.stack = w.stack[:len(w.stack)-1]
			if w.opts.Verbose && loopCount%500 == 0 {
				fmt.Fprintf(os.Stderr, "[W%d] POP-STACK depth=%d stackLen=%d path=%s\n", w.id, work.depth, len(w.stack), work.path)
			}
			w.processWork(ctx, work)
			continue
		}

		if w.opts.Verbose && loopCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "[W%d] WAITING-QUEUE inFlight=%d queueLen=%d\n", w.id, inFlight, len(w.dirQueue))
		}

		select {
		case <-ctx.Done():
			if w.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[W%d] CTX-CANCELLED inFlight=%d\n", w.id, atomic.LoadInt64(w.inFlight))
			}
			return
		case work, ok := <-w.dirQueue:
			if !ok {
				if w.opts.Verbose {
					fmt.Fprintf(os.Stderr, "[W%d] QUEUE-CLOSED inFlight=%d\n", w.id, atomic.LoadInt64(w.inFlight))
				}
				return
			}
			if w.opts.Verbose && loopCount%500 == 0 {
				fmt.Fprintf(os.Stderr, "[W%d] DEQUEUE depth=%d queueLen=%d path=%s\n", w.id, work.depth, len(w.dirQueue), work.path)
			}
			w.processWork(ctx, work)
		}
	}
}

// ProcessDirectory reads a directory and emits entries for each child.
func (w *Worker) ProcessDirectory(ctx context.Context, work dirWork) {
	if ctx.Err() != nil {
		return
	}

	dirPath := work.path
	depth := work.depth

	// DEBUG: Log every directory being processed
	if w.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[W%d] READDIR-START depth=%d path=%s\n", w.id, depth, dirPath)
	}

	readStart := time.Now()
	dirEntries, err := os.ReadDir(dirPath)
	if w.opts.Verbose {
		if took := time.Since(readStart); took > slowOpThreshold {
			fmt.Fprintf(os.Stderr, "[W%d] READDIR-SLOW depth=%d took=%s path=%s\n", w.id, depth, took, dirPath)
		}
	}

	if w.opts.Verbose {
		if err != nil {
			fmt.Fprintf(os.Stderr, "[W%d] READDIR-ERR depth=%d err=%v path=%s\n", w.id, depth, err, dirPath)
		} else {
			fmt.Fprintf(os.Stderr, "[W%d] READDIR-OK depth=%d entries=%d path=%s\n", w.id, depth, len(dirEntries), dirPath)
		}
	}

	if err != nil {
		// Non-blocking send - drop error if channel full (errors are sampled anyway)
		select {
		case w.errorCh <- entry.ScanError{
			Path:    dirPath,
			Message: err.Error(),
		}:
		default:
		}
		w.emitDirResult(ctx, work.dirID, work.parentID, 0, 0, 0, 0)
		return
	}

	var fileSize int64
	var fileBlocks int64
	var fileCount int64
	childDirs := make([]dirWork, 0, 16)

	for i, de := range dirEntries {
		// Check for cancellation every 100 entries and at start
		if i%100 == 0 && ctx.Err() != nil {
			if w.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[W%d] CTX-CANCEL in loop\n", w.id)
			}
			return
		}

		childPath := filepath.Join(dirPath, de.Name())

		if w.opts.ShouldExclude(childPath) {
			continue
		}

		// Always use Lstat to avoid following symlinks
		statStart := time.Now()
		info, err := os.Lstat(childPath)
		if w.opts.Verbose {
			if took := time.Since(statStart); took > slowOpThreshold {
				fmt.Fprintf(os.Stderr, "[W%d] LSTAT-SLOW depth=%d took=%s path=%s\n", w.id, depth, took, childPath)
			}
		}
		if err != nil {
			if w.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[W%d] LSTAT-ERR path=%s err=%v\n", w.id, childPath, err)
			}
			// Non-blocking send - drop error if channel full (errors are sampled anyway)
			select {
			case w.errorCh <- entry.ScanError{
				Path:    childPath,
				Message: err.Error(),
			}:
			default:
			}
			continue
		}

		// Get device ID, inode, and blocks from stat
		var devID, inode uint64
		var blocks int64
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			devID = uint64(stat.Dev)
			inode = stat.Ino
			blocks = stat.Blocks * 512 // st_blocks is in 512-byte units
		}

		// Cross-device check
		if w.opts.Xdev && devID != 0 && devID != w.rootDev {
			continue
		}

		kind := entry.KindFromMode(info.Mode())

		// Queue subdirectories for processing (fallback to local stack if queue is full)
		if kind == entry.KindFile {
			fileSize += info.Size()
			fileBlocks += blocks
			fileCount++
			e := entry.Entry{
				ParentID: work.dirID,
				Name:     de.Name(),
				Kind:     kind,
				Size:     info.Size(),
				Blocks:   blocks,
				ModTime:  info.ModTime(),
				DevID:    devID,
				Inode:    inode,
			}
			select {
			case w.entryCh <- e:
			case <-ctx.Done():
				return
			default:
				if w.opts.Verbose {
					fmt.Fprintf(os.Stderr, "\n[DEBUG] Entry channel full, blocking on: %s\n", childPath)
				}
				select {
				case w.entryCh <- e:
				case <-ctx.Done():
					return
				}
			}
		} else if kind == entry.KindDir {
			childID := atomic.AddInt64(w.dirIDSeq, 1)
			dirEntry := entry.Dir{
				ID:       childID,
				Path:     childPath,
				Name:     de.Name(),
				ParentID: work.dirID,
				Depth:    depth + 1,
			}
			select {
			case w.dirCh <- dirEntry:
			case <-ctx.Done():
				return
			}
			childDirs = append(childDirs, dirWork{path: childPath, dirID: childID, parentID: work.dirID, depth: depth + 1})
		} else {
			e := entry.Entry{
				ParentID: work.dirID,
				Name:     de.Name(),
				Kind:     kind,
				Size:     info.Size(),
				Blocks:   blocks,
				ModTime:  info.ModTime(),
				DevID:    devID,
				Inode:    inode,
			}
			select {
			case w.entryCh <- e:
			case <-ctx.Done():
				return
			default:
				if w.opts.Verbose {
					fmt.Fprintf(os.Stderr, "\n[DEBUG] Entry channel full, blocking on: %s\n", childPath)
				}
				select {
				case w.entryCh <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	w.emitDirResult(ctx, work.dirID, work.parentID, fileSize, fileBlocks, fileCount, len(childDirs))

	for i := len(childDirs) - 1; i >= 0; i-- {
		w.enqueueOrStack(ctx, childDirs[i])
		if ctx.Err() != nil {
			return
		}
	}
}

func (w *Worker) processWork(ctx context.Context, work dirWork) {
	w.ProcessDirectory(ctx, work)
	newInFlight := atomic.AddInt64(w.inFlight, -1)
	if w.opts.Verbose && newInFlight <= 5 {
		fmt.Fprintf(os.Stderr, "[W%d] DONE-WORK inFlight=%d->%d path=%s\n", w.id, newInFlight+1, newInFlight, work.path)
	}
}

func (w *Worker) enqueueOrStack(ctx context.Context, work dirWork) {
	if ctx.Err() != nil {
		return
	}

	newInFlight := atomic.AddInt64(w.inFlight, 1)
	select {
	case w.dirQueue <- work:
		if w.opts.Verbose && newInFlight%1000 == 0 {
			fmt.Fprintf(os.Stderr, "[W%d] ENQUEUE inFlight=%d queueLen=%d depth=%d\n", w.id, newInFlight, len(w.dirQueue), work.depth)
		}
		return
	default:
		// Queue full: keep work local to avoid deadlock
		w.stack = append(w.stack, work)
		if w.opts.Verbose && len(w.stack)%100 == 1 {
			fmt.Fprintf(os.Stderr, "[W%d] STACK-FULL queueLen=%d stackLen=%d inFlight=%d depth=%d path=%s\n", w.id, len(w.dirQueue), len(w.stack), newInFlight, work.depth, work.path)
		}
	}
}

func (w *Worker) emitDirResult(ctx context.Context, dirID, parentID int64, size, blocks, files int64, childCount int) {
	if ctx.Err() != nil {
		return
	}

	select {
	case w.dirResCh <- rollup.DirResult{
		DirID:      dirID,
		ParentID:   parentID,
		FileSize:   size,
		FileBlocks: blocks,
		FileCount:  files,
		ChildCount: childCount,
	}:
	case <-ctx.Done():
	}
}
