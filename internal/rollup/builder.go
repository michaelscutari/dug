package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
)

// Builder computes directory rollups bottom-up.
type Builder struct {
	db       *sql.DB
	cache    map[string]*entry.Rollup
	progress ProgressFunc
}

// ProgressFunc reports rollup progress.
type ProgressFunc func(done, total int64, depth, maxDepth int)

// NewBuilder creates a new rollup builder.
func NewBuilder(db *sql.DB) *Builder {
	return &Builder{
		db:    db,
		cache: make(map[string]*entry.Rollup),
	}
}

// SetProgressFunc sets a callback for rollup progress updates.
func (b *Builder) SetProgressFunc(f ProgressFunc) {
	b.progress = f
}

// Build computes rollups for all directories, processing from deepest to shallowest.
func (b *Builder) Build(ctx context.Context) error {
	// Get max depth
	var maxDepth int
	row := b.db.QueryRow(`SELECT COALESCE(MAX(depth), 0) FROM entries WHERE kind = 1`)
	if err := row.Scan(&maxDepth); err != nil {
		return fmt.Errorf("failed to get max depth: %w", err)
	}

	// Get total directory count for progress.
	var totalDirs int64
	if err := b.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE kind = 1`).Scan(&totalDirs); err != nil {
		return fmt.Errorf("failed to count directories: %w", err)
	}

	// Start transaction for all rollup writes
	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statements
	childFilesStmt, err := tx.Prepare(`
		SELECT COALESCE(SUM(size), 0), COALESCE(SUM(blocks), 0), COUNT(*)
		FROM entries
		WHERE parent = ? AND kind = 0
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare child files query: %w", err)
	}
	defer childFilesStmt.Close()

	childDirsStmt, err := tx.Prepare(`
		SELECT path FROM entries WHERE parent = ? AND kind = 1
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare child dirs query: %w", err)
	}
	defer childDirsStmt.Close()

	insertStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO rollups (path, total_size, total_blocks, total_files, total_dirs)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer insertStmt.Close()

	// Process directories from deepest to shallowest
	var processedDirs int64
	lastUpdate := time.Now()
	for depth := maxDepth; depth >= 0; depth-- {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get all directories at this depth
		rows, err := tx.Query(`SELECT path FROM entries WHERE kind = 1 AND depth = ?`, depth)
		if err != nil {
			return fmt.Errorf("failed to query directories at depth %d: %w", depth, err)
		}

		var dirs []string
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan directory path: %w", err)
			}
			dirs = append(dirs, path)
		}
		rows.Close()

		// Process each directory at this depth
		for _, dirPath := range dirs {
			rollup, err := b.computeRollup(dirPath, childFilesStmt, childDirsStmt)
			if err != nil {
				return fmt.Errorf("failed to compute rollup for %s: %w", dirPath, err)
			}

			b.cache[dirPath] = rollup

			if _, err := insertStmt.Exec(rollup.Path, rollup.TotalSize, rollup.TotalBlocks, rollup.TotalFiles, rollup.TotalDirs); err != nil {
				return fmt.Errorf("failed to insert rollup for %s: %w", dirPath, err)
			}

			processedDirs++
			if b.progress != nil {
				if processedDirs == totalDirs || processedDirs%2048 == 0 {
					now := time.Now()
					if processedDirs == totalDirs || now.Sub(lastUpdate) > 200*time.Millisecond {
						b.progress(processedDirs, totalDirs, depth, maxDepth)
						lastUpdate = now
					}
				}
			}
		}
	}

	if b.progress != nil && totalDirs == 0 {
		b.progress(0, 0, 0, maxDepth)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollups: %w", err)
	}

	return nil
}

func (b *Builder) computeRollup(dirPath string, childFilesStmt, childDirsStmt *sql.Stmt) (*entry.Rollup, error) {
	rollup := &entry.Rollup{Path: dirPath}

	// Get direct child files
	var fileSize, fileBlocks, fileCount int64
	if err := childFilesStmt.QueryRow(dirPath).Scan(&fileSize, &fileBlocks, &fileCount); err != nil {
		return nil, err
	}

	rollup.TotalSize = fileSize
	rollup.TotalBlocks = fileBlocks
	rollup.TotalFiles = fileCount

	// Get child directories and add their rollups
	rows, err := childDirsStmt.Query(dirPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var childDirCount int64
	for rows.Next() {
		var childPath string
		if err := rows.Scan(&childPath); err != nil {
			return nil, err
		}

		childDirCount++

		// Get cached rollup for child directory (should exist since we process bottom-up)
		if childRollup, ok := b.cache[childPath]; ok {
			rollup.TotalSize += childRollup.TotalSize
			rollup.TotalBlocks += childRollup.TotalBlocks
			rollup.TotalFiles += childRollup.TotalFiles
			rollup.TotalDirs += childRollup.TotalDirs + 1 // +1 for the child dir itself
		} else {
			rollup.TotalDirs++ // child dir with no rollup (empty or error)
		}
	}

	return rollup, nil
}
