package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
	"github.com/michaelscutari/dug/internal/pathutil"
)

// DisplayEntry combines entry data with rollup data for display.
type DisplayEntry struct {
	Path        string
	Name        string
	Kind        entry.Kind
	Size        int64 // Apparent size
	Blocks      int64 // Disk usage
	ModTime     time.Time
	TotalSize   int64 // Apparent size (rollup)
	TotalBlocks int64 // Disk usage (rollup)
	TotalFiles  int64
	TotalDirs   int64
}

// LoadChildren loads child entries for a directory with rollup data.
func LoadChildren(db *sql.DB, parentPath, sortBy string, limit int) ([]DisplayEntry, error) {
	parentPath = pathutil.Normalize(parentPath)
	orderClause := "total_size DESC"
	switch sortBy {
	case "name":
		orderClause = "name ASC"
	case "files":
		orderClause = "total_files DESC"
	case "size":
		orderClause = "total_size DESC"
	case "blocks", "disk":
		orderClause = "total_blocks DESC"
	}

	query := fmt.Sprintf(`
		SELECT d.path, d.name, ? as kind, 0 as size, 0 as blocks, 0 as mtime,
		       COALESCE(r.total_size, 0) as total_size,
		       COALESCE(r.total_blocks, 0) as total_blocks,
		       COALESCE(r.total_files, 0) as total_files,
		       COALESCE(r.total_dirs, 0) as total_dirs
		FROM dirs d
		LEFT JOIN rollups r ON r.dir_id = d.id
		WHERE d.parent_id = ?

		UNION ALL

		SELECT (pd.path || '/' || e.name) as path, e.name, e.kind, e.size, e.blocks, e.mtime,
		       e.size as total_size,
		       e.blocks as total_blocks,
		       CASE WHEN e.kind = 0 THEN 1 ELSE 0 END as total_files,
		       0 as total_dirs
		FROM entries e
		JOIN dirs pd ON pd.id = e.parent_id
		WHERE e.parent_id = ?
		ORDER BY %s
		LIMIT ?
	`, orderClause)

	var parentID int64
	cache := getDirCache(db)
	if cache != nil {
		if cachedID, ok := cache.Get(parentPath); ok {
			parentID = cachedID
		} else if err := db.QueryRow(`SELECT id FROM dirs WHERE path = ?`, parentPath).Scan(&parentID); err != nil {
			return nil, fmt.Errorf("parent not found: %w", err)
		} else {
			cache.Set(parentPath, parentID)
		}
	} else if err := db.QueryRow(`SELECT id FROM dirs WHERE path = ?`, parentPath).Scan(&parentID); err != nil {
		return nil, fmt.Errorf("parent not found: %w", err)
	}

	rows, err := db.Query(query, entry.KindDir, parentID, parentID, limit)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var entries []DisplayEntry
	for rows.Next() {
		var e DisplayEntry
		var mtime int64
		if err := rows.Scan(&e.Path, &e.Name, &e.Kind, &e.Size, &e.Blocks, &mtime, &e.TotalSize, &e.TotalBlocks, &e.TotalFiles, &e.TotalDirs); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		e.ModTime = time.Unix(mtime, 0)
		entries = append(entries, e)
	}

	return entries, rows.Err()
}

// GetRollup retrieves rollup data for a specific path.
func GetRollup(db *sql.DB, path string) (*entry.Rollup, error) {
	path = pathutil.Normalize(path)
	var r entry.Rollup
	var dirID int64
	cache := getDirCache(db)
	if cache != nil {
		if cachedID, ok := cache.Get(path); ok {
			dirID = cachedID
		} else if err := db.QueryRow(`SELECT id FROM dirs WHERE path = ?`, path).Scan(&dirID); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		} else {
			cache.Set(path, dirID)
		}
	} else if err := db.QueryRow(`SELECT id FROM dirs WHERE path = ?`, path).Scan(&dirID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	r.DirID = dirID

	err := db.QueryRow(`
		SELECT total_size, total_blocks, total_files, total_dirs
		FROM rollups WHERE dir_id = ?
	`, dirID).Scan(&r.TotalSize, &r.TotalBlocks, &r.TotalFiles, &r.TotalDirs)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// GetScanMeta retrieves scan metadata.
func GetScanMeta(db *sql.DB) (*entry.ScanMeta, error) {
	var m entry.ScanMeta
	var startTime, endTime int64

	err := db.QueryRow(`
		SELECT root_path, start_time, COALESCE(end_time, 0), total_size, total_blocks, file_count, dir_count, error_count
		FROM scan_meta WHERE id = 1
	`).Scan(&m.RootPath, &startTime, &endTime, &m.TotalSize, &m.TotalBlocks, &m.FileCount, &m.DirCount, &m.ErrorCount)

	if err != nil {
		return nil, err
	}

	m.StartTime = time.Unix(startTime, 0)
	if endTime > 0 {
		m.EndTime = time.Unix(endTime, 0)
	}

	return &m, nil
}
