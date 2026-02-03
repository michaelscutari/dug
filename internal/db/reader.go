package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
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
	orderClause := "COALESCE(r.total_size, e.size) DESC"
	switch sortBy {
	case "name":
		orderClause = "e.name ASC"
	case "files":
		orderClause = "COALESCE(r.total_files, CASE WHEN e.kind = 0 THEN 1 ELSE 0 END) DESC"
	case "size":
		orderClause = "COALESCE(r.total_size, e.size) DESC"
	case "blocks", "disk":
		orderClause = "COALESCE(r.total_blocks, e.blocks) DESC"
	}

	query := fmt.Sprintf(`
		SELECT e.path, e.name, e.kind, e.size, e.blocks, e.mtime,
		       COALESCE(r.total_size, e.size) as total_size,
		       COALESCE(r.total_blocks, e.blocks) as total_blocks,
		       COALESCE(r.total_files, CASE WHEN e.kind = 0 THEN 1 ELSE 0 END) as total_files,
		       COALESCE(r.total_dirs, 0) as total_dirs
		FROM entries e
		LEFT JOIN rollups r ON e.path = r.path
		WHERE e.parent = ?
		ORDER BY %s
		LIMIT ?
	`, orderClause)

	rows, err := db.Query(query, parentPath, limit)
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
	var r entry.Rollup
	r.Path = path

	err := db.QueryRow(`
		SELECT total_size, total_blocks, total_files, total_dirs
		FROM rollups WHERE path = ?
	`, path).Scan(&r.TotalSize, &r.TotalBlocks, &r.TotalFiles, &r.TotalDirs)

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

// GetEntry retrieves a single entry by path.
func GetEntry(db *sql.DB, path string) (*entry.Entry, error) {
	var e entry.Entry
	var mtime int64

	err := db.QueryRow(`
		SELECT path, name, parent, kind, size, blocks, mtime, depth, dev_id, inode
		FROM entries WHERE path = ?
	`, path).Scan(&e.Path, &e.Name, &e.Parent, &e.Kind, &e.Size, &e.Blocks, &mtime, &e.Depth, &e.DevID, &e.Inode)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	e.ModTime = time.Unix(mtime, 0)
	return &e, nil
}
