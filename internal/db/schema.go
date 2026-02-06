package db

import (
	"database/sql"
	"fmt"
	"os"
)

const dirsTableDDL = `
CREATE TABLE IF NOT EXISTS dirs (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    parent_id INTEGER,
    depth INTEGER NOT NULL
);
`

const entriesTableDDL = `
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind INTEGER NOT NULL,
    size INTEGER NOT NULL,
    blocks INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    dev_id INTEGER NOT NULL,
    inode INTEGER NOT NULL
);
`

const rollupsTableDDL = `
CREATE TABLE IF NOT EXISTS rollups (
    dir_id INTEGER PRIMARY KEY,
    total_size INTEGER NOT NULL,
    total_blocks INTEGER NOT NULL,
    total_files INTEGER NOT NULL,
    total_dirs INTEGER NOT NULL
);
`

const scanMetaTableDDL = `
CREATE TABLE IF NOT EXISTS scan_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    root_path TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER,
    total_size INTEGER DEFAULT 0,
    total_blocks INTEGER DEFAULT 0,
    file_count INTEGER DEFAULT 0,
    dir_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0
);
`

const scanErrorsTableDDL = `
CREATE TABLE IF NOT EXISTS scan_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    message TEXT NOT NULL
);
`

const dirsPathIndexDDL = `CREATE UNIQUE INDEX IF NOT EXISTS idx_dirs_path ON dirs(path);`
const dirsParentIndexDDL = `CREATE INDEX IF NOT EXISTS idx_dirs_parent ON dirs(parent_id);`
const entriesParentIndexDDL = `CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_id);`
const rollupsSizeIndexDDL = `CREATE INDEX IF NOT EXISTS idx_rollups_size ON rollups(total_size DESC);`
const rollupsBlocksIndexDDL = `CREATE INDEX IF NOT EXISTS idx_rollups_blocks ON rollups(total_blocks DESC);`
const entriesParentSizeIndexDDL = `CREATE INDEX IF NOT EXISTS idx_entries_parent_size ON entries(parent_id, size DESC);`
const entriesParentBlocksIndexDDL = `CREATE INDEX IF NOT EXISTS idx_entries_parent_blocks ON entries(parent_id, blocks DESC);`

// InitSchema creates all tables in the database.
func InitSchema(db *sql.DB) error {
	ddls := []string{
		dirsTableDDL,
		entriesTableDDL,
		rollupsTableDDL,
		scanMetaTableDDL,
		scanErrorsTableDDL,
	}

	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("failed to execute DDL: %w", err)
		}
	}

	return nil
}

// ApplyWritePragmas configures SQLite for optimal write performance during ingestion.
func ApplyWritePragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000", // 64MB cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB mmap
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to apply pragma %q: %w", pragma, err)
		}
	}

	return nil
}

// ApplyReadPragmas configures SQLite for optimal read performance.
func ApplyReadPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456",
		"PRAGMA query_only = ON",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to apply pragma %q: %w", pragma, err)
		}
	}

	// journal_mode requires write access; best-effort for read-only sessions
	if _, err := db.Exec("PRAGMA journal_mode = DELETE"); err != nil {
		return nil
	}

	return nil
}

// ApplyIndexPragmas configures SQLite for index builds.
// When diskTemp is true, temp files are stored on disk to reduce RAM usage.
func ApplyIndexPragmas(db *sql.DB, diskTemp bool, tmpDir string) error {
	if tmpDir != "" {
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			return fmt.Errorf("failed to create sqlite temp dir: %w", err)
		}
		if err := os.Setenv("SQLITE_TMPDIR", tmpDir); err != nil {
			return fmt.Errorf("failed to set SQLITE_TMPDIR: %w", err)
		}
	}

	pragma := "PRAGMA temp_store = MEMORY"
	if diskTemp {
		pragma = "PRAGMA temp_store = FILE"
	}
	if _, err := db.Exec(pragma); err != nil {
		return fmt.Errorf("failed to set temp_store: %w", err)
	}

	return nil
}

// BuildIndexes creates indexes after the initial data load for better performance.
func BuildIndexes(db *sql.DB) error {
	indexes := []string{
		dirsPathIndexDDL,
		dirsParentIndexDDL,
		entriesParentIndexDDL,
		rollupsSizeIndexDDL,
		rollupsBlocksIndexDDL,
		entriesParentSizeIndexDDL,
		entriesParentBlocksIndexDDL,
	}

	for _, idx := range indexes {
		if _, err := db.Exec(idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// Finalize prepares the database for read-only access.
func Finalize(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA optimize"); err != nil {
		return fmt.Errorf("failed to optimize: %w", err)
	}

	// Switch from WAL to DELETE for better portability
	if _, err := db.Exec("PRAGMA journal_mode = DELETE"); err != nil {
		return fmt.Errorf("failed to set journal mode: %w", err)
	}

	return nil
}
