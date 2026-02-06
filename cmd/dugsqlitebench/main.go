package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	outDir := flag.String("out", ".", "Output directory for temp DB")
	rows := flag.Int("rows", 100000, "Rows to insert")
	batch := flag.Int("batch", 10000, "Batch size per transaction")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir error: %v\n", err)
		os.Exit(1)
	}

	dbPath := filepath.Join(*outDir, fmt.Sprintf(".dugsqlitebench-%d.db", time.Now().UnixNano()))
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db error: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
	}()

	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			fmt.Fprintf(os.Stderr, "pragma error: %v\n", err)
			os.Exit(1)
		}
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS entries (
			path TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			parent TEXT NOT NULL,
			kind INTEGER NOT NULL,
			size INTEGER NOT NULL,
			blocks INTEGER NOT NULL,
			mtime INTEGER NOT NULL,
			depth INTEGER NOT NULL,
			dev_id INTEGER NOT NULL,
			inode INTEGER NOT NULL
		);
	`); err != nil {
		fmt.Fprintf(os.Stderr, "schema error: %v\n", err)
		os.Exit(1)
	}

	stmt, err := db.Prepare(`INSERT OR REPLACE INTO entries (path, name, parent, kind, size, blocks, mtime, depth, dev_id, inode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "prepare error: %v\n", err)
		os.Exit(1)
	}
	defer stmt.Close()

	start := time.Now()
	inserted := 0
	for inserted < *rows {
		tx, err := db.Begin()
		if err != nil {
			fmt.Fprintf(os.Stderr, "begin error: %v\n", err)
			os.Exit(1)
		}
		txStmt := tx.Stmt(stmt)
		n := *batch
		if inserted+n > *rows {
			n = *rows - inserted
		}
		for i := 0; i < n; i++ {
			path := fmt.Sprintf("/bench/%d", inserted+i)
			_, err := txStmt.Exec(path, "file", "/bench", 0, 1234, 4096, 0, 1, 0, 0)
			if err != nil {
				tx.Rollback()
				fmt.Fprintf(os.Stderr, "insert error: %v\n", err)
				os.Exit(1)
			}
		}
		if err := tx.Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "commit error: %v\n", err)
			os.Exit(1)
		}
		inserted += n
	}
	elapsed := time.Since(start)

	fmt.Printf("out=%s rows=%d batch=%d\n", *outDir, *rows, *batch)
	fmt.Printf("total: %v\n", elapsed)
	if elapsed.Seconds() > 0 {
		fmt.Printf("throughput: %.0f rows/sec\n", float64(*rows)/elapsed.Seconds())
	}
}
