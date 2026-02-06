package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/michaelscutari/dug/internal/entry"

	_ "modernc.org/sqlite"
)

func TestLoadChildrenSortsFilesAndDirsBySize(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	if err := InitSchema(database); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	insertDir := func(id int64, path string, parentID int64, depth int) {
		name := filepath.Base(path)
		_, err := database.Exec(
			`INSERT INTO dirs (id, path, name, parent_id, depth) VALUES (?, ?, ?, ?, ?)`,
			id, path, name, parentID, depth,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}

	insertEntry := func(parentID int64, name string, kind entry.Kind, size, blocks int64) {
		_, err := database.Exec(
			`INSERT INTO entries (parent_id, name, kind, size, blocks, mtime, dev_id, inode)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			parentID, name, kind, size, blocks, 0, 0, 0,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	insertDir(1, "/root", 0, 0)
	insertDir(2, "/root/dir1", 1, 1)
	insertEntry(1, "file1", entry.KindFile, 200, 512)
	insertEntry(1, "file2", entry.KindFile, 50, 512)

	_, err = database.Exec(
		`INSERT INTO rollups (dir_id, total_size, total_blocks, total_files, total_dirs)
		 VALUES (?, ?, ?, ?, ?)`,
		2, 100, 512, 1, 0,
	)
	if err != nil {
		t.Fatalf("insert rollup: %v", err)
	}

	children, err := LoadChildren(database, "/root", "size", 10)
	if err != nil {
		t.Fatalf("load children: %v", err)
	}
	if len(children) == 0 {
		t.Fatalf("expected children, got none")
	}
	if children[0].Name != "file1" {
		t.Fatalf("expected largest item first, got %s", children[0].Name)
	}
}
