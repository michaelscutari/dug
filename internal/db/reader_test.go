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

	insertEntry := func(path, parent string, kind entry.Kind, size, blocks int64, depth int) {
		name := filepath.Base(path)
		_, err := database.Exec(
			`INSERT INTO entries (path, name, parent, kind, size, blocks, mtime, depth, dev_id, inode)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			path, name, parent, kind, size, blocks, 0, depth, 0, 0,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}

	insertEntry("/root", "", entry.KindDir, 0, 0, 0)
	insertEntry("/root/dir1", "/root", entry.KindDir, 0, 0, 1)
	insertEntry("/root/file1", "/root", entry.KindFile, 200, 512, 1)
	insertEntry("/root/file2", "/root", entry.KindFile, 50, 512, 1)

	_, err = database.Exec(
		`INSERT INTO rollups (path, total_size, total_blocks, total_files, total_dirs)
		 VALUES (?, ?, ?, ?, ?)`,
		"/root/dir1", 100, 512, 1, 0,
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
