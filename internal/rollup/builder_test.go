package rollup

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/entry"

	_ "modernc.org/sqlite"
)

func TestBuilderRollup(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	if err := db.InitSchema(database); err != nil {
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
	insertEntry("/root/a", "/root", entry.KindDir, 0, 0, 1)
	insertEntry("/root/a/file1", "/root/a", entry.KindFile, 10, 512, 2)
	insertEntry("/root/a/file2", "/root/a", entry.KindFile, 5, 512, 2)
	insertEntry("/root/b", "/root", entry.KindDir, 0, 0, 1)
	insertEntry("/root/b/file3", "/root/b", entry.KindFile, 20, 1024, 2)

	builder := NewBuilder(database)
	if err := builder.Build(context.Background()); err != nil {
		t.Fatalf("build rollups: %v", err)
	}

	rootA, err := db.GetRollup(database, "/root/a")
	if err != nil || rootA == nil {
		t.Fatalf("rollup /root/a: %v", err)
	}
	if rootA.TotalSize != 15 || rootA.TotalBlocks != 1024 || rootA.TotalFiles != 2 || rootA.TotalDirs != 0 {
		t.Fatalf("unexpected /root/a rollup: %+v", rootA)
	}

	root, err := db.GetRollup(database, "/root")
	if err != nil || root == nil {
		t.Fatalf("rollup /root: %v", err)
	}
	if root.TotalSize != 35 || root.TotalBlocks != 2048 || root.TotalFiles != 3 || root.TotalDirs != 2 {
		t.Fatalf("unexpected /root rollup: %+v", root)
	}
}
