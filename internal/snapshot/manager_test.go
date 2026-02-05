package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/michaelscutari/dug/internal/scan"
)

func TestManagerRunScanCreatesLatestAndRetention(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("hello"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	outDir := t.TempDir()
	mgr := NewManager(outDir, 1)
	opts := scan.DefaultOptions().WithWorkers(1)

	ctx := context.Background()
	firstDB, err := mgr.RunScan(ctx, root, opts)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if _, err := os.Stat(firstDB); err != nil {
		t.Fatalf("first db missing: %v", err)
	}

	latest := filepath.Join(outDir, "latest.db")
	if info, err := os.Lstat(latest); err == nil && (info.Mode()&os.ModeSymlink != 0) {
		resolved, err := filepath.EvalSymlinks(latest)
		if err != nil {
			t.Fatalf("resolve latest: %v", err)
		}
		firstResolved, err := filepath.EvalSymlinks(firstDB)
		if err != nil {
			t.Fatalf("resolve first db: %v", err)
		}
		if resolved != firstResolved {
			t.Fatalf("latest does not point to first db: %s", resolved)
		}
	}

	time.Sleep(1100 * time.Millisecond)

	secondDB, err := mgr.RunScan(ctx, root, opts)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if _, err := os.Stat(secondDB); err != nil {
		t.Fatalf("second db missing: %v", err)
	}

	if _, err := os.Stat(firstDB); err == nil {
		t.Fatalf("expected first db to be pruned")
	}
}
