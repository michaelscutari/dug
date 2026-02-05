package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/scan"

	_ "modernc.org/sqlite"
)

// ProgressFunc is called periodically with current scan progress.
type ProgressFunc func(files, dirs, errors int64, totalBytes int64)

// StageFunc is called when scan stage changes.
type StageFunc func(stage string)

// Manager handles the scan lifecycle including locking and retention.
type Manager struct {
	outputDir    string
	retention    int
	lockFile     *os.File
	progressFunc ProgressFunc
	stageFunc    StageFunc
	indexMode    string
	sqliteTmpDir string
}

// NewManager creates a new snapshot manager.
func NewManager(outputDir string, retention int) *Manager {
	return &Manager{
		outputDir: outputDir,
		retention: retention,
	}
}

// SetProgressFunc sets a callback for progress updates during scan.
func (m *Manager) SetProgressFunc(f ProgressFunc) {
	m.progressFunc = f
}

// SetStageFunc sets a callback for scan stage updates.
func (m *Manager) SetStageFunc(f StageFunc) {
	m.stageFunc = f
}

// SetIndexMode sets the index build mode: memory|disk|skip.
func (m *Manager) SetIndexMode(mode string) {
	m.indexMode = mode
}

// SetSQLiteTmpDir sets the temp directory for SQLite during index build.
func (m *Manager) SetSQLiteTmpDir(dir string) {
	m.sqliteTmpDir = dir
}

// RunScan executes a complete scan workflow.
func (m *Manager) RunScan(ctx context.Context, root string, opts *scan.ScanOptions) (string, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(m.outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// Acquire lock
	if err := m.acquireLock(); err != nil {
		return "", fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer m.releaseLock()

	// Create temp database file
	tempPath := filepath.Join(m.outputDir, fmt.Sprintf(".dug-temp-%d.db", time.Now().UnixNano()))
	database, err := sql.Open("sqlite", tempPath)
	if err != nil {
		os.Remove(tempPath)
		return "", fmt.Errorf("failed to create database: %w", err)
	}

	// Initialize schema and pragmas
	if err := db.InitSchema(database); err != nil {
		database.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("failed to initialize schema: %w", err)
	}

	if err := db.ApplyWritePragmas(database); err != nil {
		database.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("failed to apply pragmas: %w", err)
	}

	// Run scan with progress reporting
	scanner := scan.NewScanner(opts)
	if m.stageFunc != nil {
		m.stageFunc("scan")
	}

	// Start progress reporter if callback is set
	progressDone := make(chan struct{})
	if m.progressFunc != nil {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					if p := scanner.Progress(); p != nil {
						m.progressFunc(p.Files, p.Dirs, p.Errors, p.TotalBytes)
					}
				}
			}
		}()
	}

	scanErr := scanner.Run(ctx, root, database)
	close(progressDone)
	if scanErr != nil {
		database.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("scan failed: %w", scanErr)
	}

	// Build indexes
	if m.indexMode == "" {
		m.indexMode = "memory"
	}
	if m.indexMode != "skip" {
		if m.stageFunc != nil {
			m.stageFunc("indexes")
		}
		if err := db.ApplyIndexPragmas(database, m.indexMode == "disk", m.sqliteTmpDir); err != nil {
			database.Close()
			os.Remove(tempPath)
			return "", fmt.Errorf("failed to apply index pragmas: %w", err)
		}
		if err := db.BuildIndexes(database); err != nil {
			database.Close()
			os.Remove(tempPath)
			return "", fmt.Errorf("failed to build indexes: %w", err)
		}
	}

	// Finalize
	if m.stageFunc != nil {
		m.stageFunc("finalize")
	}
	if err := db.Finalize(database); err != nil {
		database.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("failed to finalize database: %w", err)
	}

	database.Close()

	// Atomic rename to final location
	finalName := fmt.Sprintf("dug-%s.db", time.Now().Format("20060102-150405"))
	finalPath := filepath.Join(m.outputDir, finalName)

	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return "", fmt.Errorf("failed to rename database: %w", err)
	}

	// Update latest.db symlink atomically via temp symlink + rename
	latestPath := filepath.Join(m.outputDir, "latest.db")
	tempLink := filepath.Join(m.outputDir, ".latest.db.tmp")
	os.Remove(tempLink) // Clean up any stale temp link
	if err := os.Symlink(finalName, tempLink); err == nil {
		if err := os.Rename(tempLink, latestPath); err != nil {
			os.Remove(tempLink)
			fmt.Fprintf(os.Stderr, "warning: failed to update latest.db symlink: %v\n", err)
		}
	} else {
		fmt.Fprintf(os.Stderr, "warning: failed to create latest.db symlink: %v\n", err)
	}

	// Prune old snapshots
	if err := m.pruneOldSnapshots(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to prune old snapshots: %v\n", err)
	}

	return finalPath, nil
}

func (m *Manager) acquireLock() error {
	lockPath := filepath.Join(m.outputDir, ".dug.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Try to acquire exclusive lock
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return fmt.Errorf("another scan is in progress")
	}

	m.lockFile = f
	return nil
}

func (m *Manager) releaseLock() {
	if m.lockFile != nil {
		syscall.Flock(int(m.lockFile.Fd()), syscall.LOCK_UN)
		m.lockFile.Close()
		m.lockFile = nil
	}
}

func (m *Manager) pruneOldSnapshots() error {
	if m.retention <= 0 {
		return nil
	}

	entries, err := os.ReadDir(m.outputDir)
	if err != nil {
		return err
	}

	// Find all dug-*.db files
	var snapshots []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "dug-") && strings.HasSuffix(e.Name(), ".db") {
			snapshots = append(snapshots, e.Name())
		}
	}

	// Sort by name (which includes timestamp, so chronological)
	sort.Strings(snapshots)

	// Remove oldest if over retention
	for len(snapshots) > m.retention {
		oldPath := filepath.Join(m.outputDir, snapshots[0])
		if err := os.Remove(oldPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", snapshots[0], err)
		}
		snapshots = snapshots[1:]
	}

	return nil
}

// GetLatest returns the path to the latest snapshot.
func (m *Manager) GetLatest() (string, error) {
	latestPath := filepath.Join(m.outputDir, "latest.db")
	resolved, err := filepath.EvalSymlinks(latestPath)
	if err != nil {
		return "", fmt.Errorf("no latest snapshot found: %w", err)
	}
	return resolved, nil
}

// ListSnapshots returns all available snapshots sorted by date.
func (m *Manager) ListSnapshots() ([]string, error) {
	entries, err := os.ReadDir(m.outputDir)
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "dug-") && strings.HasSuffix(e.Name(), ".db") {
			snapshots = append(snapshots, filepath.Join(m.outputDir, e.Name()))
		}
	}

	sort.Strings(snapshots)
	return snapshots, nil
}
