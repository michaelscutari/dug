package entry

import (
	"os"
	"time"
)

// Kind represents the type of filesystem entry.
type Kind uint8

const (
	KindFile    Kind = 0
	KindDir     Kind = 1
	KindSymlink Kind = 2
	KindOther   Kind = 3
)

func (k Kind) String() string {
	switch k {
	case KindFile:
		return "file"
	case KindDir:
		return "dir"
	case KindSymlink:
		return "symlink"
	default:
		return "other"
	}
}

// KindFromMode derives the Kind from an os.FileMode.
func KindFromMode(mode os.FileMode) Kind {
	switch {
	case mode.IsRegular():
		return KindFile
	case mode.IsDir():
		return KindDir
	case mode&os.ModeSymlink != 0:
		return KindSymlink
	default:
		return KindOther
	}
}

// Entry represents a filesystem entry to be stored in the database.
type Entry struct {
	ParentID int64
	Name     string
	Kind     Kind
	Size     int64 // Apparent size (st_size)
	Blocks   int64 // Disk usage in bytes (st_blocks * 512)
	ModTime  time.Time
	DevID    uint64
	Inode    uint64
}

// Dir represents a directory entry stored in the database.
type Dir struct {
	ID       int64
	Path     string
	Name     string
	ParentID int64
	Depth    int
}

// ScanError represents an error encountered during scanning.
type ScanError struct {
	Path    string
	Message string
}

// Rollup represents aggregated statistics for a directory.
type Rollup struct {
	DirID       int64
	TotalSize   int64 // Apparent size
	TotalBlocks int64 // Disk usage
	TotalFiles  int64
	TotalDirs   int64
}

// ScanMeta holds metadata about a scan.
type ScanMeta struct {
	RootPath    string
	StartTime   time.Time
	EndTime     time.Time
	TotalSize   int64 // Apparent size
	TotalBlocks int64 // Disk usage
	FileCount   int64
	DirCount    int64
	ErrorCount  int64
}
