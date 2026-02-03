# dug

A filesystem profiler for large NFS environments. Scans directory trees, stores metadata in SQLite, and provides a TUI for exploring disk usage.

## Why

Tools like `du` are slow on network filesystems and don't cache results. `dug` scans once, stores everything in SQLite, and lets you browse instantly. The scanner is parallel, memory-bounded, and handles millions of files without breaking a sweat.

## Install

```bash
git clone https://github.com/michaelscutari/dug.git
cd dug
go build ./cmd/dug
```

Requires Go 1.23+.

## Usage

Scan a directory:
```bash
./dug scan --root /path/to/scan --out ./data
```

Browse interactively:
```bash
./dug tui --db ./data/latest.db
```

Query from scripts:
```bash
./dug query --db ./data/latest.db --sort size --limit 20
./dug info --db ./data/latest.db
```

## How it works

1. **Parallel scan** - Worker pool processes directories concurrently, emitting entries to a channel
2. **Batched writes** - Ingester batches inserts (10k at a time) for SQLite performance
3. **Bottom-up rollups** - After scan, computes recursive size/count for every directory
4. **Snapshot management** - Atomic writes, symlink to latest, configurable retention

The scanner stays on one filesystem by default (`--xdev`). Cross-device mounts are skipped.

### Apparent vs Disk size

dug tracks both metrics:

- **Apparent** (`st_size`) - logical file size, what `ls -l` shows
- **Disk** (`st_blocks * 512`) - actual blocks allocated, what `du` shows

These differ for sparse files, compressed filesystems, and small files (block alignment). When investigating quota issues, disk usage is what matters. When asking "how big is this file?", apparent size is usually what you want.

## TUI keybindings

| Key | Action |
|-----|--------|
| `↑/↓` or `j/k` | Navigate |
| `Enter` or `l` | Open directory |
| `Backspace` or `h` | Go back |
| `s` | Sort by apparent size |
| `d` | Sort by disk usage |
| `n` | Sort by name |
| `f` | Sort by file count |
| `q` | Quit |

## Flags

```
scan:
  --root, -r      Directory to scan (default: .)
  --out, -o       Output directory (default: ./data)
  --workers, -w   Parallel workers (default: 8)
  --xdev          Stay on one filesystem (default: true)
  --retention     Snapshots to keep (default: 5)
  --exclude, -e   Regex patterns to exclude (can repeat)
  --max-errors    Stop after N errors (default: 0 = unlimited)

query:
  --db, -d        Path to database (default: ./data/latest.db)
  --path, -p      Directory to query (default: root)
  --sort, -s      Sort by: size, disk, name, files (default: size)
  --limit, -n     Max results (default: 20)
```

Note: `.snapshot` directories are excluded by default (common NFS snapshot dir).
