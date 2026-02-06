<p align="center">
  <img src="logo.png" alt="dug" width="200">
</p>

<h1 align="center">dug</h1>

<p align="center">
  A filesystem profiler that scans directory trees into SQLite.
</p>

<p align="center">
  <a href="https://github.com/michaelscutari/dug/releases"><img src="https://img.shields.io/github/v/release/michaelscutari/dug?style=flat-square&color=blue" alt="Release"></a>
  <a href="https://goreportcard.com/report/github.com/michaelscutari/dug"><img src="https://goreportcard.com/badge/github.com/michaelscutari/dug?style=flat-square" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="License"></a>
</p>

---

Most disk-usage tools hold the entire file tree in memory. That works until it doesn't — a 50-million-file NFS share will exhaust RAM, crash mid-scan, and lose everything. dug takes a different approach. It streams metadata into a SQLite database as it scans, keeping memory usage constant regardless of filesystem size. The result is a portable `.db` file that anyone can browse, query, or archive — without ever re-scanning.

## Why dug

**Scan once, browse anywhere.** The output is a standard SQLite database. Open it with the built-in TUI, query it with `sqlite3`, or hand it to a colleague. No special tooling required.

**Memory stays flat.** Batched writes and bounded channels mean dug uses the same amount of memory whether it's scanning 1,000 files or 50 million. Other tools scale linearly with file count — dug does not.

**Multiple readers, no coordination.** The database opens in WAL mode with read-only access. Ten people can browse the same scan simultaneously from different terminals. There is no lock, no reload, and no waiting.

**Streaming aggregation.** Directory rollups (total size, file count, disk usage) are computed during the scan as workers complete directories — not in a post-processing pass. If a scan is interrupted, everything already flushed to disk is still usable.

**Built for recurring scans.** Each scan produces a timestamped database. A `latest.db` symlink always points to the newest one. Old snapshots are automatically pruned. Schedule it with cron or SLURM and forget about it.

## Quick Start

```bash
# build
go build -o dug ./cmd/dug

# scan a directory
./dug scan --root /path/to/scan

# browse interactively
./dug tui --db ./data/latest.db
```

## Install

### From source

Requires Go 1.21 or later.

```bash
go install github.com/michaelscutari/dug/cmd/dug@latest
```

### Pre-built binaries

Download from [Releases](https://github.com/michaelscutari/dug/releases). Binaries are available for Linux and macOS (amd64, arm64).

## Commands

### `dug scan`

Scan a directory tree and write the results to a SQLite database.

```bash
dug scan --root /data/shared --out ./scans --workers 16
```

| Flag | Default | Description |
|------|---------|-------------|
| `--root, -r` | `.` | Root directory to scan |
| `--out, -o` | `./data` | Output directory for databases |
| `--workers, -w` | `8` | Concurrent worker goroutines |
| `--xdev` | `true` | Stay on the same filesystem |
| `--retention` | `5` | Snapshots to keep (0 = unlimited) |
| `--exclude, -e` | | Regex patterns to skip |
| `--max-errors` | `0` | Abort after N errors (0 = unlimited) |
| `--index-mode` | `memory` | Index build strategy: `memory`, `disk`, or `skip` |
| `--sqlite-tmp-dir` | | Scratch directory for disk-mode index builds |
| `--progress-interval` | `30s` | Progress output interval for non-TTY environments |
| `--verbose, -v` | `false` | Per-directory debug logging |

Each scan writes a `dug-YYYYMMDD-HHMMSS.db` file and updates the `latest.db` symlink.

### `dug tui`

Browse a scan database interactively.

```bash
dug tui --db ./data/latest.db
```

| Key | Action |
|-----|--------|
| `j/k` or `↑/↓` | Navigate |
| `Enter` or `l/→` | Open directory |
| `Backspace` or `h/←` | Parent directory |
| `s` `d` `n` `f` | Sort by size, disk, name, files |
| `/` | Filter by name |
| `g` / `G` | Jump to top / bottom |
| `q` | Quit |

### `dug query`

Query a scan database from the command line. Designed for scripting and reports.

```bash
dug query --db ./data/latest.db --path /data/shared --sort size --limit 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `--db, -d` | `./data/latest.db` | Database path |
| `--path, -p` | scan root | Directory to list |
| `--sort, -s` | `size` | Sort by: `size`, `disk`, `name`, `files` |
| `--limit, -n` | `20` | Maximum results |

### `dug info`

Print scan metadata — timestamps, file counts, total sizes.

```bash
dug info --db ./data/latest.db
```

## Index Modes

Building indexes after a scan makes queries fast, but the index build itself needs temporary storage. On very large scans, this can spike memory usage. dug gives you control:

- **`memory`** (default) — Index build uses RAM. Fastest option for most scans.
- **`disk`** — Index build uses a scratch directory (`--sqlite-tmp-dir`). Safer for massive filesystems.
- **`skip`** — No indexes built. Fastest scan time, but queries will be slower.

## Output Format

The output is a standard SQLite database. You can query it directly:

```bash
# top 10 directories by size
sqlite3 latest.db "
  SELECT d.path, r.total_size, r.total_files
  FROM rollups r JOIN dirs d ON r.dir_id = d.id
  ORDER BY r.total_size DESC LIMIT 10;
"

# total errors from a scan
sqlite3 latest.db "SELECT error_count FROM scan_meta WHERE id = 1;"
```

### Schema

| Table | Purpose |
|-------|---------|
| `dirs` | Directory tree (id, path, name, parent, depth) |
| `entries` | Individual files and symlinks |
| `rollups` | Aggregated stats per directory (size, blocks, file count, dir count) |
| `scan_meta` | Scan metadata (root, timestamps, totals, error count) |
| `scan_errors` | Sampled permission and I/O errors |

## Scheduling Scans

dug is designed for automated, recurring scans. A nightly job produces a fresh database and prunes old ones — lab members or sysadmins browse the latest snapshot on demand.

**SLURM example:**

```bash
#!/bin/bash
#SBATCH -J dug_scan
#SBATCH --mem=32G
#SBATCH -t 2-00:00:00

SCAN_ROOT="/data/shared"
OUT_DIR="/data/shared/dug"
TMP_DIR="/scratch/${USER}/dug_tmp"

mkdir -p "${TMP_DIR}" "${OUT_DIR}"

dug scan \
  --root "${SCAN_ROOT}" \
  --out "${OUT_DIR}" \
  --workers 16 \
  --index-mode disk \
  --sqlite-tmp-dir "${TMP_DIR}" \
  --progress-interval 30s
```

**Cron example:**

```bash
0 2 * * * /usr/local/bin/dug scan --root /srv/data --out /srv/data/.dug --workers 8
```

Users browse the latest scan without any coordination:

```bash
dug tui --db /data/shared/dug/latest.db
```

## How It Works

dug runs a concurrent scan pipeline that writes directly to SQLite as it discovers files:

1. **Workers** traverse the directory tree in parallel using a hybrid queue-and-stack model that avoids deadlocks on directories with millions of children.
2. **Entries** (files, symlinks) and **directories** are batched and flushed to SQLite in transactions — bounded by batch size or a flush timer, whichever comes first.
3. **Rollups** are computed in a streaming aggregator as workers finish directories. Child results cascade upward without a second pass over the data.
4. **Indexes** are built after the scan completes, with configurable memory or disk-backed temp storage.
5. The database is atomically renamed into place, the `latest.db` symlink is updated, and old snapshots are pruned.

Permission errors on shared filesystems are expected. They are counted and sampled (up to 1,000) without interrupting the scan.

## Comparison

| | dug | ncdu | gdu | duc |
|---|---|---|---|---|
| Data model | SQLite database | In-memory tree | In-memory tree | LMDB key-value store |
| Memory at 50M files | Constant (~200 MB) | 10–25 GB | 10–25 GB | Constant |
| Concurrent readers | Unlimited (WAL) | 1 | 1 | Limited |
| Crash recovery | Partial data preserved | Total loss | Total loss | Partial |
| Output format | Portable `.db` file | Binary export | JSON export | Opaque database |
| Queryable with SQL | Yes | No | No | No |
| Recurring scan support | Built-in (snapshots, retention) | Manual | Manual | Manual |

## License

MIT
