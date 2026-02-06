# dug

Fast disk-usage scans with an interactive TUI and a persistent SQLite database. Designed for large NFS/cluster filesystems where traditional tools stall or run out of memory.

## Cluster‑Focused Features

- **Fast scans at scale**: streaming rollups, no post-scan aggregation pass.
- **Stable memory use**: bounded concurrency and backpressure.
- **TUI + CLI**: browse interactively or query in scripts.
- **Indexing controls**: choose speed vs. safety on large runs.
- **Progress heartbeat**: periodic progress lines for long jobs.

## Output Database

Each scan writes a timestamped DB file:

```
dug-YYYYMMDD-HHMMSS.db
```

`latest.db` is a symlink to the newest scan.

## Indexing Modes

- `--index-mode memory` (default): fastest, uses more RAM.
- `--index-mode disk`: safer for huge scans.
- `--index-mode skip`: no indexes (fastest), but queries slower.

## Progress / Logs

- `--progress-interval 30s` emits periodic progress lines to stderr.
- `--verbose` adds per-directory debug logging.

## Errors

Permission errors are expected on shared filesystems. They are counted in `scan_meta.error_count` and sampled in `scan_errors`.

```bash
sqlite3 ./data/latest.db "SELECT error_count FROM scan_meta WHERE id=1;"
```

## Usage (Secondary)

Build locally:

```bash
go build -o dug ./cmd/dug
```

Scan and open the TUI:

```bash
./dug scan --root /path/to/scan --out ./data
./dug tui --db ./data/latest.db
```

Query without the TUI:

```bash
./dug query --db ./data/latest.db --path /path/to/scan --limit 20
```
