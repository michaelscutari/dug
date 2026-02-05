package rollup

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/michaelscutari/dug/internal/entry"
)

// DirResult summarizes a scanned directory for streaming rollup aggregation.
type DirResult struct {
	Path       string
	Parent     string
	FileSize   int64
	FileBlocks int64
	FileCount  int64
	ChildCount int
}

// Aggregator computes rollups during scan using directory results.
type Aggregator struct {
	roots     map[string]struct{}
	parents   map[string]string
	partial   map[string]*entry.Rollup
	expected  map[string]int
	completed map[string]int
	orphans   map[string]*orphanAgg
}

type orphanAgg struct {
	total entry.Rollup
	count int
}

// NewAggregator creates a streaming rollup aggregator.
func NewAggregator(roots []string) *Aggregator {
	rootSet := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		rootSet[filepath.Clean(root)] = struct{}{}
	}
	return &Aggregator{
		roots:     rootSet,
		parents:   make(map[string]string),
		partial:   make(map[string]*entry.Rollup),
		expected:  make(map[string]int),
		completed: make(map[string]int),
		orphans:   make(map[string]*orphanAgg),
	}
}

// Run consumes directory results and emits completed rollups to out.
func (a *Aggregator) Run(ctx context.Context, in <-chan DirResult, out chan<- entry.Rollup) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-in:
			if !ok {
				if len(a.partial) > 0 {
					return fmt.Errorf("rollup aggregator incomplete: %d directories pending", len(a.partial))
				}
				return nil
			}
			if err := a.handleResult(ctx, res, out); err != nil {
				return err
			}
		}
	}
}

func (a *Aggregator) handleResult(ctx context.Context, res DirResult, out chan<- entry.Rollup) error {
	dir := filepath.Clean(res.Path)
	parent := filepath.Clean(res.Parent)
	if res.Parent == "" {
		parent = ""
	}

	rollup := &entry.Rollup{
		Path:        dir,
		TotalSize:   res.FileSize,
		TotalBlocks: res.FileBlocks,
		TotalFiles:  res.FileCount,
	}

	a.partial[dir] = rollup
	a.parents[dir] = parent
	a.expected[dir] = res.ChildCount

	if orphan, ok := a.orphans[dir]; ok {
		rollup.TotalSize += orphan.total.TotalSize
		rollup.TotalBlocks += orphan.total.TotalBlocks
		rollup.TotalFiles += orphan.total.TotalFiles
		rollup.TotalDirs += orphan.total.TotalDirs
		a.completed[dir] += orphan.count
		delete(a.orphans, dir)
	}

	if a.completed[dir] >= a.expected[dir] {
		return a.markComplete(ctx, dir, out)
	}

	return nil
}

func (a *Aggregator) markComplete(ctx context.Context, dir string, out chan<- entry.Rollup) error {
	for {
		rollup := a.partial[dir]
		parent := a.parents[dir]

		delete(a.partial, dir)
		delete(a.expected, dir)
		delete(a.completed, dir)
		delete(a.parents, dir)

		select {
		case out <- *rollup:
		case <-ctx.Done():
			return ctx.Err()
		}

		if _, isRoot := a.roots[dir]; isRoot || parent == "" {
			return nil
		}

		if parentRollup, ok := a.partial[parent]; ok {
			a.addChildRollup(parentRollup, rollup)
			a.completed[parent]++
			if a.completed[parent] < a.expected[parent] {
				return nil
			}
			dir = parent
			continue
		}

		a.addOrphan(parent, rollup)
		return nil
	}
}

func (a *Aggregator) addChildRollup(parent, child *entry.Rollup) {
	parent.TotalSize += child.TotalSize
	parent.TotalBlocks += child.TotalBlocks
	parent.TotalFiles += child.TotalFiles
	parent.TotalDirs += child.TotalDirs + 1
}

func (a *Aggregator) addOrphan(parent string, child *entry.Rollup) {
	agg := a.orphans[parent]
	if agg == nil {
		agg = &orphanAgg{}
		a.orphans[parent] = agg
	}
	agg.total.TotalSize += child.TotalSize
	agg.total.TotalBlocks += child.TotalBlocks
	agg.total.TotalFiles += child.TotalFiles
	agg.total.TotalDirs += child.TotalDirs + 1
	agg.count++
}
