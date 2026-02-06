package rollup

import (
	"context"
	"fmt"

	"github.com/michaelscutari/dug/internal/entry"
)

// DirResult summarizes a scanned directory for streaming rollup aggregation.
type DirResult struct {
	DirID      int64
	ParentID   int64
	FileSize   int64
	FileBlocks int64
	FileCount  int64
	ChildCount int
}

// Aggregator computes rollups during scan using directory results.
type Aggregator struct {
	roots     map[int64]struct{}
	parents   map[int64]int64
	partial   map[int64]*entry.Rollup
	expected  map[int64]int
	completed map[int64]int
	orphans   map[int64]*orphanAgg
}

type orphanAgg struct {
	total entry.Rollup
	count int
}

// NewAggregator creates a streaming rollup aggregator.
func NewAggregator(roots []int64) *Aggregator {
	rootSet := make(map[int64]struct{}, len(roots))
	for _, root := range roots {
		rootSet[root] = struct{}{}
	}
	return &Aggregator{
		roots:     rootSet,
		parents:   make(map[int64]int64),
		partial:   make(map[int64]*entry.Rollup),
		expected:  make(map[int64]int),
		completed: make(map[int64]int),
		orphans:   make(map[int64]*orphanAgg),
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
	dirID := res.DirID
	parentID := res.ParentID
	rollup := &entry.Rollup{
		DirID:       dirID,
		TotalSize:   res.FileSize,
		TotalBlocks: res.FileBlocks,
		TotalFiles:  res.FileCount,
	}

	a.partial[dirID] = rollup
	a.parents[dirID] = parentID
	a.expected[dirID] = res.ChildCount

	if orphan, ok := a.orphans[dirID]; ok {
		rollup.TotalSize += orphan.total.TotalSize
		rollup.TotalBlocks += orphan.total.TotalBlocks
		rollup.TotalFiles += orphan.total.TotalFiles
		rollup.TotalDirs += orphan.total.TotalDirs
		a.completed[dirID] += orphan.count
		delete(a.orphans, dirID)
	}

	if a.completed[dirID] >= a.expected[dirID] {
		return a.markComplete(ctx, dirID, out)
	}

	return nil
}

func (a *Aggregator) markComplete(ctx context.Context, dirID int64, out chan<- entry.Rollup) error {
	for {
		rollup := a.partial[dirID]
		parentID := a.parents[dirID]

		delete(a.partial, dirID)
		delete(a.expected, dirID)
		delete(a.completed, dirID)
		delete(a.parents, dirID)

		select {
		case out <- *rollup:
		case <-ctx.Done():
			return ctx.Err()
		}

		if _, isRoot := a.roots[dirID]; isRoot || parentID == 0 {
			return nil
		}

		if parentRollup, ok := a.partial[parentID]; ok {
			a.addChildRollup(parentRollup, rollup)
			a.completed[parentID]++
			if a.completed[parentID] < a.expected[parentID] {
				return nil
			}
			dirID = parentID
			continue
		}

		a.addOrphan(parentID, rollup)
		return nil
	}
}

func (a *Aggregator) addChildRollup(parent, child *entry.Rollup) {
	parent.TotalSize += child.TotalSize
	parent.TotalBlocks += child.TotalBlocks
	parent.TotalFiles += child.TotalFiles
	parent.TotalDirs += child.TotalDirs + 1
}

func (a *Aggregator) addOrphan(parentID int64, child *entry.Rollup) {
	agg := a.orphans[parentID]
	if agg == nil {
		agg = &orphanAgg{}
		a.orphans[parentID] = agg
	}
	agg.total.TotalSize += child.TotalSize
	agg.total.TotalBlocks += child.TotalBlocks
	agg.total.TotalFiles += child.TotalFiles
	agg.total.TotalDirs += child.TotalDirs + 1
	agg.count++
}
