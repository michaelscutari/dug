package rollup

import (
	"context"
	"testing"

	"github.com/michaelscutari/dug/internal/entry"
)

func TestAggregatorStreamingRollups(t *testing.T) {
	ctx := context.Background()
	in := make(chan DirResult, 4)
	out := make(chan entry.Rollup, 4)

	agg := NewAggregator([]int64{1})
	done := make(chan error, 1)
	go func() {
		done <- agg.Run(ctx, in, out)
	}()

	in <- DirResult{
		DirID:      1,
		ParentID:   0,
		FileSize:   10,
		FileBlocks: 10,
		FileCount:  1,
		ChildCount: 2,
	}
	in <- DirResult{
		DirID:      2,
		ParentID:   1,
		FileSize:   5,
		FileBlocks: 5,
		FileCount:  1,
		ChildCount: 0,
	}
	in <- DirResult{
		DirID:      3,
		ParentID:   1,
		FileSize:   0,
		FileBlocks: 0,
		FileCount:  0,
		ChildCount: 0,
	}
	close(in)

	rollups := make(map[int64]entry.Rollup)
	for r := range out {
		rollups[r.DirID] = r
	}

	if err := <-done; err != nil {
		t.Fatalf("aggregator error: %v", err)
	}

	root := rollups[1]
	if root.TotalSize != 15 || root.TotalBlocks != 15 || root.TotalFiles != 2 || root.TotalDirs != 2 {
		t.Fatalf("unexpected root rollup: %+v", root)
	}

	sub := rollups[2]
	if sub.TotalSize != 5 || sub.TotalBlocks != 5 || sub.TotalFiles != 1 || sub.TotalDirs != 0 {
		t.Fatalf("unexpected sub rollup: %+v", sub)
	}

	empty := rollups[3]
	if empty.TotalSize != 0 || empty.TotalBlocks != 0 || empty.TotalFiles != 0 || empty.TotalDirs != 0 {
		t.Fatalf("unexpected empty rollup: %+v", empty)
	}
}
