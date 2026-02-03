package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/michaelscutari/dug/internal/entry"
)

func TestIngesterCancelsOnMaxErrors(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	if err := InitSchema(database); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entryCh := make(chan entry.Entry, 1)
	errorCh := make(chan entry.ScanError, 1)

	ing := NewIngester(database, entryCh, errorCh, 10, 10, 1, cancel)
	done := make(chan error, 1)
	go func() {
		done <- ing.Run(ctx)
	}()

	errorCh <- entry.ScanError{Path: "/bad", Message: "boom"}
	close(entryCh)
	close(errorCh)

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("expected context cancellation")
	}

	if err := <-done; err != nil {
		t.Fatalf("ingester error: %v", err)
	}

	if ing.ErrorCount() != 1 {
		t.Fatalf("expected error count 1, got %d", ing.ErrorCount())
	}
}
