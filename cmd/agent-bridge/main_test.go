package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestRunCancellationExitsZero asserts that a runApp which returns cleanly after
// its context is canceled produces exit code 0 (SIGINT/SIGTERM contract).
func TestRunCancellationExitsZero(t *testing.T) {
	original := runApp
	t.Cleanup(func() { runApp = original })

	entered := make(chan struct{})
	var calls atomic.Int32
	runApp = func(ctx context.Context) error {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- run(ctx) }()

	<-entered
	cancel()

	if code := <-done; code != 0 {
		t.Fatalf("run() = %d, want 0 after context cancellation", code)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("runApp calls = %d, want 1", got)
	}
}

// TestRunStartupErrorExitsNonzero asserts returned startup/runtime errors map to
// a nonzero process exit.
func TestRunStartupErrorExitsNonzero(t *testing.T) {
	original := runApp
	t.Cleanup(func() { runApp = original })

	runApp = func(context.Context) error { return errors.New("startup failed") }

	if code := run(context.Background()); code != 1 {
		t.Fatalf("run() = %d, want 1 when runApp returns an error", code)
	}
}
