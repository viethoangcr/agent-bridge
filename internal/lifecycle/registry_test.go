package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

func TestShutdownRunsHooksInReverseOrderAndJoinsErrors(t *testing.T) {
	errB := errors.New("b failed")
	errC := errors.New("c failed")

	var (
		mu    sync.Mutex
		order []string
	)
	record := func(name string, err error) lifecycle.Cleanup {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return err
		}
	}

	var r lifecycle.Registry
	if err := r.Add("a", record("a", nil)); err != nil {
		t.Fatalf("Add(a) = %v", err)
	}
	if err := r.Add("b", record("b", errB)); err != nil {
		t.Fatalf("Add(b) = %v", err)
	}
	if err := r.Add("c", record("c", errC)); err != nil {
		t.Fatalf("Add(c) = %v", err)
	}

	got := r.Shutdown(context.Background())
	if got == nil {
		t.Fatal("Shutdown() = nil, want joined error")
	}
	if !errors.Is(got, errB) {
		t.Errorf("Shutdown() = %v, want it to wrap errB", got)
	}
	if !errors.Is(got, errC) {
		t.Errorf("Shutdown() = %v, want it to wrap errC", got)
	}

	want := []string{"c", "b", "a"}
	if !slices.Equal(order, want) {
		t.Errorf("execution order = %v, want %v", order, want)
	}
}

func TestShutdownReturnsNilWhenNoHookFails(t *testing.T) {
	var calls atomic.Int64
	var r lifecycle.Registry
	for _, name := range []string{"a", "b"} {
		if err := r.Add(name, func(context.Context) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Add(%s) = %v", name, err)
		}
	}

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v, want nil", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("cleanup calls = %d, want 2", got)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	sentinel := errors.New("boom")
	var calls atomic.Int64
	var r lifecycle.Registry
	if err := r.Add("hook", func(context.Context) error {
		calls.Add(1)
		return sentinel
	}); err != nil {
		t.Fatalf("Add(hook) = %v", err)
	}

	first := r.Shutdown(context.Background())
	second := r.Shutdown(context.Background())
	if got := calls.Load(); got != 1 {
		t.Fatalf("cleanup calls = %d, want 1", got)
	}
	if !errors.Is(second, sentinel) {
		t.Fatalf("second Shutdown() = %v, want it to wrap sentinel", second)
	}
	if first != second {
		t.Fatalf("Shutdown() results differ: %v vs %v", first, second)
	}
}

func TestShutdownRunsHooksOnceUnderConcurrentCalls(t *testing.T) {
	sentinel := errors.New("boom")
	const hooks = 8
	var calls atomic.Int64
	var r lifecycle.Registry
	for range hooks {
		if err := r.Add("hook", func(context.Context) error {
			calls.Add(1)
			return sentinel
		}); err != nil {
			t.Fatalf("Add(hook) = %v", err)
		}
	}

	const callers = 16
	start := make(chan struct{})
	results := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start
			results[i] = r.Shutdown(context.Background())
		})
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != hooks {
		t.Fatalf("cleanup calls = %d, want %d", got, hooks)
	}
	for i, err := range results {
		if !errors.Is(err, sentinel) {
			t.Fatalf("results[%d] = %v, want it to wrap sentinel", i, err)
		}
		if err != results[0] {
			t.Fatalf("results[%d] = %v, want the same shared result as results[0] = %v", i, err, results[0])
		}
	}
}

func TestAddRejectedAfterShutdown(t *testing.T) {
	var r lifecycle.Registry
	if err := r.Add("first", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Add(first) = %v", err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}

	lateCalled := false
	err := r.Add("late", func(context.Context) error {
		lateCalled = true
		return nil
	})
	if err == nil {
		t.Fatal("Add() after Shutdown = nil, want error")
	}
	if lateCalled {
		t.Fatal("late cleanup was invoked")
	}
}

func TestShutdownPassesContextToHooks(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "value")

	var got any
	var r lifecycle.Registry
	if err := r.Add("ctx", func(c context.Context) error {
		got = c.Value(key{})
		return nil
	}); err != nil {
		t.Fatalf("Add(ctx) = %v", err)
	}
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if got != "value" {
		t.Fatalf("hook context value = %v, want value", got)
	}
}
