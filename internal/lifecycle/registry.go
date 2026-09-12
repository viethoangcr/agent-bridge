// Package lifecycle provides concurrency-safe registries of cleanup hooks
// invoked during staged application shutdown.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Cleanup is a shutdown hook. It receives the context governing the shutdown
// stage and reports any error encountered while releasing resources.
type Cleanup func(context.Context) error

// Registry stores named cleanup hooks and, on the first Shutdown call, runs
// them once in reverse registration order. Later Add calls are rejected. A
// Registry must not be copied after first use.
type Registry struct {
	mu      sync.Mutex
	hooks   []namedCleanup
	started bool
	done    chan struct{}
	err     error
}

type namedCleanup struct {
	name    string
	cleanup Cleanup
}

// Add registers a cleanup hook under name. It returns an error if cleanup is
// nil or if shutdown has already started.
func (r *Registry) Add(name string, cleanup Cleanup) error {
	if cleanup == nil {
		return fmt.Errorf("lifecycle: nil cleanup %q", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("lifecycle: cannot add %q after shutdown started", name)
	}
	r.hooks = append(r.hooks, namedCleanup{name: name, cleanup: cleanup})
	return nil
}

// Shutdown runs every registered hook exactly once in reverse registration
// order, passing ctx to each. Hooks continue to run after one fails, and the
// joined errors are returned. Concurrent and repeated calls return the same
// shared result and never re-run hooks.
func (r *Registry) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		done := r.done
		r.mu.Unlock()
		<-done
		return r.err
	}
	r.started = true
	r.done = make(chan struct{})
	hooks := r.hooks
	r.hooks = nil
	r.mu.Unlock()

	var errs []error
	for i := len(hooks) - 1; i >= 0; i-- {
		if err := hooks[i].cleanup(ctx); err != nil {
			errs = append(errs, fmt.Errorf("lifecycle: cleanup %q: %w", hooks[i].name, err))
		}
	}
	err := errors.Join(errs...)

	r.mu.Lock()
	r.err = err
	close(r.done)
	r.mu.Unlock()
	return err
}
