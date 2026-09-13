package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

// registerShutdown wires the staged shutdown: the pre-drain hook stops the
// reaper, closes subscriptions, blocks new leases, and signal-and-waits every
// runtime so long-lived handlers cannot stall HTTP drain. The post-drain hooks
// idempotently confirm runtime completion (registered last, so it runs before
// the run-reverse-order database close) and then checkpoint-close the store.
func registerShutdown(pre, post *lifecycle.Registry, closeDB lifecycle.Cleanup, proxy acpLifecycle) error {
	if err := post.Add("database", closeDB); err != nil {
		return err
	}
	if err := pre.Add("acp-pre-drain", proxy.Shutdown); err != nil {
		return err
	}
	if err := post.Add("acp-confirm", proxy.Confirm); err != nil {
		return err
	}
	return nil
}

// processLifecycle is the process manager's staged-shutdown surface: the
// pre-drain blocker rejects new starts/runs, and the shutdown hook kills and
// waits for every process group and pump.
type processLifecycle interface {
	BlockNew(context.Context) error
	Shutdown(context.Context) error
}

// registerProcessShutdown wires the process manager's staged hooks. The
// blocker is registered last so it runs first in the reverse-order pre-drain
// stage, before the killer.
func registerProcessShutdown(pre *lifecycle.Registry, manager processLifecycle) error {
	if err := pre.Add("process-shutdown", manager.Shutdown); err != nil {
		return err
	}
	if err := pre.Add("process-block", manager.BlockNew); err != nil {
		return err
	}
	return nil
}

// httpShutdowner is the subset of *http.Server the staged shutdown uses. It
// lets tests inject a fake that observes stage ordering and budget expiry.
type httpShutdowner interface {
	Shutdown(context.Context) error
	Close() error
}

// httpServer is the full server surface serve drives: Serve to accept
// connections plus the staged-shutdown primitives. It is an interface so tests
// can inject a fake that observes handler drain on every exit path.
type httpServer interface {
	httpShutdowner
	Serve(net.Listener) error
}

// serve runs the HTTP server until ctx is canceled or Serve fails for a reason
// other than the expected shutdown close. Every exit runs the pre-drain ACP
// shutdown (stop reaper, close subscriptions, signal-and-wait runtimes) and
// then the shared post-drain cleanup under one absolute deadline, always
// closing the store before removing the PID file. It returns nil after bounded
// best effort; only an unexpected Serve failure is surfaced.
func serve(ctx context.Context, ln net.Listener, srv httpServer, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string, grace time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		// Listener acceptance already stopped because Serve returned. Run the
		// same staged drain as cancellation: pre-drain, HTTP Shutdown waiting
		// for handlers, post-drain confirm/commit/checkpoint/close, PID removal
		// last, all under one absolute deadline.
		drain(ctx, ln, srv, pre, post, logger, pidFile, grace)
		if err != nil && !isNormalClose(err) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	drain(ctx, ln, srv, pre, post, logger, pidFile, grace)
	return nil
}

// closePostAndRemovePID runs the post-drain registry (which checkpoint-closes
// the database) under deadline and removes the PID file only after it
// completes. The cancellation drain and the unexpected serve-error path both
// funnel through it, so the PID file can never outlive the store.
func closePostAndRemovePID(parent context.Context, post *lifecycle.Registry, logger *slog.Logger, deadline time.Time, pidFile string) {
	postCtx, cancelPost := context.WithDeadline(parent, deadline)
	postErr := post.Shutdown(postCtx)
	cancelPost()
	if postErr != nil {
		logger.Error("post-drain cleanup", "error", postErr)
	}
	removePIDFileLogged(logger, pidFile)
}

// shutdownPre runs the pre-drain stage under the shared absolute deadline on
// every server-exit path: it closes subscriptions, stops the reaper, and
// signal-and-waits every runtime so live processes never outlive the store.
// The deadline is never reset; failures are logged and never surfaced.
func shutdownPre(parent context.Context, deadline time.Time, pre *lifecycle.Registry, logger *slog.Logger) {
	preCtx, cancelPre := context.WithDeadline(parent, deadline)
	preErr := pre.Shutdown(preCtx)
	cancelPre()
	if preErr != nil {
		logger.Error("pre-drain cleanup", "error", preErr)
	}
}

// drain performs the cancellation-driven staged shutdown against one absolute
// deadline. The run ctx is already canceled, so every stage is derived from a
// non-canceled parent carrying that same deadline; each stage gets a fresh
// context with the decreasing remaining budget, never a reset one. Cleanup
// failures are logged and never surfaced: a signal exits 0 after bounded best
// effort. The PID file is removed last.
func drain(ctx context.Context, ln io.Closer, srv httpShutdowner, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string, grace time.Duration) {
	deadline := time.Now().Add(grace)
	parent := context.WithoutCancel(ctx)

	// 1. Stop accepting new connections.
	if err := ln.Close(); err != nil && !isNormalClose(err) {
		logger.Error("closing listener", "error", err)
	}

	// 2. Pre-drain: close streams, stop reapers, signal and wait processes.
	shutdownPre(parent, deadline, pre, logger)

	// 3. Drain now-unblocked handlers. If the absolute budget expires while
	// Shutdown is still draining, force-close remaining connections.
	shutdownCtx, cancelShutdown := context.WithDeadline(parent, deadline)
	shutdownErr := srv.Shutdown(shutdownCtx)
	expired := shutdownCtx.Err() != nil
	cancelShutdown()
	if shutdownErr != nil {
		logger.Error("http shutdown", "error", shutdownErr)
	}
	if expired {
		if err := srv.Close(); err != nil && !isNormalClose(err) {
			logger.Error("forcing http close", "error", err)
		}
	}

	// 4. Post-drain: confirm completion, commit non-process work, close the DB.
	// 5. The PID file goes last.
	closePostAndRemovePID(parent, post, logger, deadline, pidFile)
}

// isNormalClose reports whether err is the expected result of closing the
// listener or HTTP server during shutdown.
func isNormalClose(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed)
}
