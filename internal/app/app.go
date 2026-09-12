// Package app is the composition root for the agent-bridge process. It selects
// the private mock dispatch, loads configuration, owns the listener and PID
// file, and coordinates a bounded staged shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/config"
	"github.com/viethoangcr/agent-bridge/internal/httpapi"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

// IO is the process input/output surface supplied by the command entrypoint.
// Tests inject buffers so the application never writes to the real process
// streams directly.
type IO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// shutdownGrace is the single absolute budget for the whole staged shutdown.
// It is a variable so tests can shrink it without waiting ten real seconds.
var shutdownGrace = 10 * time.Second

// Server timeouts. There is deliberately no global WriteTimeout: future SSE
// responses stream for longer than any finite budget.
const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 60 * time.Second
)

// errMockAgentUnimplemented is the temporary phase boundary returned by the
// private mock dispatch until Phase 02 implements the JSONL loop.
var errMockAgentUnimplemented = errors.New("internal mock agent is not implemented")

// httpShutdowner is the subset of *http.Server the staged shutdown uses. It
// lets tests inject a fake that observes stage ordering and budget expiry.
type httpShutdowner interface {
	Shutdown(context.Context) error
	Close() error
}

// Run starts the agent bridge and blocks until ctx is canceled or a startup or
// runtime error occurs. The private mock dispatch is checked before any HTTP
// configuration, listener, or PID-file work, so mock mode never binds a port or
// creates a PID file.
func Run(ctx context.Context, getenv func(string) string, io IO) error {
	if getenv("AGENT_BRIDGE_INTERNAL_MOCK_AGENT") == "1" {
		return runInternalMockAgent(ctx, io)
	}

	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(io.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	listener, err := net.Listen("tcp", cfg.Address())
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.Address(), err)
	}

	if cfg.PIDFile != "" {
		if err := writePIDFile(cfg.PIDFile); err != nil {
			_ = listener.Close()
			return fmt.Errorf("writing pid file: %w", err)
		}
	}

	handler := httpapi.NewServer(httpapi.Dependencies{Token: cfg.Token, Log: logger}).Handler()
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}

	pre := &lifecycle.Registry{}
	post := &lifecycle.Registry{}

	return serve(ctx, listener, srv, pre, post, logger, cfg.PIDFile)
}

// runInternalMockAgent is the Phase 01 boundary for the private mock agent. The
// JSONL loop arrives in Phase 02; until then it fails instead of silently
// starting the HTTP server.
func runInternalMockAgent(context.Context, IO) error {
	return errMockAgentUnimplemented
}

// serve runs the HTTP server until ctx is canceled or Serve fails for a reason
// other than the expected shutdown close. Cancellation runs the staged drain
// and always returns nil after bounded best effort.
func serve(ctx context.Context, ln net.Listener, srv *http.Server, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		_ = ln.Close()
		removePIDFileLogged(logger, pidFile)
		if err != nil && !isNormalClose(err) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	drain(ctx, ln, srv, pre, post, logger, pidFile)
	return nil
}

// drain performs the cancellation-driven staged shutdown against one absolute
// deadline. The run ctx is already canceled, so every stage is derived from a
// non-canceled parent carrying that same deadline; each stage gets a fresh
// context with the decreasing remaining budget, never a reset one. Cleanup
// failures are logged and never surfaced: a signal exits 0 after bounded best
// effort. The PID file is removed last.
func drain(ctx context.Context, ln io.Closer, srv httpShutdowner, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string) {
	deadline := time.Now().Add(shutdownGrace)
	parent := context.WithoutCancel(ctx)

	// 1. Stop accepting new connections.
	if err := ln.Close(); err != nil && !isNormalClose(err) {
		logger.Error("closing listener", "error", err)
	}

	// 2. Pre-drain: close streams, stop reapers, signal and wait processes.
	preCtx, cancelPre := context.WithDeadline(parent, deadline)
	preErr := pre.Shutdown(preCtx)
	cancelPre()
	if preErr != nil {
		logger.Error("pre-drain cleanup", "error", preErr)
	}

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

	// 4. Post-drain: confirm completion, commit non-process work, close DB.
	postCtx, cancelPost := context.WithDeadline(parent, deadline)
	postErr := post.Shutdown(postCtx)
	cancelPost()
	if postErr != nil {
		logger.Error("post-drain cleanup", "error", postErr)
	}

	// 5. The PID file goes last.
	removePIDFileLogged(logger, pidFile)
}

// writePIDFile creates path exclusively with mode 0600 and writes the decimal
// process id followed by a newline. It fails rather than replacing an existing
// file so a second bridge cannot claim the same PID file.
func writePIDFile(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// removePIDFile deletes path, treating an already-absent file as success.
func removePIDFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removePIDFileLogged removes path when configured, logging any failure.
func removePIDFileLogged(logger *slog.Logger, path string) {
	if path == "" {
		return
	}
	if err := removePIDFile(path); err != nil {
		logger.Error("removing pid file", "error", err)
	}
}

// isNormalClose reports whether err is the expected result of closing the
// listener or HTTP server during shutdown.
func isNormalClose(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed)
}
