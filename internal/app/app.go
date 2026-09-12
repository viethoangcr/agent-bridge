// Package app is the composition root for the agent-bridge process. It selects
// the private mock dispatch, loads configuration, owns the listener and PID
// file, and coordinates a bounded staged shutdown.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/childenv"
	"github.com/viethoangcr/agent-bridge/internal/config"
	"github.com/viethoangcr/agent-bridge/internal/httpapi"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"
	"github.com/viethoangcr/agent-bridge/internal/process"
)

// acpLifecycle is the ACP proxy's staged-shutdown surface: the pre-drain hook
// terminates runtimes immediately, and the post-drain hook idempotently
// confirms completion before the store is checkpointed and closed.
type acpLifecycle interface {
	StartReaper()
	Shutdown(context.Context) error
	Confirm(context.Context) error
}

// acpService is the composed ACP proxy surface: staged shutdown plus the HTTP
// dispatch methods the server consumes. The concrete *acpproxy.Proxy also
// satisfies the SSE and DELETE consumer interfaces at runtime.
type acpService interface {
	acpLifecycle
	Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error)
	LivePID(serverID string) (int, bool)
}

// newACPProxy constructs the ACP lifecycle owner. It is a package variable so
// tests can inject a deterministic lifecycle owner without spawning processes.
var newACPProxy = func(store *acpstore.Store, cfg config.Config, logger *slog.Logger) acpService {
	executable, err := os.Executable()
	if err != nil {
		executable = ""
	}
	return acpproxy.New(store, acpruntime.Resolver{
		Executable: executable,
		Commands:   cfg.Agents,
		Environ:    os.Environ(),
		LookPath:   exec.LookPath,
	}, cfg.ACPRequestTimeout, cfg.IdleTTL, logger)
}

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

	// Capture the bridge startup directory once; managed processes default to
	// it and child requests cannot replace their sanitized inherited env.
	startupCWD, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolving startup directory: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(io.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	// Open and reconcile durable state before the listener becomes ready so
	// health never serves pre-reconciliation statuses. Every startup failure
	// after opening closes the store before returning.
	store, err := acpstore.Open(ctx, cfg.DBPath)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	if err := store.Reconcile(ctx); err != nil {
		closeStore(ctx, logger, store)
		return fmt.Errorf("reconciling database: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.Address())
	if err != nil {
		closeStore(ctx, logger, store)
		return fmt.Errorf("listening on %s: %w", cfg.Address(), err)
	}

	if cfg.PIDFile != "" {
		if err := writePIDFile(cfg.PIDFile); err != nil {
			_ = listener.Close()
			closeStore(ctx, logger, store)
			return fmt.Errorf("writing pid file: %w", err)
		}
	}

	proxy := newACPProxy(store, cfg, logger)
	proxy.StartReaper()

	processManager := process.NewManager(childenv.Sanitized(os.Environ()), startupCWD)

	handler := httpapi.NewServer(httpapi.Dependencies{
		Token:     cfg.Token,
		Log:       logger,
		ACP:       proxy,
		ACPStore:  store,
		Processes: processManager,
	}).Handler()
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}

	pre := &lifecycle.Registry{}
	post := &lifecycle.Registry{}
	if err := registerShutdown(pre, post, store.Close, proxy); err != nil {
		_ = listener.Close()
		closeStore(ctx, logger, store)
		return fmt.Errorf("registering shutdown hooks: %w", err)
	}
	if err := registerProcessShutdown(pre, processManager); err != nil {
		_ = listener.Close()
		closeStore(ctx, logger, store)
		return fmt.Errorf("registering process shutdown hooks: %w", err)
	}

	if err := serve(ctx, listener, srv, pre, post, logger, cfg.PIDFile); err != nil {
		return err
	}
	return nil
}

// closeStore closes the application database after a startup failure. Cleanup
// errors are logged rather than masking the original failure.
func closeStore(ctx context.Context, logger *slog.Logger, store *acpstore.Store) {
	if err := store.Close(context.WithoutCancel(ctx)); err != nil {
		logger.Error("closing database", "error", err)
	}
}

// runInternalMockAgent runs the private mock agent JSONL loop in place of the
// HTTP server. It never loads HTTP configuration, binds a listener, or creates
// a PID file or database.
func runInternalMockAgent(ctx context.Context, io IO) error {
	return mockagent.Run(ctx, io.Stdin, io.Stdout, io.Stderr)
}

// serve runs the HTTP server until ctx is canceled or Serve fails for a reason
// other than the expected shutdown close. Every exit runs the pre-drain ACP
// shutdown (stop reaper, close subscriptions, signal-and-wait runtimes) and
// then the shared post-drain cleanup under one absolute deadline, always
// closing the store before removing the PID file. It returns nil after bounded
// best effort; only an unexpected Serve failure is surfaced.
func serve(ctx context.Context, ln net.Listener, srv *http.Server, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		_ = ln.Close()
		parent := context.WithoutCancel(ctx)
		deadline := time.Now().Add(shutdownGrace)
		shutdownPre(parent, deadline, pre, logger)
		closePostAndRemovePID(parent, post, logger, deadline, pidFile)
		if err != nil && !isNormalClose(err) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	drain(ctx, ln, srv, pre, post, logger, pidFile)
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
func drain(ctx context.Context, ln io.Closer, srv httpShutdowner, pre, post *lifecycle.Registry, logger *slog.Logger, pidFile string) {
	deadline := time.Now().Add(shutdownGrace)
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
