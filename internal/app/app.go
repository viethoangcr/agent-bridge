// Package app is the composition root for the agent-bridge process. It selects
// the private mock dispatch, loads configuration, owns the listener and PID
// file, and coordinates a bounded staged shutdown.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/childenv"
	"github.com/viethoangcr/agent-bridge/internal/config"
	"github.com/viethoangcr/agent-bridge/internal/filesystem"
	"github.com/viethoangcr/agent-bridge/internal/httpapi"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"
	"github.com/viethoangcr/agent-bridge/internal/process"
	"github.com/viethoangcr/agent-bridge/internal/projectconfig"
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
// dispatch methods the server consumes, including SSE subscription and
// lifecycle deletion so every registered ACP route is satisfied at compile
// time.
type acpService interface {
	acpLifecycle
	Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error)
	LivePID(serverID string) (int, bool)
	Subscribe(ctx context.Context, serverID string, after int64) (acpproxy.Subscription, error)
	Delete(ctx context.Context, serverID string) error
}

// IO is the process input/output surface supplied by the command entrypoint.
// Tests inject buffers so the application never writes to the real process
// streams directly.
type IO struct {
	// Stdin is read by the private mock agent. Run borrows it for the run's
	// duration and does not close it.
	Stdin io.Reader
	// Stdout is written by the private mock agent. Run borrows it for the run's
	// duration and does not close it.
	Stdout io.Writer
	// Stderr receives structured logs. Run borrows it for the run's duration,
	// does not close it, and requires it to support concurrent writer use.
	Stderr io.Writer
}

// Server timeouts. There is deliberately no global WriteTimeout because SSE
// responses may stream longer than any finite budget.
const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 60 * time.Second
)

// defaultShutdownGrace is the single absolute budget for the whole staged
// shutdown in production.
const defaultShutdownGrace = 10 * time.Second

// options carries the composition seams Run threads through the process. It is
// private so Run stays the only public entry point; tests inject deterministic
// values per run instead of mutating package state, which keeps concurrent runs
// independent.
type options struct {
	newACPProxy   func(*acpstore.Store, config.Config, *slog.Logger) acpService
	shutdownGrace time.Duration
}

// defaultOptions returns the production seams.
func defaultOptions() options {
	return options{
		newACPProxy:   newACPProxy,
		shutdownGrace: defaultShutdownGrace,
	}
}

// newACPProxy constructs the ACP lifecycle owner. It resolves the current
// executable and the configured agent commands, deferring per-agent LookPath
// resolution to the acpruntime resolver.
func newACPProxy(store *acpstore.Store, cfg config.Config, logger *slog.Logger) acpService {
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

// Run starts the agent bridge and blocks until ctx is canceled or a failure
// occurs. Cancellation performs bounded cleanup and returns nil; startup and
// unexpected serving failures are returned. The private mock dispatch is checked
// before any HTTP configuration, listener, or PID-file work, so mock mode never
// binds a port or creates a PID file.
func Run(ctx context.Context, getenv func(string) string, io IO) error {
	return run(ctx, getenv, io, defaultOptions())
}

// run is Run with injected composition seams.
func run(ctx context.Context, getenv func(string) string, io IO, opts options) error {
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

	// One process-wide mutation mutex serializes every bridge-originated
	// filesystem and config mutation. HOME is captured once from the injected
	// environment so the filesystem and config services share one relative root.
	mutations := &sync.Mutex{}
	files, err := filesystem.New(getenv("HOME"), mutations)
	if err != nil {
		return fmt.Errorf("constructing filesystem service: %w", err)
	}
	projectConfig := projectconfig.New(files, mutations)

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

	proxy := opts.newACPProxy(store, cfg, logger)
	proxy.StartReaper()

	processManager := process.NewManager(childenv.Sanitized(os.Environ()), startupCWD)

	handler := httpapi.NewServer(httpapi.Dependencies{
		Token:     cfg.Token,
		Log:       logger,
		ACP:       proxy,
		ACPStore:  store,
		Processes: processManager,
		Files:     files,
		Config:    projectConfig,
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

	if err := serve(ctx, listener, srv, pre, post, logger, cfg.PIDFile, opts.shutdownGrace); err != nil {
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
