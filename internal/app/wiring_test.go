package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/config"
)

// TestRunMockBypassesStartup proves the private mock dispatch runs the JSONL
// loop before config.Load, listener creation, and PID-file creation: the HTTP
// environment is deliberately invalid, yet the mock answers on stdio. The mock
// never loads HTTP config, binds, or creates a database or its WAL files.
func TestRunMockBypassesStartup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bridge.db")
	pidPath := filepath.Join(dir, "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT": "1",
		"AGENT_BRIDGE_PORT":                "not-a-port",
		"AGENT_BRIDGE_HOST":                "not-a-real-host",
		"AGENT_BRIDGE_PID_FILE":            pidPath,
		"AGENT_BRIDGE_DB":                  dbPath,
	})
	request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}` + "\n"
	var out strings.Builder
	appIO := IO{Stdin: strings.NewReader(request), Stdout: &out, Stderr: io.Discard}

	err := Run(context.Background(), getenv, appIO)
	if err != nil {
		t.Fatalf("Run() = %v, want nil from private mock execution", err)
	}
	if !strings.Contains(out.String(), `"protocolVersion":1`) {
		t.Fatalf("private mock did not execute: stdout = %q", out.String())
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file exists after mock run: stat error = %v", statErr)
	}
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("mock mode created %s: stat error = %v", path, statErr)
		}
	}
}

// TestACPRegistryOrder proves app.Run constructs and starts the ACP proxy and
// runs its pre-drain hook before its post-drain confirmation. The store is
// checkpoint-closed only after confirmation, which the reopen verifies.
func TestACPRegistryOrder(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "bridge.db")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST": host,
		"AGENT_BRIDGE_PORT": port,
		"AGENT_BRIDGE_DB":   dbPath,
	})

	rec := &eventRecorder{}
	proxy := &fakeACPLifecycle{rec: rec}
	opts := testOptions(2 * time.Second)
	opts.newACPProxy = func(*acpstore.Store, config.Config, *slog.Logger) acpService { return proxy }

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- run(ctx, getenv, testIO(), opts) }()
	waitForHealth(t, "http://"+addr+"/v1/health")
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if !proxy.started.Load() {
		t.Fatal("Run did not start the ACP reaper")
	}
	if got := proxy.shutdownCalls.Load(); got != 1 {
		t.Fatalf("pre-drain Shutdown calls = %d, want 1", got)
	}
	if got := proxy.confirmCalls.Load(); got != 1 {
		t.Fatalf("post-drain Confirm calls = %d, want 1", got)
	}
	got := rec.snapshot()
	preIdx := slices.Index(got, "acp-pre")
	confirmIdx := slices.Index(got, "acp-confirm")
	if preIdx < 0 || confirmIdx < 0 || preIdx > confirmIdx {
		t.Fatalf("ACP stage order = %v, want pre-drain before post-drain confirm", got)
	}

	reopened, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("reopen store after shutdown: %v", err)
	}
	_ = reopened.Close(context.Background())
}

// TestRunParallelUsesPerRunSeams proves two concurrently running servers never
// share mutable composition seams: each Run observes its own injected ACP
// proxy. Under package-global seams the parallel subtests race, which -race
// detects.
func TestRunParallelUsesPerRunSeams(t *testing.T) {
	for _, name := range []string{"first", "second"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			addr := freeAddress(t)
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("split address: %v", err)
			}
			dir := t.TempDir()
			getenv := testGetenv(map[string]string{
				"AGENT_BRIDGE_HOST":     host,
				"AGENT_BRIDGE_PORT":     port,
				"AGENT_BRIDGE_DB":       filepath.Join(dir, "bridge.db"),
				"AGENT_BRIDGE_PID_FILE": filepath.Join(dir, "bridge.pid"),
			})

			proxy := &fakeACPLifecycle{}
			opts := testOptions(2 * time.Second)
			opts.newACPProxy = func(*acpstore.Store, config.Config, *slog.Logger) acpService { return proxy }

			ctx, cancel := context.WithCancel(context.Background())
			runErr := make(chan error, 1)
			go func() { runErr <- run(ctx, getenv, testIO(), opts) }()
			waitForHealth(t, "http://"+addr+"/v1/health")
			cancel()
			select {
			case err := <-runErr:
				if err != nil {
					t.Fatalf("Run() = %v, want nil after cancellation", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancellation")
			}

			if !proxy.started.Load() {
				t.Fatal("Run did not start the ACP reaper for its own proxy")
			}
			if got := proxy.shutdownCalls.Load(); got != 1 {
				t.Fatalf("pre-drain Shutdown calls = %d, want 1", got)
			}
			if got := proxy.confirmCalls.Load(); got != 1 {
				t.Fatalf("post-drain Confirm calls = %d, want 1", got)
			}
		})
	}
}

// TestRunWiresConfigServices proves Run builds one shared filesystem and
// project-config stack from the injected HOME and serves the config routes
// through the public handler, writing mode-0600 files.
func TestRunWiresConfigServices(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	home := t.TempDir()
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST": host,
		"AGENT_BRIDGE_PORT": port,
		"AGENT_BRIDGE_DB":   filepath.Join(home, "bridge.db"),
		"HOME":              home,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- run(ctx, getenv, testIO(), testOptions(2*time.Second)) }()
	waitForHealth(t, "http://"+addr+"/v1/health")

	client := &http.Client{Timeout: time.Second}
	target := "http://" + addr + "/v1/config/mcp?directory=project"
	req, err := http.NewRequest(http.MethodPut, target, strings.NewReader(`{"s":{"command":"run"}}`))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	path := filepath.Join(home, "project", ".agent-bridge", "config", "mcp.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %04o, want 0600", info.Mode().Perm())
	}

	getResp, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
