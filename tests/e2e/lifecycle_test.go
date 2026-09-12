//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// processView is the subset of a managed-process snapshot these tests need.
type processView struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	PID    *int   `json:"pid"`
}

// parseProcState extracts the state byte from a /proc/<pid>/stat line. The
// command name may contain spaces or parentheses, so the state is the first
// field after the final ')'.
func parseProcState(data []byte) (byte, bool) {
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return 0, false
	}
	return data[end+2], true
}

// processState returns the /proc state of pid inside the container and whether
// the process still exists. A missing process reports ok=false.
func processState(t *testing.T, name string, pid int) (byte, bool) {
	t.Helper()
	out, err := docker("exec", name, "cat", "/proc/"+strconv.Itoa(pid)+"/stat")
	if err != nil {
		return 0, false
	}
	return parseProcState(out)
}

// containerPID scans /proc inside the container for the first process whose
// comm equals comm and returns its PID, or 0 when absent.
func containerPID(t *testing.T, name, comm string) int {
	t.Helper()
	script := `for d in /proc/[0-9]*; do if [ "$(cat "$d/comm" 2>/dev/null)" = "$1" ]; then printf '%s\n' "${d##*/}"; exit 0; fi; done; exit 1`
	out, err := docker("exec", name, "sh", "-c", script, "sh", comm)
	if err != nil {
		return 0
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(out)))
	if convErr != nil {
		t.Fatalf("containerPID %q in %s: unparseable output %q", comm, name, out)
	}
	return pid
}

// containerDiagnostics reports the container state and a /proc-derived process
// listing for a failing cleanup assertion.
func containerDiagnostics(name string) string {
	inspect, _ := docker("inspect", "--format", "{{json .State}}", name)
	script := `for d in /proc/[0-9]*; do s=$(cat "$d/stat" 2>/dev/null) || continue; printf '%s %s\n' "${d##*/}" "$s"; done`
	procs, _ := docker("exec", name, "sh", "-c", script)
	return fmt.Sprintf("state: %s\nprocs:\n%s", inspect, procs)
}

// assertProcessGone polls until pid is absent. A process observed in state Z
// is an immediate failure: a zombie is not evidence of cleanup.
func assertProcessGone(t *testing.T, name string, pid int, desc string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		state, ok := processState(t, name, pid)
		if !ok {
			return
		}
		if state == 'Z' {
			t.Fatalf("%s: pid %d in %s is a zombie, not cleaned up\n%s", desc, pid, name, containerDiagnostics(name))
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: pid %d still present in %s\n%s", desc, pid, name, containerDiagnostics(name))
}

// readPIDFile polls for path inside the container and returns the decimal PID
// it holds.
func readPIDFile(t *testing.T, name, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		out, err := docker("exec", name, "cat", path)
		if err == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(out))); convErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("pid file %s never appeared in %s\n%s", path, name, containerDiagnostics(name))
	return 0
}

// startManagedProcess starts one managed process and returns its running
// snapshot.
func startManagedProcess(t *testing.T, c *container, command string, args []string) processView {
	t.Helper()
	body, err := json.Marshal(map[string]any{"command": command, "args": args})
	if err != nil {
		t.Fatalf("marshal managed start: %v", err)
	}
	resp, data := c.request(http.MethodPost, "/v1/processes", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("managed start %s = %d: %s\n%s", command, resp.StatusCode, data, c.diagnostics())
	}
	var view processView
	if err := json.Unmarshal(data, &view); err != nil || view.ID == "" || view.PID == nil {
		t.Fatalf("managed start snapshot = %s (%v)", data, err)
	}
	return view
}

// assertContainerRunning fails when the container is no longer running.
func assertContainerRunning(t *testing.T, name string) {
	t.Helper()
	out, err := docker("inspect", "--format", "{{.State.Running}}", name)
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container %s is not running: %v %s\n%s", name, err, out, containerDiagnostics(name))
	}
}

// copyWorkspace copies the container's /workspace volume into a host temp dir.
func copyWorkspace(t *testing.T, c *container) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := docker("cp", c.name+":/workspace/.", dir); err != nil {
		t.Fatalf("docker cp workspace: %v\n%s", err, out)
	}
	return dir
}

// managedForkArgs returns the argument vector for a leader that stays alive
// while a descendant shares its process group.
func managedForkArgs(pidFile string) []string {
	return []string{"-c", "sleep 300 & echo $! > " + pidFile + "; wait"}
}

// TestDockerIdleReaper proves a nonzero idle TTL reaps the process group while
// durable state survives, and a zero TTL disables reaping entirely.
func TestDockerIdleReaper(t *testing.T) {
	image := buildImage(t)

	t.Run("nonzero-ttl-reaps", func(t *testing.T) {
		const serverID = "reap"
		c := startMock(t, image, uniqueToken("reap"), map[string]string{
			"AGENT_BRIDGE_IDLE_TTL_MS": "1500",
		})

		initialize(t, c, serverID, "mock")
		sessionID, _ := sessionNewResult(t, c, serverID, mockCWD)
		if code, body := prompt(t, c, serverID, sessionID, "idle reaper"); code != http.StatusOK {
			t.Fatalf("prompt before reap = %d: %s\n%s", code, body, c.diagnostics())
		}
		before, code := events(t, c, serverID, "limit=1000")
		if code != http.StatusOK || len(before) == 0 {
			t.Fatalf("events before reap = %d (status %d)\n%s", len(before), code, c.diagnostics())
		}
		live, _ := assertServerStatus(t, c, serverID, "idle", true)
		pid := *live.PID

		waitFor(t, c, 15*time.Second, "idle reap to exited", func() bool {
			view, code := status(t, c, serverID)
			return code == http.StatusOK && view.Status == "exited" && view.PID == nil
		})
		assertProcessGone(t, c.name, pid, "idle-reaped ACP leader")

		after, code := events(t, c, serverID, "limit=1000")
		if code != http.StatusOK || len(after) < len(before) {
			t.Fatalf("events after reap = %d (status %d), want at least %d\n%s", len(after), code, len(before), c.diagnostics())
		}
		if after[0].Seq != before[0].Seq {
			t.Fatalf("reap dropped durable events: first seq %d, want %d", after[0].Seq, before[0].Seq)
		}
	})

	t.Run("zero-ttl-disabled", func(t *testing.T) {
		const serverID = "noreap"
		c := startMock(t, image, uniqueToken("noreap"), map[string]string{
			"AGENT_BRIDGE_IDLE_TTL_MS": "0",
		})

		initialize(t, c, serverID, "mock")
		live, _ := assertServerStatus(t, c, serverID, "idle", true)
		pid := *live.PID

		// The production reaper sweeps every second; a zero TTL must never
		// start it, so the process stays live well past several cadences.
		time.Sleep(2500 * time.Millisecond)

		still, _ := assertServerStatus(t, c, serverID, "idle", true)
		if *still.PID != pid {
			t.Fatalf("live PID changed with reaping disabled: got %d, want %d", *still.PID, pid)
		}
		state, ok := processState(t, c.name, pid)
		if !ok || state == 'Z' {
			t.Fatalf("process %d not alive with a zero idle TTL: state=%c ok=%v\n%s", pid, state, ok, containerDiagnostics(c.name))
		}
	})
}

// TestDockerDeleteKillsProcessGroup starts ACP, managed, and one-shot groups
// whose leaders fork descendants, then tears each down through DELETE, stop,
// kill, and timeout while inspecting /proc. Leaders and descendants must both
// disappear; a zombie is a failure.
func TestDockerDeleteKillsProcessGroup(t *testing.T) {
	image := buildImage(t)
	token := uniqueToken("pgroup")
	acpEnv := map[string]string{
		"AGENT_BRIDGE_IDLE_TTL_MS":            "0",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "1000",
		"AGENT_BRIDGE_CLAUDE_BIN":             "/bin/sh",
		"AGENT_BRIDGE_CLAUDE_ARGS":            `["-c","sleep 300 & echo $! > /workspace/acp-child.pid; cat >/dev/null"]`,
	}

	t.Run("acp-delete", func(t *testing.T) {
		const serverID = "acpgroup"
		c := startMock(t, image, token, acpEnv)

		// The shell fixture never answers initialize, so the request times out
		// while the runtime stays live. Only its existence matters here.
		postACP(t, c, serverID, "claude", rpc("initialize", "initialize", map[string]any{
			"protocolVersion":    1,
			"clientCapabilities": map[string]any{},
		}))
		view, code := status(t, c, serverID)
		if code != http.StatusOK || view.PID == nil {
			t.Fatalf("ACP fixture status = %d %+v, want a live PID\n%s", code, view, c.diagnostics())
		}
		child := readPIDFile(t, c.name, "/workspace/acp-child.pid")

		resp, data := c.request(http.MethodDelete, "/v1/acp/"+serverID, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE %s = %d: %s\n%s", serverID, resp.StatusCode, data, c.diagnostics())
		}
		assertProcessGone(t, c.name, *view.PID, "ACP leader after DELETE")
		assertProcessGone(t, c.name, child, "ACP descendant after DELETE")
	})

	t.Run("managed-stop", func(t *testing.T) {
		c := startMock(t, image, token, map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})
		p := startManagedProcess(t, c, "/bin/sh", managedForkArgs("/workspace/stop-child.pid"))
		child := readPIDFile(t, c.name, "/workspace/stop-child.pid")

		resp, data := c.request(http.MethodPost, "/v1/processes/"+p.ID+"/stop", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stop %s = %d: %s\n%s", p.ID, resp.StatusCode, data, c.diagnostics())
		}
		assertProcessGone(t, c.name, *p.PID, "managed leader after stop")
		assertProcessGone(t, c.name, child, "managed descendant after stop")
	})

	t.Run("managed-kill", func(t *testing.T) {
		c := startMock(t, image, token, map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})
		p := startManagedProcess(t, c, "/bin/sh", managedForkArgs("/workspace/kill-child.pid"))
		child := readPIDFile(t, c.name, "/workspace/kill-child.pid")

		resp, data := c.request(http.MethodPost, "/v1/processes/"+p.ID+"/kill", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("kill %s = %d: %s\n%s", p.ID, resp.StatusCode, data, c.diagnostics())
		}
		assertProcessGone(t, c.name, *p.PID, "managed leader after kill")
		assertProcessGone(t, c.name, child, "managed descendant after kill")
	})

	t.Run("run-timeout", func(t *testing.T) {
		c := startMock(t, image, token, map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})
		body, err := json.Marshal(map[string]any{
			"command":   "/bin/sh",
			"args":      managedForkArgs("/workspace/run-child.pid"),
			"timeoutMs": 800,
		})
		if err != nil {
			t.Fatalf("marshal run: %v", err)
		}
		resp, data := c.request(http.MethodPost, "/v1/processes/run", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("run = %d: %s\n%s", resp.StatusCode, data, c.diagnostics())
		}
		var result struct {
			TimedOut bool `json:"timedOut"`
		}
		if err := json.Unmarshal(data, &result); err != nil || !result.TimedOut {
			t.Fatalf("run result = %s (%v), want timedOut", data, err)
		}
		child := readPIDFile(t, c.name, "/workspace/run-child.pid")
		assertProcessGone(t, c.name, child, "one-shot descendant after timeout")
	})

	// Leader-exit fixtures: the direct group leader exits on its own while a
	// descendant blocks on the inherited stdio pipes. Each owner must SIGKILL
	// the captured negative PGID so pumps/status/capacity release, and PID-1
	// must reap the killed descendant while the container keeps running.

	t.Run("acp-leader-exit", func(t *testing.T) {
		const serverID = "acp-orch"
		c := startMock(t, image, token, map[string]string{
			"AGENT_BRIDGE_IDLE_TTL_MS":            "0",
			"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "1000",
			"AGENT_BRIDGE_CLAUDE_BIN":             "/bin/sh",
			"AGENT_BRIDGE_CLAUDE_ARGS":            `["-c","sleep 300 & echo $! > /workspace/acp-orch.pid; exit 0"]`,
		})

		postACP(t, c, serverID, "claude", rpc("initialize", "initialize", map[string]any{
			"protocolVersion":    1,
			"clientCapabilities": map[string]any{},
		}))
		child := readPIDFile(t, c.name, "/workspace/acp-orch.pid")

		waitFor(t, c, 10*time.Second, "ACP leader-exit status release", func() bool {
			view, code := status(t, c, serverID)
			return code == http.StatusOK && view.Status == "exited" && view.PID == nil
		})
		assertProcessGone(t, c.name, child, "ACP descendant after leader exit")
		assertContainerRunning(t, c.name)
	})

	t.Run("managed-leader-exit", func(t *testing.T) {
		c := startMock(t, image, token, map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})
		p := startManagedProcess(t, c, "/bin/sh", []string{
			"-c", "sleep 300 & echo $! > /workspace/mg-orch.pid; sleep 0.2; exit 0",
		})
		child := readPIDFile(t, c.name, "/workspace/mg-orch.pid")

		waitFor(t, c, 10*time.Second, "managed leader-exit status release", func() bool {
			resp, data := c.request(http.MethodGet, "/v1/processes/"+p.ID, nil)
			if resp.StatusCode != http.StatusOK {
				return false
			}
			var view processView
			return json.Unmarshal(data, &view) == nil && view.Status == "exited"
		})
		assertProcessGone(t, c.name, child, "managed descendant after leader exit")
		assertContainerRunning(t, c.name)
	})

	t.Run("run-leader-exit", func(t *testing.T) {
		c := startMock(t, image, token, map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})
		body, err := json.Marshal(map[string]any{
			"command":   "/bin/sh",
			"args":      []string{"-c", "sleep 300 & echo $! > /workspace/run-orch.pid; exit 0"},
			"timeoutMs": 20000,
		})
		if err != nil {
			t.Fatalf("marshal run: %v", err)
		}
		start := time.Now()
		resp, data := c.request(http.MethodPost, "/v1/processes/run", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("run leader-exit = %d: %s\n%s", resp.StatusCode, data, c.diagnostics())
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("one-shot leader-exit returned after %s; pumps did not release on group kill", elapsed)
		}
		child := readPIDFile(t, c.name, "/workspace/run-orch.pid")
		assertProcessGone(t, c.name, child, "one-shot descendant after leader exit")
		assertContainerRunning(t, c.name)
	})
}

// TestDockerInitReapsOrphans proves the pinned PID-1 init remains PID 1 and
// reaps a deliberately orphaned, group-escaping grandchild while the container
// keeps running.
func TestDockerInitReapsOrphans(t *testing.T) {
	image := buildImage(t)
	c := startMock(t, image, uniqueToken("orphan"), map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"})

	if pid := containerPID(t, c.name, "tini"); pid != 1 {
		t.Fatalf("tini PID = %d, want 1\n%s", pid, containerDiagnostics(c.name))
	}

	// setsid escapes the leader's process group, so the bridge's negative-PGID
	// kill cannot reach the grandchild: only PID-1 subreaper reaping can. The
	// leader blocks until the grandchild has completed setsid and written its
	// PID, so the negative-PGID kill cannot race the detach.
	script := `setsid /bin/sh -c 'echo $$ > /workspace/orphan.pid; sleep 1' >/dev/null 2>&1 < /dev/null & while [ ! -s /workspace/orphan.pid ]; do sleep 0.02; done`
	leader := startManagedProcess(t, c, "/bin/sh", []string{"-c", script})
	orphan := readPIDFile(t, c.name, "/workspace/orphan.pid")
	if orphan == *leader.PID {
		t.Fatalf("orphan PID %d equals the leader PID; setsid did not detach", orphan)
	}

	assertProcessGone(t, c.name, orphan, "orphaned grandchild")

	if pid := containerPID(t, c.name, "tini"); pid != 1 {
		t.Fatalf("tini PID after reaping = %d, want 1", pid)
	}
	out, err := docker("inspect", "--format", "{{.State.Running}}", c.name)
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container exited while reaping an orphan: %v %s\n%s", err, out, containerDiagnostics(c.name))
	}
}

// TestDockerGracefulShutdown exercises the bounded staged SIGTERM shutdown in
// the real container and abrupt SIGKILL recovery on the same volume.
func TestDockerGracefulShutdown(t *testing.T) {
	image := buildImage(t)

	t.Run("term-staged", func(t *testing.T) {
		const serverID = "shutdown"
		token := uniqueToken("shutdown")
		volume := "agent-bridge-e2e-shutdown-vol-" + uniqueSuffix()
		dockerOrFail(t, "volume", "create", volume)
		t.Cleanup(func() {
			if !keep() {
				_, _ = docker("volume", "rm", "-f", volume)
			}
		})
		env := map[string]string{
			"AGENT_BRIDGE_IDLE_TTL_MS": "0",
			"AGENT_BRIDGE_PID_FILE":    "/workspace/bridge.pid",
		}

		c := restartContainer(t, image, token, volume, env, nil)
		c.mustHealthy()

		initialize(t, c, serverID, "mock")
		sessionID, _ := sessionNewResult(t, c, serverID, mockCWD)
		if code, body := prompt(t, c, serverID, sessionID, "before shutdown"); code != http.StatusOK {
			t.Fatalf("prompt before shutdown = %d: %s\n%s", code, body, c.diagnostics())
		}
		before, code := events(t, c, serverID, "limit=1000")
		if code != http.StatusOK || len(before) == 0 {
			t.Fatalf("events before shutdown = %d (status %d)\n%s", len(before), code, c.diagnostics())
		}
		lastSeq := before[len(before)-1].Seq
		assertServerStatus(t, c, serverID, "idle", true)
		// A managed group gives the staged pre-drain real process teardown
		// work. The exact listener/pre-drain/http/post-drain order and
		// PID-file-last rule are asserted deterministically with injected
		// hooks in internal/app's shutdown tests; the direct group cleanup is
		// asserted in TestDockerDeleteKillsProcessGroup.
		startManagedProcess(t, c, "/bin/sh", managedForkArgs("/workspace/sd-child.pid"))

		if out, err := docker("exec", c.name, "cat", "/workspace/bridge.pid"); err != nil || strings.TrimSpace(string(out)) == "" {
			t.Fatalf("PID file missing before shutdown: %v %s", err, out)
		}

		sseCtx, sseCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer sseCancel()
		stream, err := c.subscribe(sseCtx, serverID, "")
		if err != nil {
			t.Fatalf("subscribe before shutdown: %v\n%s", err, c.diagnostics())
		}
		defer stream.close()
		streamClosed := make(chan struct{})
		go func() {
			for {
				if _, nextErr := stream.next(); nextErr != nil {
					close(streamClosed)
					return
				}
			}
		}()

		start := time.Now()
		dockerOrFail(t, "kill", "-s", "TERM", c.name)

		select {
		case <-streamClosed:
		case <-time.After(10 * time.Second):
			t.Fatalf("SSE did not close during pre-drain\n%s", c.diagnostics())
		}

		exitOut := dockerOrFail(t, "wait", c.name)
		elapsed := time.Since(start)
		if code := strings.TrimSpace(string(exitOut)); code != "0" {
			t.Fatalf("container exit code = %q, want 0\n%s", code, c.diagnostics())
		}
		if elapsed > 10*time.Second {
			t.Fatalf("clean shutdown took %s, want within one 10s budget", elapsed)
		}
		// PID removal is last: the file must be gone and the DB must reopen
		// with committed events after the store was checkpointed and closed.
		dir := copyWorkspace(t, c)
		if _, statErr := os.Stat(filepath.Join(dir, "bridge.pid")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("PID file survived shutdown: %v", statErr)
		}
		ctx := context.Background()
		store, openErr := acpstore.Open(ctx, filepath.Join(dir, "agent-bridge.db"))
		if openErr != nil {
			t.Fatalf("reopen checkpointed DB: %v", openErr)
		}
		persisted, eventsErr := store.Events(ctx, serverID, acpstore.EventQuery{})
		if eventsErr != nil {
			t.Fatalf("read committed events: %v", eventsErr)
		}
		_ = store.Close(ctx)
		if len(persisted) < len(before) {
			t.Fatalf("committed events after shutdown = %d, want at least %d", len(persisted), len(before))
		}

		// Restart on the same volume: durable events survive and the stale live
		// server is recovered as exited regardless of WAL file presence.
		second := restartContainer(t, image, token, volume, env, nil)
		second.mustHealthy()
		recovered, _ := assertServerStatus(t, second, serverID, "exited", false)
		if recovered.LastEventSeq < lastSeq {
			t.Fatalf("post-shutdown lastEventSeq = %d, want at least %d", recovered.LastEventSeq, lastSeq)
		}
		afterRestart, code := events(t, second, serverID, "limit=1000")
		if code != http.StatusOK || len(afterRestart) < len(before) {
			t.Fatalf("reopened events = %d (status %d), want at least %d", len(afterRestart), code, len(before))
		}
		if afterRestart[0].Seq != before[0].Seq {
			t.Fatalf("restart dropped events: first seq %d, want %d", afterRestart[0].Seq, before[0].Seq)
		}
	})

	t.Run("sigkill-restart", func(t *testing.T) {
		const serverID = "crash"
		token := uniqueToken("crash")
		volume := "agent-bridge-e2e-crash-vol-" + uniqueSuffix()
		dockerOrFail(t, "volume", "create", volume)
		t.Cleanup(func() {
			if !keep() {
				_, _ = docker("volume", "rm", "-f", volume)
			}
		})
		env := map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "0"}

		c := restartContainer(t, image, token, volume, env, nil)
		c.mustHealthy()
		initialize(t, c, serverID, "mock")
		sessionID, _ := sessionNewResult(t, c, serverID, mockCWD)
		if code, body := prompt(t, c, serverID, sessionID, "before crash"); code != http.StatusOK {
			t.Fatalf("prompt before crash = %d: %s\n%s", code, body, c.diagnostics())
		}
		before, _ := events(t, c, serverID, "limit=1000")

		dockerOrFail(t, "kill", "-s", "KILL", c.name)
		if out, err := docker("wait", c.name); err != nil {
			t.Fatalf("wait after SIGKILL: %v\n%s", err, out)
		}

		second := restartContainer(t, image, token, volume, env, nil)
		second.mustHealthy()
		assertServerStatus(t, second, serverID, "exited", false)
		after, code := events(t, second, serverID, "limit=1000")
		if code != http.StatusOK || len(after) < len(before) {
			t.Fatalf("events after crash restart = %d (status %d), want at least %d", len(after), code, len(before))
		}
		if after[0].Seq != before[0].Seq {
			t.Fatalf("crash restart dropped events: first seq %d, want %d", after[0].Seq, before[0].Seq)
		}
	})
}
