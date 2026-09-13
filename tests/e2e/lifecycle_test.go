//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

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

func assertContainerRunning(t *testing.T, name string) {
	t.Helper()
	out, err := docker("inspect", "--format", "{{.State.Running}}", name)
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container %s is not running: %v %s\n%s", name, err, out, containerDiagnostics(name))
	}
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
