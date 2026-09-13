//go:build e2e

package e2e

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

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
