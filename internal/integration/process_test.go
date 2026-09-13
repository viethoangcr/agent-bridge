package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

type processSnapshotView struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	PID      *int   `json:"pid"`
	ExitCode *int   `json:"exitCode"`
}

// processDone reports whether the process is gone or a zombie in /proc.
func processDone(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return true
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return true
	}
	return data[end+2] == 'Z'
}

func (b *bridge) waitProcessDone(pid int) {
	b.t.Helper()
	b.waitFor(5*time.Second, "process group termination", func() bool { return processDone(pid) })
}

func (b *bridge) waitProcessStdout(id, want string) {
	b.t.Helper()
	b.waitFor(5*time.Second, "stdout "+want, func() bool {
		var out struct {
			Entries []struct {
				Data string `json:"data"`
			} `json:"entries"`
		}
		if code := b.getJSON("/v1/processes/"+id+"/logs?stream=stdout", &out); code != http.StatusOK {
			return false
		}
		var decoded []byte
		for _, entry := range out.Entries {
			raw, err := base64.StdEncoding.DecodeString(entry.Data)
			if err != nil {
				return false
			}
			decoded = append(decoded, raw...)
		}
		return string(decoded) == want
	})
}

// TestProcessHTTP drives the process API through the full app composition over
// real HTTP and proves the staged pre-drain/shutdown hooks terminate managed
// groups while the bridge stays up.
func TestProcessHTTP(t *testing.T) {
	t.Run("auth-and-problem-middleware", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{token: "secret"})
		defer b.stop()

		resp := b.do(http.MethodGet, "/v1/processes/config", "", nil)
		body, _ := readAll(resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status = %d, want 401: %s", resp.StatusCode, body)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("unauthorized Content-Type = %q, want application/problem+json", ct)
		}

		resp = b.do(http.MethodGet, "/v1/processes/config", "", map[string]string{"Authorization": "Bearer secret"})
		_, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("authenticated config status = %d, want 200", resp.StatusCode)
		}

		resp = b.do(http.MethodGet, "/v1/processes/nope", "", map[string]string{"Authorization": "Bearer secret"})
		body, _ = readAll(resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown process status = %d, want 404: %s", resp.StatusCode, body)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("not-found Content-Type = %q, want application/problem+json", ct)
		}
	})

	t.Run("managed-run-lifecycle", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		resp := b.do(http.MethodPost, "/v1/processes", `{"command":"/bin/cat"}`, nil)
		body, _ := readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("start status = %d, want 200: %s", resp.StatusCode, body)
		}
		var snap processSnapshotView
		if err := json.Unmarshal(body, &snap); err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
		if snap.ID == "" || snap.PID == nil {
			t.Fatalf("running snapshot = %+v, want id and pid", snap)
		}

		// Input echo round-trips through stdin and the bounded log ring.
		resp = b.do(http.MethodPost, "/v1/processes/"+snap.ID+"/input", `{"data":"hello-integration","encoding":"utf8"}`, nil)
		body, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("input status = %d, want 200: %s", resp.StatusCode, body)
		}
		b.waitProcessStdout(snap.ID, "hello-integration")

		resp = b.do(http.MethodPost, "/v1/processes/"+snap.ID+"/stop", "", nil)
		body, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stop status = %d, want 200: %s", resp.StatusCode, body)
		}
		var stopped processSnapshotView
		if err := json.Unmarshal(body, &stopped); err != nil || stopped.Status != "exited" {
			t.Fatalf("stop snapshot = %+v (%v), want exited", stopped, err)
		}

		resp = b.do(http.MethodGet, "/v1/processes/"+snap.ID, "", nil)
		_, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get status = %d, want 200", resp.StatusCode)
		}

		resp = b.do(http.MethodDelete, "/v1/processes/"+snap.ID, "", nil)
		_, _ = readAll(resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status = %d, want 204", resp.StatusCode)
		}

		resp = b.do(http.MethodPost, "/v1/processes/run", `{"command":"/bin/sleep","args":["30"],"timeoutMs":50}`, nil)
		body, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("run status = %d, want 200: %s", resp.StatusCode, body)
		}
		var run process.RunResult
		if err := json.Unmarshal(body, &run); err != nil {
			t.Fatalf("decode run result: %v", err)
		}
		if !run.TimedOut || run.ExitCode != nil {
			t.Fatalf("run result = %+v, want timedOut with no exitCode", run)
		}

		cfg := process.DefaultConfig()
		cfg.MaxOutputBytes = 2048
		encoded, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		resp = b.do(http.MethodPost, "/v1/processes/config", string(encoded), nil)
		_, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("config status = %d, want 200", resp.StatusCode)
		}
		var got process.Config
		if code := b.getJSON("/v1/processes/config", &got); code != http.StatusOK || got.MaxOutputBytes != 2048 {
			t.Fatalf("config get = %d %+v, want maxOutputBytes 2048", code, got)
		}

		// A managed group that is running when the bridge shuts down must be
		// terminated by the staged pre-drain hook before HTTP drain finishes.
		resp = b.do(http.MethodPost, "/v1/processes", `{"command":"/bin/sleep","args":["30"]}`, nil)
		body, _ = readAll(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("shutdown start status = %d, want 200: %s", resp.StatusCode, body)
		}
		var lingering processSnapshotView
		if err := json.Unmarshal(body, &lingering); err != nil || lingering.PID == nil {
			t.Fatalf("lingering snapshot = %+v (%v), want pid", lingering, err)
		}

		b.stop()
		b.waitProcessDone(*lingering.PID)
	})
}

func readAll(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
