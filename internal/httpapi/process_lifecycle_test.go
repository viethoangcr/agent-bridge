package httpapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// TestProcessSnapshotOmittedArgs is the HTTP-visible half of the snapshot
// args contract: an omitted request args array must serialize as [] not null.
func TestProcessSnapshotOmittedArgs(t *testing.T) {
	s, _ := newProcessServer(t)
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", `{"command":"/bin/cat"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"args":[]`) {
		t.Fatalf("snapshot body = %s, want args serialized as []", rec.Body.String())
	}
}

func TestProcessStart(t *testing.T) {
	s, _ := newProcessServer(t)
	dir := t.TempDir()
	body := `{"command":"/bin/sleep","args":["30"],"cwd":` + strconv.Quote(dir) + `}`
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	assertExactKeys(t, decodeObject(t, rec.Body.Bytes()), "id", "command", "args", "cwd", "status", "pid", "createdAtMs")

	var snap process.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.Command != "/bin/sleep" || len(snap.Args) != 1 || snap.Args[0] != "30" {
		t.Fatalf("command/args = %q %v, want /bin/sleep [30]", snap.Command, snap.Args)
	}
	if snap.Cwd != dir {
		t.Fatalf("cwd = %q, want %q", snap.Cwd, dir)
	}
	if snap.Status != process.StatusRunning {
		t.Fatalf("status = %q, want running", snap.Status)
	}
	if snap.PID == nil || *snap.PID <= 1 {
		t.Fatalf("pid = %v, want a live group leader", snap.PID)
	}
	if snap.ExitCode != nil || snap.ExitedAtMs != nil {
		t.Fatalf("running snapshot exposed exit fields: %+v", snap)
	}
}

func TestProcessStartRejectsInvalidAndConflicts(t *testing.T) {
	t.Run("empty command", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", `{"command":""}`)
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("unknown field", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", `{"command":"/bin/sleep","owner":"me"}`)
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("capacity conflict", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", configJSON(t, func(c *process.Config) {
			c.MaxConcurrentProcesses = 1
		}))
		if rec.Code != http.StatusOK {
			t.Fatalf("config status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)
		rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", `{"command":"/bin/sleep","args":["30"]}`)
		assertProblem(t, rec, http.StatusConflict)
	})

	t.Run("spawn failure hides environment", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json",
			`{"command":"/no/such/agent-bridge-binary","env":{"AGENT_BRIDGE_TEST_SECRET":"hunter2"}}`)
		assertProblem(t, rec, http.StatusBadGateway)
		if strings.Contains(rec.Body.String(), "hunter2") {
			t.Fatalf("problem leaked environment value: %s", rec.Body.String())
		}
	})
}

func TestProcessList(t *testing.T) {
	s, _ := newProcessServer(t)
	first := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)
	second := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)

	rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertExactKeys(t, decodeObject(t, rec.Body.Bytes()), "processes")

	var resp struct {
		Processes []process.Snapshot `json:"processes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(resp.Processes) != 2 {
		t.Fatalf("processes = %d, want 2", len(resp.Processes))
	}
	if !sort.SliceIsSorted(resp.Processes, func(i, j int) bool { return resp.Processes[i].ID < resp.Processes[j].ID }) {
		t.Fatalf("processes not sorted by ID: %+v", resp.Processes)
	}
	if resp.Processes[0].ID != first.ID || resp.Processes[1].ID != second.ID {
		t.Fatalf("process IDs = [%s %s], want [%s %s]", resp.Processes[0].ID, resp.Processes[1].ID, first.ID, second.ID)
	}
}

func TestProcessGet(t *testing.T) {
	s, manager := newProcessServer(t)
	running := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)

	rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+running.ID, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("running status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertExactKeys(t, decodeObject(t, rec.Body.Bytes()), "id", "command", "args", "cwd", "status", "pid", "createdAtMs")

	exited := startManagedProcess(t, s, `{"command":"/bin/sh","args":["-c","exit 3"]}`)
	waitProcessExited(t, manager, exited.ID)

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+exited.ID, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("exited status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertExactKeys(t, decodeObject(t, rec.Body.Bytes()), "id", "command", "args", "cwd", "status", "exitCode", "createdAtMs", "exitedAtMs")

	var snap process.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode exited snapshot: %v", err)
	}
	if snap.Status != process.StatusExited {
		t.Fatalf("status = %q, want exited", snap.Status)
	}
	if snap.PID != nil {
		t.Fatalf("exited snapshot exposed pid %v", *snap.PID)
	}
	if snap.ExitCode == nil || *snap.ExitCode != 3 {
		t.Fatalf("exitCode = %v, want 3", snap.ExitCode)
	}
	if snap.ExitedAtMs == nil {
		t.Fatalf("exited snapshot omitted exitedAtMs")
	}

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/proc_missing", "", "")
	assertProblem(t, rec, http.StatusNotFound)
}

func TestProcessStop(t *testing.T) {
	s, _ := newProcessServer(t)
	snap := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)

	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+snap.ID+"/stop", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got process.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode stop snapshot: %v", err)
	}
	if got.Status != process.StatusExited || got.PID != nil {
		t.Fatalf("stop snapshot = %+v, want exited without pid", got)
	}

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+snap.ID+"/stop", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("repeat stop status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/proc_missing/stop", "", "")
	assertProblem(t, rec, http.StatusNotFound)
}

func TestProcessKill(t *testing.T) {
	s, _ := newProcessServer(t)
	snap := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)

	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+snap.ID+"/kill", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got process.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode kill snapshot: %v", err)
	}
	if got.Status != process.StatusExited || got.PID != nil {
		t.Fatalf("kill snapshot = %+v, want exited without pid", got)
	}

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/proc_missing/kill", "", "")
	assertProblem(t, rec, http.StatusNotFound)
}

func TestProcessDelete(t *testing.T) {
	s, _ := newProcessServer(t)
	running := startManagedProcess(t, s, `{"command":"/bin/sleep","args":["30"]}`)

	rec := doProcessRequest(t, s, http.MethodDelete, "/v1/processes/"+running.ID, "", "")
	assertProblem(t, rec, http.StatusConflict)

	doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+running.ID+"/stop", "", "")
	rec = doProcessRequest(t, s, http.MethodDelete, "/v1/processes/"+running.ID, "", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("DELETE body = %q, want empty", rec.Body.String())
	}

	rec = doProcessRequest(t, s, http.MethodDelete, "/v1/processes/proc_missing", "", "")
	assertProblem(t, rec, http.StatusNotFound)
}
