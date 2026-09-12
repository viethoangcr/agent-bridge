package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// logData base64-decodes and concatenates the entries for stream (all streams
// when stream is empty).
func logData(t *testing.T, entries []process.LogEntry, stream string) string {
	t.Helper()
	var out []byte
	for _, entry := range entries {
		if stream != "" && entry.Stream != stream {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(entry.Data)
		if err != nil {
			t.Fatalf("decode log data: %v", err)
		}
		out = append(out, raw...)
	}
	return string(out)
}

// decodeEntries decodes a logs response into entries and validates each entry's
// exact key set and base64 encoding.
func decodeEntries(t *testing.T, rec *httptest.ResponseRecorder) []process.LogEntry {
	t.Helper()
	obj := decodeObject(t, rec.Body.Bytes())
	assertExactKeys(t, obj, "entries")

	var rawEntries []map[string]json.RawMessage
	if err := json.Unmarshal(obj["entries"], &rawEntries); err != nil {
		t.Fatalf("decode entries: %v", err)
	}
	for _, raw := range rawEntries {
		assertExactKeys(t, raw, "sequence", "stream", "timestampMs", "data", "encoding")
		var entry process.LogEntry
		if err := json.Unmarshal(mustMarshal(t, raw), &entry); err != nil {
			t.Fatalf("decode entry: %v", err)
		}
		if entry.Encoding != "base64" {
			t.Fatalf("encoding = %q, want base64", entry.Encoding)
		}
	}
	var resp struct {
		Entries []process.LogEntry `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode logs response: %v", err)
	}
	return resp.Entries
}

// waitStreamData polls the logs endpoint until the given stream's decoded bytes
// equal want.
func waitStreamData(t *testing.T, s *Server, id, stream, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+id+"/logs?stream="+stream, "", "")
		if rec.Code == http.StatusOK {
			var resp struct {
				Entries []process.LogEntry `json:"entries"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode logs: %v", err)
			}
			if logData(t, resp.Entries, stream) == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s for %s did not reach %q", stream, id, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProcessLogs(t *testing.T) {
	s, manager := newProcessServer(t)
	snap := startManagedProcess(t, s, `{"command":"/bin/sh","args":["-c","printf stdout-a; printf stderr-b >&2; printf stdout-c"]}`)
	waitProcessExited(t, manager, snap.ID)

	rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	all := decodeEntries(t, rec)
	if len(all) == 0 {
		t.Fatalf("expected log entries")
	}
	if got := logData(t, all, "stdout"); got != "stdout-astdout-c" {
		t.Fatalf("stdout = %q, want stdout-astdout-c", got)
	}
	if got := logData(t, all, "stderr"); got != "stderr-b" {
		t.Fatalf("stderr = %q, want stderr-b", got)
	}

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs?stream=stdout", "", "")
	stdoutEntries := decodeEntries(t, rec)
	if got := logData(t, stdoutEntries, "stdout"); got != "stdout-astdout-c" {
		t.Fatalf("stream=stdout data = %q", got)
	}
	if len(stdoutEntries) > 0 {
		target := "?stream=stdout&since=" + strconv.FormatInt(stdoutEntries[0].Sequence-1, 10) + "&tail=1"
		rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs"+target, "", "")
		last := decodeEntries(t, rec)
		if len(last) != 1 || last[0].Stream != "stdout" || last[0].Sequence != stdoutEntries[len(stdoutEntries)-1].Sequence {
			t.Fatalf("stream+since+tail = %+v, want last stdout entry", last)
		}
	}
	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs?stream=stderr", "", "")
	stderrEntries := decodeEntries(t, rec)
	if got := logData(t, stderrEntries, ""); got != "stderr-b" {
		t.Fatalf("stream=stderr data = %q", got)
	}
	for _, entry := range stderrEntries {
		if entry.Stream != "stderr" {
			t.Fatalf("stream=stderr returned %q entry", entry.Stream)
		}
	}

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs?since="+strconv.FormatInt(all[0].Sequence, 10), "", "")
	for _, entry := range decodeEntries(t, rec) {
		if entry.Sequence <= all[0].Sequence {
			t.Fatalf("since returned sequence %d, want > %d", entry.Sequence, all[0].Sequence)
		}
	}

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs?tail=0", "", "")
	if entries := decodeEntries(t, rec); len(entries) != 0 {
		t.Fatalf("tail=0 returned %d entries, want 0", len(entries))
	}
	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs?tail=1", "", "")
	last := decodeEntries(t, rec)
	if len(last) != 1 || last[0].Sequence != all[len(all)-1].Sequence {
		t.Fatalf("tail=1 = %+v, want last entry sequence %d", last, all[len(all)-1].Sequence)
	}

	for _, target := range []string{"?tail=1&tail=2", "?tail=", "?nope=1", "?stream=bogus", "?since=-1"} {
		rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/"+snap.ID+"/logs"+target, "", "")
		assertProblem(t, rec, http.StatusBadRequest)
	}

	rec = doProcessRequest(t, s, http.MethodGet, "/v1/processes/proc_missing/logs", "", "")
	assertProblem(t, rec, http.StatusNotFound)
}
