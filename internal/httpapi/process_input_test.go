package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

func TestProcessInput(t *testing.T) {
	s, manager := newProcessServer(t)
	snap := startManagedProcess(t, s, `{"command":"/bin/cat"}`)
	id := snap.ID

	base := []byte("hello-base64")
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json",
		`{"data":`+strconv.Quote(base64.StdEncoding.EncodeToString(base))+`,"encoding":"base64"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("base64 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertBytesWritten(t, rec, len(base))

	text := "hello-utf8"
	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", `{"data":"`+text+`","encoding":"utf8"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("utf8 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertBytesWritten(t, rec, len(text))

	waitStreamData(t, s, id, "stdout", string(base)+text)

	for _, body := range []string{
		`{"data":"!!!","encoding":"base64"}`,
		`{"data":"aGk="}`,
		`{"data":"aGk=","encoding":"hex"}`,
		`{"data":"aGk=","encoding":"base64","extra":1}`,
	} {
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", body)
		assertProblem(t, rec, http.StatusBadRequest)
	}

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/proc_missing/input", "application/json", `{"data":"aGk=","encoding":"base64"}`)
	assertProblem(t, rec, http.StatusNotFound)

	exited := startManagedProcess(t, s, `{"command":"/bin/sh","args":["-c","exit 0"]}`)
	waitProcessExited(t, manager, exited.ID)
	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+exited.ID+"/input", "application/json", `{"data":"aGk=","encoding":"base64"}`)
	assertProblem(t, rec, http.StatusConflict)
}

func TestProcessInputDecodedLimit(t *testing.T) {
	s, _ := newProcessServer(t)
	snap := startManagedProcess(t, s, `{"command":"/bin/cat"}`)
	id := snap.ID

	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", configJSON(t, func(c *process.Config) {
		c.MaxInputBytesPerRequest = 4
	}))
	if rec.Code != http.StatusOK {
		t.Fatalf("config status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", `{"data":"abcd","encoding":"utf8"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("at-limit utf8 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertBytesWritten(t, rec, 4)

	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", `{"data":"YWJjZA==","encoding":"base64"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("at-limit base64 status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	assertBytesWritten(t, rec, 4)

	for _, body := range []string{
		`{"data":"abcde","encoding":"utf8"}`,
		`{"data":"YWJjZGU=","encoding":"base64"}`,
	} {
		rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", body)
		assertProblem(t, rec, http.StatusRequestEntityTooLarge)
	}

	oversized := `{"data":"` + strings.Repeat("a", 2000) + `","encoding":"utf8"}`
	rec = doProcessRequest(t, s, http.MethodPost, "/v1/processes/"+id+"/input", "application/json", oversized)
	assertProblem(t, rec, http.StatusRequestEntityTooLarge)
}

func TestProcessInputEncodedLimitProductionBoundary(t *testing.T) {
	const productionMaxInput = 7340032
	if got, want := inputEncodedBodyLimit(productionMaxInput), int64(9787736); got != want {
		t.Fatalf("inputEncodedBodyLimit(%d) = %d, want %d", productionMaxInput, got, want)
	}
	if got := inputEncodedBodyLimit(productionMaxInput); got > maxProcessJSONBytes {
		t.Fatalf("encoded limit %d exceeds global ceiling %d", got, maxProcessJSONBytes)
	}
}

func assertBytesWritten(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	assertExactKeys(t, decodeObject(t, rec.Body.Bytes()), "bytesWritten")
	var resp struct {
		BytesWritten int `json:"bytesWritten"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bytesWritten: %v", err)
	}
	if resp.BytesWritten != want {
		t.Fatalf("bytesWritten = %d, want %d", resp.BytesWritten, want)
	}
}
