package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// newOutputRuntime builds a runtime with just enough state to exercise the
// stdout classifier and pump without spawning a process.
func newOutputRuntime(t *testing.T) (*Runtime, *acpstore.Store) {
	t.Helper()
	store := newRuntimeStore(t)
	r := &Runtime{
		store:    store,
		serverID: "srv",
		log:      testLogger(),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	return r, store
}

// runReadOutput feeds input through the real bounded pump and waits for it to
// reach EOF.
func runReadOutput(t *testing.T, r *Runtime, input []byte) {
	t.Helper()
	pr, pw := io.Pipe()
	r.pumps.Add(1)
	go r.readOutput(pr)
	if _, err := pw.Write(input); err != nil {
		t.Fatalf("write output: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close output writer: %v", err)
	}
	r.pumps.Wait()
}

func eventMethod(e acpstore.Event) string {
	if e.Method == nil {
		return ""
	}
	return *e.Method
}

func listServerEvents(t *testing.T, store *acpstore.Store) []acpstore.Event {
	t.Helper()
	events, err := store.Events(context.Background(), "srv", acpstore.EventQuery{})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return events
}

func TestClassifyOutputKinds(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantOK   bool
		wantKind string
		wantMeth string
	}{
		{name: "agent request string id", raw: `{"jsonrpc":"2.0","id":"a","method":"fs/read_text_file","params":{}}`, wantOK: true, wantKind: "request", wantMeth: "fs/read_text_file"},
		{name: "agent request numeric id", raw: `{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{}}`, wantOK: true, wantKind: "request", wantMeth: "session/request_permission"},
		{name: "notification without id", raw: `{"jsonrpc":"2.0","method":"session/update","params":{}}`, wantOK: true, wantKind: "notification", wantMeth: "session/update"},
		{name: "response with result", raw: `{"jsonrpc":"2.0","id":1,"result":{}}`, wantOK: true, wantKind: "response"},
		{name: "response with null result", raw: `{"jsonrpc":"2.0","id":1,"result":null}`, wantOK: true, wantKind: "response"},
		{name: "response with error", raw: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603}}`, wantOK: true, wantKind: "response"},
		{name: "synthetic invalid stdout is a notification", raw: `{"jsonrpc":"2.0","method":"_adapter/invalid_stdout"}`, wantOK: true, wantKind: "notification", wantMeth: "_adapter/invalid_stdout"},

		{name: "malformed json", raw: `{not json`, wantOK: false},
		{name: "empty object", raw: `{}`, wantOK: false},
		{name: "json null", raw: `null`, wantOK: false},
		{name: "array", raw: `[{"id":1}]`, wantOK: false},
		{name: "scalar", raw: `"hello"`, wantOK: false},
		{name: "id null", raw: `{"jsonrpc":"2.0","method":"x","id":null}`, wantOK: false},
		{name: "id null response", raw: `{"jsonrpc":"2.0","id":null,"result":{}}`, wantOK: false},
		{name: "id boolean", raw: `{"jsonrpc":"2.0","id":true,"method":"x"}`, wantOK: false},
		{name: "id object", raw: `{"jsonrpc":"2.0","id":{},"method":"x"}`, wantOK: false},
		{name: "id array", raw: `{"jsonrpc":"2.0","id":[],"method":"x"}`, wantOK: false},
		{name: "method non-string", raw: `{"jsonrpc":"2.0","id":1,"method":7}`, wantOK: false},
		{name: "method null", raw: `{"jsonrpc":"2.0","id":1,"method":null}`, wantOK: false},
		{name: "method empty", raw: `{"jsonrpc":"2.0","id":1,"method":""}`, wantOK: false},
		{name: "response both result and error", raw: `{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":1}}`, wantOK: false},
		{name: "response neither result nor error", raw: `{"jsonrpc":"2.0","id":1}`, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, ok := classifyOutput([]byte(tt.raw), nil)
			if ok != tt.wantOK {
				t.Fatalf("classifyOutput(%s) ok = %t, want %t", tt.raw, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if out.Kind != tt.wantKind {
				t.Errorf("kind = %q, want %q", out.Kind, tt.wantKind)
			}
			if got := ptrValue(out.Method); got != tt.wantMeth {
				t.Errorf("method = %q, want %q", got, tt.wantMeth)
			}
			if !bytes.Equal(out.Payload, []byte(tt.raw)) {
				t.Errorf("payload = %q, want exact %q", out.Payload, tt.raw)
			}
		})
	}
}

func ptrValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestClassifyOutputSessionScopedInspection(t *testing.T) {
	scoped := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s-1","cwd":"/work","update":{}}}`)
	out, ok := classifyOutput(scoped, nil)
	if !ok {
		t.Fatal("classifyOutput(session/update) = !ok, want notification")
	}
	if out.SessionID == nil || *out.SessionID != "s-1" {
		t.Errorf("sessionId = %v, want s-1", out.SessionID)
	}
	if out.Mutation != nil {
		t.Errorf("notification mutation = %+v, want nil", out.Mutation)
	}

	unrelated := []byte(`{"jsonrpc":"2.0","method":"initialize","params":{"sessionId":"s-2","cwd":"/other"}}`)
	out, ok = classifyOutput(unrelated, nil)
	if !ok {
		t.Fatal("classifyOutput(initialize) = !ok, want request")
	}
	if out.SessionID != nil {
		t.Errorf("unrelated sessionId = %q, want nil", *out.SessionID)
	}
	if out.Mutation != nil {
		t.Errorf("unrelated mutation = %+v, want nil", out.Mutation)
	}

	elicitation := []byte(`{"jsonrpc":"2.0","method":"elicitation/complete","params":{"elicitationId":"e"}}`)
	out, ok = classifyOutput(elicitation, nil)
	if !ok {
		t.Fatal("classifyOutput(elicitation/complete) = !ok, want notification")
	}
	if out.SessionID != nil {
		t.Errorf("elicitation sessionId = %q, want nil", *out.SessionID)
	}
}

func TestClassifyOutputOverLimitSessionID(t *testing.T) {
	over := strings.Repeat("a", maxSessionIDBytes+1)
	raw := fmt.Sprintf(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":%q}}`, over)
	out, ok := classifyOutput([]byte(raw), nil)
	if !ok {
		t.Fatal("classifyOutput(over-limit session) = !ok, want successful classification")
	}
	if out.SessionID != nil {
		t.Errorf("over-limit sessionId = %q, want omitted", *out.SessionID)
	}
	if !bytes.Equal(out.Payload, []byte(raw)) {
		t.Error("over-limit payload was altered, want exact raw bytes")
	}

	exact := strings.Repeat("a", maxSessionIDBytes)
	raw = fmt.Sprintf(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":%q}}`, exact)
	out, ok = classifyOutput([]byte(raw), nil)
	if !ok || out.SessionID == nil || *out.SessionID != exact {
		t.Fatalf("exact-limit sessionId = %v (ok=%t), want retained", out.SessionID, ok)
	}
}

func fakePending(id string, meta pendingMeta) pendingLookup {
	return func(raw json.RawMessage) (pendingMeta, bool) {
		if string(raw) == id {
			return meta, true
		}
		return pendingMeta{}, false
	}
}

func TestClassifyOutputSessionMutationSeam(t *testing.T) {
	cwdNew := "/work/new"

	tests := []struct {
		name        string
		raw         string
		lookup      pendingLookup
		wantSession *string
		wantMut     *acpstore.SessionMutation
	}{
		{
			name:        "successful session/new uses result id and pending cwd",
			raw:         `{"jsonrpc":"2.0","id":1,"result":{"sessionId":"new-1"}}`,
			lookup:      fakePending("1", pendingMeta{Lifecycle: LifecycleNew, CWD: stringPtr(cwdNew)}),
			wantSession: stringPtr("new-1"),
			wantMut:     &acpstore.SessionMutation{Lifecycle: "new", SessionID: "new-1", CWD: cwdNew},
		},
		{
			name:        "successful session/load uses pending session id and cwd",
			raw:         `{"jsonrpc":"2.0","id":2,"result":null}`,
			lookup:      fakePending("2", pendingMeta{Lifecycle: LifecycleLoad, SessionID: stringPtr("s-load"), CWD: stringPtr("/w")}),
			wantSession: stringPtr("s-load"),
			wantMut:     &acpstore.SessionMutation{Lifecycle: "load", SessionID: "s-load", CWD: "/w"},
		},
		{
			name:        "successful session/resume uses pending session id and cwd",
			raw:         `{"jsonrpc":"2.0","id":"r","result":{}}`,
			lookup:      fakePending(`"r"`, pendingMeta{Lifecycle: LifecycleResume, SessionID: stringPtr("s-resume"), CWD: stringPtr("/r")}),
			wantSession: stringPtr("s-resume"),
			wantMut:     &acpstore.SessionMutation{Lifecycle: "resume", SessionID: "s-resume", CWD: "/r"},
		},
		{
			name:    "error response never mutates",
			raw:     `{"jsonrpc":"2.0","id":3,"error":{"code":-32603}}`,
			lookup:  fakePending("3", pendingMeta{Lifecycle: LifecycleNew, CWD: stringPtr(cwdNew)}),
			wantMut: nil,
		},
		{
			name:    "malformed session/new success never mutates",
			raw:     `{"jsonrpc":"2.0","id":4,"result":{}}`,
			lookup:  fakePending("4", pendingMeta{Lifecycle: LifecycleNew, CWD: stringPtr(cwdNew)}),
			wantMut: nil,
		},
		{
			name:    "over-limit result session id never mutates",
			raw:     `{"jsonrpc":"2.0","id":5,"result":{"sessionId":"` + strings.Repeat("a", maxSessionIDBytes+1) + `"}}`,
			lookup:  fakePending("5", pendingMeta{Lifecycle: LifecycleNew}),
			wantMut: nil,
		},
		{
			name:        "non-lifecycle response attributed but not mutated",
			raw:         `{"jsonrpc":"2.0","id":6,"result":{"sessionId":"other"}}`,
			lookup:      fakePending("6", pendingMeta{Lifecycle: LifecycleNone, SessionID: stringPtr("s-none")}),
			wantSession: stringPtr("s-none"),
			wantMut:     nil,
		},
		{
			name:    "unmatched response has no metadata",
			raw:     `{"jsonrpc":"2.0","id":99,"result":{"sessionId":"x"}}`,
			lookup:  fakePending("1", pendingMeta{Lifecycle: LifecycleNew}),
			wantMut: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, ok := classifyOutput([]byte(tt.raw), tt.lookup)
			if !ok {
				t.Fatal("classifyOutput = !ok, want response")
			}
			if !samePtr(out.SessionID, tt.wantSession) {
				t.Errorf("session = %v, want %v", ptrValue(out.SessionID), ptrValue(tt.wantSession))
			}
			if !sameMutation(out.Mutation, tt.wantMut) {
				t.Errorf("mutation = %+v, want %+v", out.Mutation, tt.wantMut)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }

func samePtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameMutation(a, b *acpstore.SessionMutation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Lifecycle == b.Lifecycle && a.SessionID == b.SessionID && a.CWD == b.CWD
}

func TestRuntimeOutputByteFidelity(t *testing.T) {
	r, store := newOutputRuntime(t)
	// Deliberately non-compact internal whitespace and an escape sequence that
	// json.Marshal would rewrite; the payload must survive byte-for-byte.
	object := "{\"jsonrpc\": \"2.0\", \"id\": 1, \"result\": {\"a\" : 1, \t \"b\":\"x\\u0041\"} }"
	input := []byte("  " + object + " \t\r\n")

	runReadOutput(t, r, input)

	events := listServerEvents(t, store)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if got := string(events[0].Payload); got != object {
		t.Errorf("payload = %q, want exact %q", got, object)
	}
	if events[0].Kind != "response" {
		t.Errorf("kind = %q, want response", events[0].Kind)
	}
}

func TestRuntimeOutputUnitInvalidRecovery(t *testing.T) {
	r, store := newOutputRuntime(t)

	valid := []byte(`{"jsonrpc":"2.0","id":2,"result":{}}`)
	input := bytes.Join([][]byte{
		[]byte(`{bad json`),
		[]byte(`123`),
		[]byte(`[1,2]`),
		[]byte("{\"id\":1,\"x\":\"\xff\xfe\"}"),
		append(bytes.Repeat([]byte("a"), maxOutputLine+1), '\n'),
		valid,
	}, []byte("\n"))
	input = append(input, '\n')

	runReadOutput(t, r, input)

	events := listServerEvents(t, store)
	wantInvalid := 5
	gotInvalid := 0
	for _, e := range events {
		if e.Kind == "notification" && eventMethod(e) == invalidStdoutMethod {
			gotInvalid++
		}
	}
	if gotInvalid != wantInvalid {
		t.Fatalf("_adapter/invalid_stdout events = %d, want %d (events=%d)", gotInvalid, wantInvalid, len(events))
	}
	if len(events) != wantInvalid+1 {
		t.Fatalf("events = %d, want %d", len(events), wantInvalid+1)
	}
	last := events[len(events)-1]
	if last.Kind != "response" || !bytes.Equal(last.Payload, valid) {
		t.Errorf("trailing valid line not committed: kind=%q payload=%q", last.Kind, last.Payload)
	}
}
