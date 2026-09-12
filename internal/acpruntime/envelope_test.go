package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// invalidClientEnvelopes is the corpus that must be rejected before any
// reservation or stdin write. It is shared by the classifier and Post tests so
// both prove they reject exactly the same inputs.
var invalidClientEnvelopes = []string{
	``,
	`{}`,
	`{"jsonrpc":"2.0"}`,
	`[{"jsonrpc":"2.0","id":1,"method":"x"}]`,
	`[]`,
	`"hello"`,
	`123`,
	`null`,
	`true`,
	// A valid JSON-RPC 2.0 object must carry exactly "jsonrpc":"2.0" in every
	// client form: request, notification, and response.
	`{"id":1,"method":"x"}`,
	`{"method":"x"}`,
	`{"id":1,"result":{}}`,
	`{"id":1,"error":{"code":1}}`,
	`{"jsonrpc":"1.0","id":1,"method":"x"}`,
	`{"jsonrpc":"2.0.0","id":1,"method":"x"}`,
	`{"jsonrpc":"","method":"x"}`,
	`{"jsonrpc":2.0,"id":1,"method":"x"}`,
	`{"jsonrpc":null,"id":1,"method":"x"}`,
	`{"jsonrpc":{"version":"2.0"},"method":"x"}`,
	`{"jsonrpc":"2.0","id":null,"method":"x"}`,
	`{"jsonrpc":"2.0","id":null,"result":{}}`,
	`{"jsonrpc":"2.0","id":1}`,
	`{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":1}}`,
	`{"jsonrpc":"2.0","result":{}}`,
	`{"jsonrpc":"2.0","error":{"code":1}}`,
	`{"jsonrpc":"2.0","id":true,"method":"x"}`,
	`{"jsonrpc":"2.0","id":{},"method":"x"}`,
	`{"jsonrpc":"2.0","id":[],"method":"x"}`,
	`{"jsonrpc":"2.0","id":1,"method":null}`,
	`{"jsonrpc":"2.0","id":1,"method":7}`,
	`{"jsonrpc":"2.0","id":1,"method":""}`,
	`{"jsonrpc":"2.0","method":null}`,
	`{"jsonrpc":"2.0","method":7}`,
	`{"jsonrpc":"2.0","method":""}`,
	`{"jsonrpc":"2.0","id":01,"method":"x"}`,
}

func TestClassifyClientEnvelopeKinds(t *testing.T) {
	tests := []struct {
		name          string
		raw           string
		wantKind      ClientKind
		wantID        string
		wantLifecycle Lifecycle
		wantSession   *string
		wantCWD       *string
	}{
		{
			name:          "request string id",
			raw:           `{"jsonrpc":"2.0","id":"a","method":"initialize"}`,
			wantKind:      ClientRequest,
			wantID:        `"a"`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "request numeric id preserves raw token",
			raw:           `{"jsonrpc":"2.0","id":1e0,"method":"initialize"}`,
			wantKind:      ClientRequest,
			wantID:        `1e0`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "notification without id",
			raw:           `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s"}}`,
			wantKind:      ClientNotification,
			wantLifecycle: LifecycleNone,
			// session-scoped notification attributes its sessionId.
			wantSession: stringPtr("s"),
		},
		{
			name:          "notification without params",
			raw:           `{"jsonrpc":"2.0","method":"session/cancel"}`,
			wantKind:      ClientNotification,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "response with result",
			raw:           `{"jsonrpc":"2.0","id":7,"result":{"ok":true}}`,
			wantKind:      ClientResponse,
			wantID:        `7`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "response with null result",
			raw:           `{"jsonrpc":"2.0","id":"r","result":null}`,
			wantKind:      ClientResponse,
			wantID:        `"r"`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "response with error",
			raw:           `{"jsonrpc":"2.0","id":9,"error":{"code":-32603}}`,
			wantKind:      ClientResponse,
			wantID:        `9`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "session/new carries cwd only",
			raw:           `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/work"}}`,
			wantKind:      ClientRequest,
			wantID:        `1`,
			wantLifecycle: LifecycleNew,
			wantCWD:       stringPtr("/work"),
		},
		{
			name:          "session/load carries session and cwd",
			raw:           `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"s-load","cwd":"/load"}}`,
			wantKind:      ClientRequest,
			wantID:        `2`,
			wantLifecycle: LifecycleLoad,
			wantSession:   stringPtr("s-load"),
			wantCWD:       stringPtr("/load"),
		},
		{
			name:          "session/resume carries session and cwd",
			raw:           `{"jsonrpc":"2.0","id":"res","method":"session/resume","params":{"sessionId":"s-resume","cwd":"/resume"}}`,
			wantKind:      ClientRequest,
			wantID:        `"res"`,
			wantLifecycle: LifecycleResume,
			wantSession:   stringPtr("s-resume"),
			wantCWD:       stringPtr("/resume"),
		},
		{
			name:          "session/prompt retains sessionId without lifecycle",
			raw:           `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"s-prompt"}}`,
			wantKind:      ClientRequest,
			wantID:        `3`,
			wantLifecycle: LifecycleNone,
			wantSession:   stringPtr("s-prompt"),
		},
		{
			name:          "initialize has no session metadata and ignores session keys",
			raw:           `{"jsonrpc":"2.0","id":4,"method":"initialize","params":{"sessionId":"ignored","cwd":"ignored"}}`,
			wantKind:      ClientRequest,
			wantID:        `4`,
			wantLifecycle: LifecycleNone,
		},
		{
			name:          "fs method is session-scoped",
			raw:           `{"jsonrpc":"2.0","id":5,"method":"fs/read_text_file","params":{"sessionId":"s-fs","path":"/p"}}`,
			wantKind:      ClientRequest,
			wantID:        `5`,
			wantLifecycle: LifecycleNone,
			wantSession:   stringPtr("s-fs"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, pending, err := ClassifyClientEnvelope([]byte(tt.raw))
			if err != nil {
				t.Fatalf("ClassifyClientEnvelope(%s) error = %v, want nil", tt.raw, err)
			}
			if kind != tt.wantKind {
				t.Errorf("kind = %q, want %q", kind, tt.wantKind)
			}
			if got := string(pending.ID); got != tt.wantID {
				t.Errorf("pending.ID = %q, want %q", got, tt.wantID)
			}
			if pending.Lifecycle != tt.wantLifecycle {
				t.Errorf("lifecycle = %q, want %q", pending.Lifecycle, tt.wantLifecycle)
			}
			if !samePtr(pending.SessionID, tt.wantSession) {
				t.Errorf("sessionId = %v, want %v", ptrValue(pending.SessionID), ptrValue(tt.wantSession))
			}
			if !samePtr(pending.CWD, tt.wantCWD) {
				t.Errorf("cwd = %v, want %v", ptrValue(pending.CWD), ptrValue(tt.wantCWD))
			}
		})
	}
}

func TestClassifyClientEnvelopeRejectsCorpus(t *testing.T) {
	for _, raw := range invalidClientEnvelopes {
		t.Run(fmt.Sprintf("reject %q", raw), func(t *testing.T) {
			kind, _, err := ClassifyClientEnvelope([]byte(raw))
			if !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("ClassifyClientEnvelope(%s) error = %v, want ErrInvalidEnvelope", raw, err)
			}
			if kind != "" {
				t.Errorf("kind = %q, want empty on rejection", kind)
			}
		})
	}
}

func TestClassifyClientEnvelopeRejectsInvalidUTF8(t *testing.T) {
	// json.Valid accepts invalid UTF-8 inside a JSON string; the classifier must
	// reject it before shape validation.
	raw := []byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"x\",\"params\":{\"s\":\"\xff\"}}")
	if !json.Valid(raw) {
		t.Fatal("test setup: json.Valid unexpectedly rejected invalid UTF-8")
	}
	if _, _, err := ClassifyClientEnvelope(raw); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("ClassifyClientEnvelope(invalid UTF-8) error = %v, want ErrInvalidEnvelope", err)
	}
}

func TestClassifyClientEnvelopeLimits(t *testing.T) {
	exactID := `"` + strings.Repeat("a", maxIDTokenBytes-2) + `"`
	overID := `"` + strings.Repeat("a", maxIDTokenBytes-1) + `"`

	exactSession := strings.Repeat("a", maxSessionIDBytes)
	overSession := strings.Repeat("a", maxSessionIDBytes+1)
	exactCWD := strings.Repeat("a", maxCWDBytes)
	overCWD := strings.Repeat("a", maxCWDBytes+1)

	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name: "exact 128-byte id accepted",
			raw:  `{"jsonrpc":"2.0","id":` + exactID + `,"method":"initialize"}`,
		},
		{
			name:    "over 128-byte id rejected",
			raw:     `{"jsonrpc":"2.0","id":` + overID + `,"method":"initialize"}`,
			wantErr: true,
		},
		{
			name: "exact session id accepted",
			raw:  `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"` + exactSession + `"}}`,
		},
		{
			name:    "over-limit session id rejected",
			raw:     `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"` + overSession + `"}}`,
			wantErr: true,
		},
		{
			name: "exact cwd accepted",
			raw:  `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"` + exactCWD + `"}}`,
		},
		{
			name:    "over-limit cwd rejected",
			raw:     `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"` + overCWD + `"}}`,
			wantErr: true,
		},
		{
			name:    "exponent at limit rejected as invalid numeric id",
			raw:     `{"jsonrpc":"2.0","id":1e1000001,"method":"initialize"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ClassifyClientEnvelope([]byte(tt.raw))
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidEnvelope) {
					t.Fatalf("error = %v, want ErrInvalidEnvelope", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
		})
	}
}

func TestIDKeyEquivalence(t *testing.T) {
	groups := [][]string{
		{`1`, `1.0`, `1e0`, `1.00`, `10e-1`, `0.1e1`},
		{`100`, `1e2`, `1.0e2`, `100.0`, `0.1e3`},
		{`0`, `0.0`, `-0`, `-0.0`, `0e5`, `0.000`, `-0e-1000000`},
		{`0.001`, `1e-3`, `0.0010`, `10e-4`},
		{`123`, `123.0`, `1.23e2`, `12.3e1`},
		{`-5`, `-5.0`, `-5e0`, `-50e-1`},
		{`1e1000000`},
		{`"a"`, `"\u0061"`},
	}

	for _, group := range groups {
		t.Run(group[0], func(t *testing.T) {
			want, err := idKey([]byte(group[0]))
			if err != nil {
				t.Fatalf("idKey(%s) error = %v, want nil", group[0], err)
			}
			for _, raw := range group[1:] {
				got, err := idKey([]byte(raw))
				if err != nil {
					t.Fatalf("idKey(%s) error = %v, want nil", raw, err)
				}
				if got != want {
					t.Errorf("idKey(%s) = %q, want equal to idKey(%s) = %q", raw, got, group[0], want)
				}
			}
		})
	}
}

func TestIDKeyDistinct(t *testing.T) {
	distinct := []string{
		`1`, `2`, `-1`, `1.1`,
		`"1"`, `"a"`, `"b"`,
		`1e1000000`, `1e999999`,
		`0`, `-1e-1000000`,
	}
	seen := make(map[string]string, len(distinct))
	for _, raw := range distinct {
		key, err := idKey([]byte(raw))
		if err != nil {
			t.Fatalf("idKey(%s) error = %v, want nil", raw, err)
		}
		if prev, ok := seen[key]; ok {
			t.Errorf("idKey(%s) collided with idKey(%s) = %q", raw, prev, key)
		}
		seen[key] = raw
	}
}

func TestIDKeyRejectsInvalidNumbers(t *testing.T) {
	invalid := []string{
		``, `01`, `-01`, `+1`, `1.`, `.1`, `1e`, `1e+`, `1e-`, `--1`,
		`0x1`, `NaN`, `Infinity`, `true`, `false`, `null`,
		`"unterminated`, `[1]`, `{}`, `1e1000001`, `1e-1000001`, `-1e1000001`,
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			if _, err := idKey([]byte(raw)); !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("idKey(%s) error = %v, want ErrInvalidEnvelope", raw, err)
			}
		})
	}
}

func TestIDKeyTokenBoundaries(t *testing.T) {
	stringAt := `"` + strings.Repeat("a", maxIDTokenBytes-2) + `"`
	stringOver := `"` + strings.Repeat("a", maxIDTokenBytes-1) + `"`
	numAt := "1" + strings.Repeat("0", maxIDTokenBytes-1)
	numOver := "1" + strings.Repeat("0", maxIDTokenBytes)

	if len(stringAt) != maxIDTokenBytes || len(stringOver) != maxIDTokenBytes+1 {
		t.Fatalf("test setup: string tokens = %d/%d bytes", len(stringAt), len(stringOver))
	}
	if len(numAt) != maxIDTokenBytes || len(numOver) != maxIDTokenBytes+1 {
		t.Fatalf("test setup: numeric tokens = %d/%d bytes", len(numAt), len(numOver))
	}

	for _, raw := range []string{stringAt, numAt} {
		if _, err := idKey([]byte(raw)); err != nil {
			t.Errorf("idKey(token of %d bytes) error = %v, want nil", len(raw), err)
		}
	}
	for _, raw := range []string{stringOver, numOver} {
		if _, err := idKey([]byte(raw)); !errors.Is(err, ErrInvalidEnvelope) {
			t.Errorf("idKey(token of %d bytes) error = %v, want ErrInvalidEnvelope", len(raw), err)
		}
	}

	if _, err := idKey([]byte(`1e1000000`)); err != nil {
		t.Errorf("idKey(1e1000000) error = %v, want nil", err)
	}
	if _, err := idKey([]byte(`1e-1000000`)); err != nil {
		t.Errorf("idKey(1e-1000000) error = %v, want nil", err)
	}
}

// maxIDKeyAllocs bounds the allocations of one idKey call on a boundary-sized
// token. Canonicalization must be lexical (O(token length), no power expansion).
const maxIDKeyAllocs = 4

func TestIDKeyAllocations(t *testing.T) {
	tokens := [][]byte{
		[]byte(`"` + strings.Repeat("a", maxIDTokenBytes-2) + `"`),
		[]byte("1" + strings.Repeat("0", maxIDTokenBytes-1)),
		[]byte(`1.23456789e1000000`),
		[]byte(`-9.99999999e-1000000`),
	}
	for _, tok := range tokens {
		allocs := testing.AllocsPerRun(200, func() {
			_, _ = idKey(tok)
		})
		if allocs > maxIDKeyAllocs {
			t.Errorf("idKey(%q) allocations = %.1f, want <= %d", tok, allocs, maxIDKeyAllocs)
		}
	}
}

func TestRuntimePostRejectsInvalidCorpus(t *testing.T) {
	r, _ := newOutputRuntime(t)

	for _, raw := range invalidClientEnvelopes {
		_, err := r.Post(context.Background(), []byte(raw))
		if !errors.Is(err, ErrInvalidEnvelope) {
			t.Errorf("Post(%s) error = %v, want ErrInvalidEnvelope", raw, err)
		}
	}

	// A valid envelope must not be mistaken for an invalid one.
	if _, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`)); errors.Is(err, ErrInvalidEnvelope) {
		t.Errorf("Post(valid) error = %v, want not ErrInvalidEnvelope", err)
	}
}
