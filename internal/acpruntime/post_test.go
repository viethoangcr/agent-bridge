package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestByteFidelity(t *testing.T) {
	payloads := []struct {
		name string
		raw  string
	}{
		{
			name: "html and unicode escapes",
			raw:  `{ "jsonrpc" : "2.0" , "method" : "x" , "params" : { "s" : "\u003c\u003e\u0026" } }`,
		},
		{
			name: "number lexemes preserved",
			raw:  `{"jsonrpc":"2.0","method":"x","params":{"n":1e0,"m":1.0,"p":100}}`,
		},
		{
			name: "client response raw bytes",
			raw:  `{"jsonrpc": "2.0", "id": 1, "result": {"a": "<b>&c"}}`,
		},
	}

	for _, tt := range payloads {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &recordWriter{}
			r := newWriterRuntime(t, time.Minute, recorder.write)

			res, err := r.Post(context.Background(), []byte(tt.raw))
			if err != nil {
				t.Fatalf("Post error = %v, want nil", err)
			}
			if !res.Accepted {
				t.Fatal("Post Accepted = false, want true")
			}

			var compact bytes.Buffer
			if err := json.Compact(&compact, []byte(tt.raw)); err != nil {
				t.Fatalf("compact expectation: %v", err)
			}
			want := append(compact.Bytes(), '\n')

			got := recorder.all()
			if len(got) != 1 {
				t.Fatalf("records = %d, want 1", len(got))
			}
			if !bytes.Equal(got[0], want) {
				t.Errorf("record = %q, want %q", got[0], want)
			}
		})
	}
}

func TestPostOverLimitClientMetadataRejectedBeforeReservation(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	over := make([]byte, maxSessionIDBytes+1)
	for i := range over {
		over[i] = 'a'
	}
	payload := `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"` +
		string(over) + `","cwd":"/w"}}`
	if _, err := h.r.Post(context.Background(), []byte(payload)); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("over-limit session load error = %v, want ErrInvalidEnvelope", err)
	}
	if h.corrLen() != 0 {
		t.Fatalf("over-limit client metadata reserved %d correlations, want 0", h.corrLen())
	}
	if len(h.writer.all()) != 0 {
		t.Fatal("over-limit client metadata was written")
	}
}
