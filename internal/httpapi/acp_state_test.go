package httpapi_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestACPList(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)
	handler := acpStoreHandler(t, &fakeACP{}, store)

	t.Run("empty store returns an empty array", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got := string(body["servers"]); got != "[]" {
			t.Fatalf("servers = %s, want []", got)
		}
	})

	t.Run("sorted by server ID with exactly five fields", func(t *testing.T) {
		for _, id := range []string{"zeta", "alpha", "mid"} {
			if _, err := store.CreateServer(ctx, id, "claude"); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
		rec := getACP(t, handler, "/v1/acp")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Servers []map[string]any `json:"servers"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		want := []string{"alpha", "mid", "zeta"}
		if len(body.Servers) != len(want) {
			t.Fatalf("servers = %d, want %d", len(body.Servers), len(want))
		}
		for i, srv := range body.Servers {
			if srv["serverId"] != want[i] {
				t.Errorf("servers[%d] = %v, want %q", i, srv["serverId"], want[i])
			}
			if len(srv) != 5 {
				t.Errorf("server %v has %d fields, want exactly 5", srv, len(srv))
			}
			for _, field := range []string{"agent", "status", "createdAtMs", "updatedAtMs"} {
				if _, ok := srv[field]; !ok {
					t.Errorf("server %v missing %q", srv, field)
				}
			}
		}
	})
}

func TestACPStatus(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)

	t.Run("unknown server is 404", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{}, store)
		rec := getACP(t, handler, "/v1/acp/ghost/status")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)
	})

	if _, err := store.CreateServer(ctx, "status-1", "claude"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := store.SetLive(ctx, "status-1", 1111); err != nil {
		t.Fatalf("set live: %v", err)
	}
	for _, sessionID := range []string{"s2", "s1"} {
		if _, err := store.AppendOutput(ctx, "status-1", acpstore.Output{
			Kind:     "response",
			Payload:  []byte(`{"ok":true}`),
			Mutation: &acpstore.SessionMutation{SessionID: sessionID, CWD: "/work"},
		}); err != nil {
			t.Fatalf("append session event: %v", err)
		}
	}

	type statusBody struct {
		ServerID     string   `json:"serverId"`
		Agent        string   `json:"agent"`
		Status       string   `json:"status"`
		CreatedAtMs  int64    `json:"createdAtMs"`
		LastEventSeq int64    `json:"lastEventSeq"`
		SessionIDs   []string `json:"sessionIds"`
		PID          *int     `json:"pid"`
		UpdatedAtMs  int64    `json:"updatedAtMs"`
	}

	t.Run("durable fields with sorted sessions and no PID when not live", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{live: false}, store)
		rec := getACP(t, handler, "/v1/acp/status-1/status")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body statusBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.ServerID != "status-1" {
			t.Errorf("serverId = %q, want status-1", body.ServerID)
		}
		if body.Agent != "claude" {
			t.Errorf("agent = %q, want claude", body.Agent)
		}
		if body.Status != string(acpstore.StatusIdle) {
			t.Errorf("status = %q, want idle", body.Status)
		}
		if body.LastEventSeq != 2 {
			t.Errorf("lastEventSeq = %d, want 2", body.LastEventSeq)
		}
		if body.CreatedAtMs <= 0 || body.UpdatedAtMs <= 0 {
			t.Errorf("timestamps = %d/%d, want positive", body.CreatedAtMs, body.UpdatedAtMs)
		}
		if !slices.Equal(body.SessionIDs, []string{"s1", "s2"}) {
			t.Errorf("sessionIds = %v, want [s1 s2]", body.SessionIDs)
		}
		if body.PID != nil {
			t.Errorf("pid = %d, want omitted when not live", *body.PID)
		}
	})

	t.Run("PID present only when LivePID confirms", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{live: true, pid: 4321}, store)
		rec := getACP(t, handler, "/v1/acp/status-1/status")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body statusBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.PID == nil || *body.PID != 4321 {
			t.Fatalf("pid = %v, want 4321", body.PID)
		}
	})
}

func TestACPEvents(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)
	handler := acpStoreHandler(t, &fakeACP{}, store)

	if _, err := store.CreateServer(ctx, "events-1", "claude"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	for _, payload := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		appendHTTPEvent(t, store, "events-1", payload, acpStrPtr("s1"))
	}

	t.Run("defaults after 0 limit 100 ascending", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := decodeACPEvents(t, rec)
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("seqs = %v, want [1 2 3]", got)
		}
		if body.Events[0].Kind != "notification" {
			t.Errorf("kind = %q, want notification", body.Events[0].Kind)
		}
		if body.Events[0].SessionID == nil || *body.Events[0].SessionID != "s1" {
			t.Errorf("sessionId = %v, want s1", body.Events[0].SessionID)
		}
		if body.Events[0].CreatedAtMs <= 0 {
			t.Errorf("createdAtMs = %d, want positive", body.Events[0].CreatedAtMs)
		}
	})

	t.Run("after is exclusive", func(t *testing.T) {
		body := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?after=1"))
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{2, 3}) {
			t.Fatalf("seqs = %v, want [2 3]", got)
		}
	})

	t.Run("limit bounds", func(t *testing.T) {
		cases := []struct {
			name  string
			query string
			want  int
			seqs  []int64
		}{
			{name: "one", query: "?limit=1", want: http.StatusOK, seqs: []int64{1}},
			{name: "max", query: "?limit=1000", want: http.StatusOK, seqs: []int64{1, 2, 3}},
			{name: "zero", query: "?limit=0", want: http.StatusBadRequest},
			{name: "over max", query: "?limit=1001", want: http.StatusBadRequest},
			{name: "negative", query: "?limit=-1", want: http.StatusBadRequest},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events"+tc.query)
				if rec.Code != tc.want {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want == http.StatusBadRequest {
					assertProblem(t, rec, tc.want)
					return
				}
				body := decodeACPEvents(t, rec)
				if got := eventSeqs(body.Events); !slices.Equal(got, tc.seqs) {
					t.Fatalf("seqs = %v, want %v", got, tc.seqs)
				}
			})
		}
	})

	t.Run("order ascending and descending", func(t *testing.T) {
		asc := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?order=asc"))
		desc := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?order=desc"))
		if got := eventSeqs(asc.Events); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("asc seqs = %v, want [1 2 3]", got)
		}
		if got := eventSeqs(desc.Events); !slices.Equal(got, []int64{3, 2, 1}) {
			t.Fatalf("desc seqs = %v, want [3 2 1]", got)
		}
	})

	t.Run("after parsing through math.MaxInt64", func(t *testing.T) {
		cases := []struct {
			name  string
			value string
			want  int
		}{
			{name: "max int64", value: "9223372036854775807", want: http.StatusOK},
			{name: "max int64 plus one", value: "9223372036854775808", want: http.StatusBadRequest},
			{name: "negative", value: "-1", want: http.StatusBadRequest},
			{name: "plus sign", value: "+1", want: http.StatusBadRequest},
			{name: "non decimal", value: "1a", want: http.StatusBadRequest},
			{name: "fraction", value: "1.0", want: http.StatusBadRequest},
			{name: "empty", value: "", want: http.StatusBadRequest},
			{name: "whitespace", value: "%20", want: http.StatusBadRequest},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events?after="+tc.value)
				if rec.Code != tc.want {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want == http.StatusBadRequest {
					assertProblem(t, rec, tc.want)
					return
				}
				if body := decodeACPEvents(t, rec); len(body.Events) != 0 {
					t.Fatalf("events = %v, want none past math.MaxInt64", body.Events)
				}
			})
		}
	})

	t.Run("repeated empty and unknown query keys are rejected", func(t *testing.T) {
		cases := []struct {
			name  string
			query string
		}{
			{name: "repeated after", query: "after=1&after=2"},
			{name: "repeated limit", query: "limit=1&limit=2"},
			{name: "repeated order", query: "order=asc&order=desc"},
			{name: "repeated session", query: "sessionId=s1&sessionId=s2"},
			{name: "unknown key", query: "bogus=1"},
			{name: "invalid order", query: "order=sideways"},
			{name: "empty order", query: "order="},
			{name: "empty session", query: "sessionId="},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events?"+tc.query)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertProblem(t, rec, http.StatusBadRequest)
			})
		}
	})

	t.Run("session filter", func(t *testing.T) {
		if _, err := store.CreateServer(ctx, "events-2", "claude"); err != nil {
			t.Fatalf("create server: %v", err)
		}
		if _, err := store.AppendOutput(ctx, "events-2", acpstore.Output{
			Kind:      "response",
			Payload:   []byte(`{"ok":true}`),
			SessionID: acpStrPtr("s2"),
			Mutation:  &acpstore.SessionMutation{SessionID: "s2", CWD: "/work"},
		}); err != nil {
			t.Fatalf("create session: %v", err)
		}
		appendHTTPEvent(t, store, "events-2", `{"n":4}`, acpStrPtr("s2"))
		appendHTTPEvent(t, store, "events-2", `{"n":5}`, acpStrPtr("s1"))

		body := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-2/events?sessionId=s2"))
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("seqs = %v, want [1 2]", got)
		}
	})

	t.Run("session filter length boundary", func(t *testing.T) {
		atLimit := strings.Repeat("a", 1024)
		rec := getACP(t, handler, "/v1/acp/events-1/events?sessionId="+atLimit)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("1024-byte session status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		overLimit := strings.Repeat("a", 1025)
		rec = getACP(t, handler, "/v1/acp/events-1/events?sessionId="+overLimit)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("1025-byte session status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("unknown session is 404 and unknown server takes precedence", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events?sessionId=missing")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown session status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)

		rec = getACP(t, handler, "/v1/acp/ghost/events?sessionId=missing")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown server status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)
	})

	t.Run("raw payload is embedded not JSON-string-quoted", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events?limit=1")
		body := decodeACPEvents(t, rec)
		if len(body.Events) != 1 {
			t.Fatalf("events = %d, want 1", len(body.Events))
		}
		if got := string(body.Events[0].Payload); got != `{"n":1}` {
			t.Fatalf("payload = %q, want raw {\"n\":1}", got)
		}
		if strings.Contains(rec.Body.String(), `"payload":"`) {
			t.Fatalf("payload was JSON-string-quoted: %s", rec.Body.String())
		}
	})
}
