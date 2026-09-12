//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// stateServerID is the durable server exercised across the container restart.
const stateServerID = "state"

// sentinelRecoveryScript occupies PID $STATE_PID with a long-lived sleep, then
// execs the real image entrypoint. If startup recovery signals the persisted
// PID, that sleep disappears or becomes a zombie, making the negative check
// deterministic instead of hoping the kernel reuses a PID.
const sentinelRecoveryScript = `i=1; while [ "$i" -lt "$STATE_PID" ]; do sleep 600 & i=$((i+1)); done; exec /usr/bin/tini -- /usr/local/bin/agent-bridge`

// TestDockerStateRestart proves SQLite-backed events, sessions, and cwd survive
// an abrupt bridge restart while stale live metadata becomes exited, the
// persisted PID is never signaled, only initialize recreates the runtime, and
// SSE replay/live continuity holds.
func TestDockerStateRestart(t *testing.T) {
	image := buildImage(t)
	token := uniqueToken("state")
	volume := "agent-bridge-e2e-state-vol-" + uniqueSuffix()
	dockerOrFail(t, "volume", "create", volume)
	t.Cleanup(func() {
		if !keep() {
			_, _ = docker("volume", "rm", "-f", volume)
		}
	})

	// Phase 1: build durable state on the named volume, then terminate abruptly
	// without DELETE so the durable row keeps stale live metadata.
	first := restartContainer(t, image, token, volume, nil, nil)
	initialize(t, first, stateServerID, "mock")
	sessionID, gotCWD := sessionNewResult(t, first, stateServerID, mockCWD)
	if gotCWD != mockCWD {
		t.Fatalf("session/new cwd = %q, want %q", gotCWD, mockCWD)
	}
	if code, body := prompt(t, first, stateServerID, sessionID, "before restart"); code != http.StatusOK {
		t.Fatalf("pre-restart prompt = %d: %s\n%s", code, body, first.diagnostics())
	}

	before, code := events(t, first, stateServerID, "limit=1000")
	if code != http.StatusOK || len(before) != 5 {
		t.Fatalf("pre-restart events = %d (status %d), want 5\n%s", len(before), code, first.diagnostics())
	}
	for i, event := range before {
		if event.Seq != int64(i+1) {
			t.Fatalf("pre-restart seq[%d] = %d, want %d", i, event.Seq, i+1)
		}
	}
	lastSeq := before[len(before)-1].Seq

	live, _ := assertServerStatus(t, first, stateServerID, "idle", true)
	if len(live.SessionIDs) != 1 || live.SessionIDs[0] != sessionID {
		t.Fatalf("pre-restart sessionIds = %v, want [%s]", live.SessionIDs, sessionID)
	}
	persistedPID := *live.PID

	dockerOrFail(t, "rm", "-f", first.name)

	// Phase 2: restart on the same volume. The persisted PID is occupied by a
	// sentinel before the bridge starts so any startup signal is observable.
	env := map[string]string{}
	var entrypoint []string
	if persistedPID >= 2 && persistedPID <= 1024 {
		env["STATE_PID"] = strconv.Itoa(persistedPID)
		entrypoint = []string{"/bin/sh", "-c", sentinelRecoveryScript}
	}
	second := restartContainer(t, image, token, volume, env, entrypoint)
	second.mustHealthy()

	if entrypoint != nil {
		assertSentinelSurvived(t, second.name, persistedPID)
		t.Logf("proved no persisted PID was signaled: sentinel reused PID %d and survived startup recovery", persistedPID)
	} else {
		t.Logf("persisted PID %d outside sentinel range; recovery proved through status only", persistedPID)
	}

	recovered, _ := assertServerStatus(t, second, stateServerID, "exited", false)
	if recovered.LastEventSeq != lastSeq {
		t.Fatalf("post-restart lastEventSeq = %d, want %d", recovered.LastEventSeq, lastSeq)
	}
	if len(recovered.SessionIDs) != 1 || recovered.SessionIDs[0] != sessionID {
		t.Fatalf("post-restart sessionIds = %v, want [%s]", recovered.SessionIDs, sessionID)
	}
	afterRestart, code := events(t, second, stateServerID, "limit=1000")
	if code != http.StatusOK || len(afterRestart) != len(before) {
		t.Fatalf("post-restart events = %d (status %d), want %d", len(afterRestart), code, len(before))
	}
	for i := range before {
		if afterRestart[i].Seq != before[i].Seq || !bytes.Equal(afterRestart[i].Payload, before[i].Payload) {
			t.Fatalf("post-restart event %d = %+v, want seq %d payload %s", i, afterRestart[i], before[i].Seq, before[i].Payload)
		}
	}
	// cwd is not exposed over HTTP; read the durable session row directly
	// before any post-restart session/new can upsert the same mock session ID.
	if got := persistedCWD(t, second, stateServerID, sessionID); got != mockCWD {
		t.Fatalf("persisted session cwd = %q, want %q", got, mockCWD)
	}

	// Phase 3: non-initialize is 409 with reinitialization guidance, then
	// initialize recreates without dropping events and keeps the sequence
	// monotonic.
	resp, body := second.request(http.MethodPost, acpPath(stateServerID, ""), rpc("stale", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "stale"}},
	}))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale non-initialize = %d: %s, want 409\n%s", resp.StatusCode, body, second.diagnostics())
	}
	assertReinitializeProblem(t, resp.Header.Get("Content-Type"), body)

	initialize(t, second, stateServerID, "")
	recreated, _ := assertServerStatus(t, second, stateServerID, "idle", true)
	if *recreated.PID == persistedPID {
		t.Fatalf("recreated PID = persisted PID %d; expected a fresh process", persistedPID)
	}
	retained, code := events(t, second, stateServerID, "limit=1000")
	if code != http.StatusOK || len(retained) != len(before)+1 {
		t.Fatalf("after reinitialize events = %d (status %d), want %d", len(retained), code, len(before)+1)
	}
	if retained[0].Seq != before[0].Seq {
		t.Fatalf("reinitialize dropped old events: first seq %d, want %d", retained[0].Seq, before[0].Seq)
	}
	reinitSeq := retained[len(retained)-1].Seq

	// Phase 4: session/load is forwarded to the fresh agent, which does not
	// know the old session; the bridge neither synthesizes nor replays prompts.
	code, loadBody := postACP(t, second, stateServerID, "", rpc("load", "session/load", map[string]any{
		"sessionId": sessionID,
		"cwd":       mockCWD,
	}))
	if code != http.StatusOK {
		t.Fatalf("session/load = %d: %s, want 200 JSON-RPC envelope", code, loadBody)
	}
	loadEnv := decodeEnvelope(t, loadBody)
	if loadEnv.Error == nil || loadEnv.Error.Code != -32002 {
		t.Fatalf("session/load envelope = %+v, want the fresh mock's -32002 unknown session", loadEnv)
	}
	loaded, code := events(t, second, stateServerID, "limit=1000")
	if code != http.StatusOK {
		t.Fatalf("events after load = %d", code)
	}
	for _, event := range loaded {
		if event.Seq > reinitSeq && event.Method != nil && *event.Method == "session/update" {
			t.Fatalf("bridge synthesized a session/update after load: %+v", event)
		}
	}

	newSession, _ := sessionNewResult(t, second, stateServerID, "/workspace/next")
	if code, body := prompt(t, second, stateServerID, newSession, "after restart"); code != http.StatusOK {
		t.Fatalf("post-restart prompt = %d: %s\n%s", code, body, second.diagnostics())
	}
	final, code := events(t, second, stateServerID, "limit=1000")
	if code != http.StatusOK || len(final) <= len(loaded) {
		t.Fatalf("final events = %d (status %d), want more than %d", len(final), code, len(loaded))
	}
	for i := 1; i < len(final); i++ {
		if final[i].Seq != final[i-1].Seq+1 {
			t.Fatalf("event sequence not monotonic: %d then %d", final[i-1].Seq, final[i].Seq)
		}
	}
	if final[0].Seq != before[0].Seq {
		t.Fatalf("old events not retained: first seq %d, want %d", final[0].Seq, before[0].Seq)
	}

	// Phase 5: reconnect SSE with the pre-restart Last-Event-ID and prove
	// replay plus live continuity with no duplicates or gaps.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stream, err := second.subscribe(ctx, stateServerID, strconv.FormatInt(lastSeq, 10))
	if err != nil {
		t.Fatalf("reconnect SSE: %v\n%s", err, second.diagnostics())
	}
	defer stream.close()

	for want := lastSeq + 1; want <= final[len(final)-1].Seq; want++ {
		event, err := stream.nextMessage(second)
		if err != nil {
			t.Fatalf("replay event %d: %v\n%s", want, err, second.diagnostics())
		}
		if event.ID != want {
			t.Fatalf("replay id = %d, want %d (no duplicates or gaps)", event.ID, want)
		}
	}
	if code, body := prompt(t, second, stateServerID, newSession, "live"); code != http.StatusOK {
		t.Fatalf("live prompt = %d: %s\n%s", code, body, second.diagnostics())
	}
	liveStart := final[len(final)-1].Seq
	for i := 0; i < 3; i++ {
		event, err := stream.nextMessage(second)
		if err != nil {
			t.Fatalf("live event %d: %v\n%s", i, err, second.diagnostics())
		}
		if event.ID != liveStart+int64(i)+1 {
			t.Fatalf("live id = %d, want %d", event.ID, liveStart+int64(i)+1)
		}
	}
}

// restartContainer starts one bridge container on an existing named volume so
// two generations can share durable state. A non-nil entrypoint overrides the
// image entrypoint, which the sentinel negative check uses to occupy a PID
// before the bridge starts.
func restartContainer(t *testing.T, image, token, volume string, env map[string]string, entrypoint []string) *container {
	t.Helper()
	c := &container{
		t:              t,
		name:           "agent-bridge-e2e-state-" + uniqueSuffix(),
		image:          image,
		token:          token,
		volume:         volume,
		requestTimeout: requestTimeout,
	}
	args := []string{
		"run", "-d", "--name", c.name,
		"-p", "127.0.0.1::" + containerPort,
		"-e", "AGENT_BRIDGE_TOKEN=" + token,
		"-v", volume + ":/workspace",
	}
	for key, value := range env {
		args = append(args, "-e", key+"="+value)
	}
	if len(entrypoint) > 0 {
		args = append(args, "--entrypoint", entrypoint[0])
	}
	args = append(args, image)
	if len(entrypoint) > 1 {
		args = append(args, entrypoint[1:]...)
	}
	if out, err := docker(args...); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(c.cleanup)

	port, err := c.discoverPort()
	if err != nil {
		t.Fatalf("%v\n%s", err, c.diagnostics())
	}
	c.port = port
	c.base = "http://127.0.0.1:" + port
	return c
}

// assertServerStatus fetches serverID's status and asserts the durable status
// plus whether a live PID is present.
func assertServerStatus(t *testing.T, c *container, serverID, wantStatus string, wantPID bool) (statusView, int) {
	t.Helper()
	view, code := status(t, c, serverID)
	if code != http.StatusOK {
		t.Fatalf("status %s = %d\n%s", serverID, code, c.diagnostics())
	}
	if view.Status != wantStatus {
		t.Fatalf("status %s = %q, want %q\n%s", serverID, view.Status, wantStatus, c.diagnostics())
	}
	if wantPID && view.PID == nil {
		t.Fatalf("status %s has no PID, want a live PID\n%s", serverID, c.diagnostics())
	}
	if !wantPID && view.PID != nil {
		t.Fatalf("status %s has PID %d, want it omitted\n%s", serverID, *view.PID, c.diagnostics())
	}
	return view, code
}

// assertReinitializeProblem asserts the exact RFC 9457 shape and that the
// detail instructs the client to reinitialize.
func assertReinitializeProblem(t *testing.T, contentType string, body []byte) {
	t.Helper()
	if !strings.HasPrefix(contentType, "application/problem+json") {
		t.Fatalf("409 content-type = %q, want application/problem+json", contentType)
	}
	var p problem
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode 409 problem: %v (%s)", err, body)
	}
	if p.Type != "about:blank" || p.Status != http.StatusConflict || p.Title == "" || p.Detail == "" {
		t.Fatalf("409 problem = %+v, want about:blank 409 with title and detail", p)
	}
	if !strings.Contains(strings.ToLower(p.Detail), "initialize") {
		t.Fatalf("409 detail %q does not instruct reinitialization", p.Detail)
	}
}

// assertSentinelSurvived proves the process occupying the persisted PID was
// not signaled by startup recovery: it must still exist, be alive, and be the
// planted sleep rather than a zombie.
func assertSentinelSurvived(t *testing.T, name string, pid int) {
	t.Helper()
	out, err := docker("exec", name, "cat", "/proc/"+strconv.Itoa(pid)+"/stat")
	if err != nil {
		t.Fatalf("sentinel pid %d vanished: startup recovery signaled the persisted PID: %v\n%s", pid, err, out)
	}
	stat := string(out)
	end := strings.LastIndex(stat, ")")
	if end < 0 || end+2 >= len(stat) {
		t.Fatalf("unparseable /proc/%d/stat: %q", pid, stat)
	}
	if state := stat[end+2]; state == 'Z' {
		t.Fatalf("sentinel pid %d is a zombie: startup recovery signaled the persisted PID: %q", pid, stat)
	}
	if comm := stat[strings.Index(stat, "(")+1 : end]; comm != "sleep" {
		t.Fatalf("pid %d comm = %q, want the planted sleep sentinel", pid, comm)
	}
}

// persistedCWD copies the volume's database out of the container and returns
// the durable cwd for serverID's session, which no HTTP endpoint exposes.
func persistedCWD(t *testing.T, c *container, serverID, sessionID string) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := docker("cp", c.name+":/workspace/.", dir); err != nil {
		t.Fatalf("docker cp workspace: %v\n%s", err, out)
	}
	ctx := context.Background()
	store, err := acpstore.Open(ctx, filepath.Join(dir, "agent-bridge.db"))
	if err != nil {
		t.Fatalf("open copied store: %v", err)
	}
	defer func() { _ = store.Close(ctx) }()
	sessions, err := store.Sessions(ctx, serverID)
	if err != nil {
		t.Fatalf("read persisted sessions: %v", err)
	}
	for _, session := range sessions {
		if session.SessionID == sessionID {
			return session.CWD
		}
	}
	t.Fatalf("session %s not found in persisted store", sessionID)
	return ""
}
