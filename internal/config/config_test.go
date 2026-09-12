package config_test

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/config"
)

func getenv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load(getenv(nil))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want %q", cfg.Host, "127.0.0.1")
	}
	if cfg.Port != 2468 {
		t.Errorf("Port = %d, want %d", cfg.Port, 2468)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.DBPath != "./agent-bridge.db" {
		t.Errorf("DBPath = %q, want %q", cfg.DBPath, "./agent-bridge.db")
	}
	if cfg.Token != "" {
		t.Errorf("Token = %q, want empty", cfg.Token)
	}
	if cfg.AllowInsecureRemote {
		t.Error("AllowInsecureRemote = true, want false")
	}
	if cfg.PIDFile != "" {
		t.Errorf("PIDFile = %q, want empty", cfg.PIDFile)
	}
	if cfg.ACPRequestTimeout != 600000*time.Millisecond {
		t.Errorf("ACPRequestTimeout = %v, want %v", cfg.ACPRequestTimeout, 600000*time.Millisecond)
	}
	if cfg.IdleTTL != 900000*time.Millisecond {
		t.Errorf("IdleTTL = %v, want %v", cfg.IdleTTL, 900000*time.Millisecond)
	}

	wantAgents := map[string]config.AgentCommand{
		"claude":   {Binary: "claude-agent-acp"},
		"codex":    {Binary: "codex-acp"},
		"opencode": {Binary: "opencode", Args: []string{"acp"}},
	}
	if !reflect.DeepEqual(cfg.Agents, wantAgents) {
		t.Errorf("Agents = %#v, want %#v", cfg.Agents, wantAgents)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := config.Load(getenv(map[string]string{
		"AGENT_BRIDGE_HOST":                   "127.0.0.1",
		"AGENT_BRIDGE_PORT":                   "9999",
		"AGENT_BRIDGE_LOG_LEVEL":              "warn",
		"AGENT_BRIDGE_DB":                     "/data/bridge.db",
		"AGENT_BRIDGE_TOKEN":                  "tok",
		"AGENT_BRIDGE_PID_FILE":               "/run/bridge.pid",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "1500",
		"AGENT_BRIDGE_IDLE_TTL_MS":            "0",
		"AGENT_BRIDGE_CLAUDE_BIN":             "/opt/claude",
		"AGENT_BRIDGE_CODEX_ARGS":             `["--codex"]`,
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Port != 9999 {
		t.Errorf("Port = %d, want %d", cfg.Port, 9999)
	}
	if cfg.LogLevel != slog.LevelWarn {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelWarn)
	}
	if cfg.DBPath != "/data/bridge.db" {
		t.Errorf("DBPath = %q, want %q", cfg.DBPath, "/data/bridge.db")
	}
	if cfg.Token != "tok" {
		t.Errorf("Token = %q, want %q", cfg.Token, "tok")
	}
	if cfg.PIDFile != "/run/bridge.pid" {
		t.Errorf("PIDFile = %q, want %q", cfg.PIDFile, "/run/bridge.pid")
	}
	if cfg.ACPRequestTimeout != 1500*time.Millisecond {
		t.Errorf("ACPRequestTimeout = %v, want %v", cfg.ACPRequestTimeout, 1500*time.Millisecond)
	}
	if cfg.IdleTTL != 0 {
		t.Errorf("IdleTTL = %v, want 0", cfg.IdleTTL)
	}
	if cfg.Agents["claude"].Binary != "/opt/claude" {
		t.Errorf("claude binary = %q, want %q", cfg.Agents["claude"].Binary, "/opt/claude")
	}
	if got := cfg.Agents["codex"].Args; !reflect.DeepEqual(got, []string{"--codex"}) {
		t.Errorf("codex args = %#v, want %#v", got, []string{"--codex"})
	}
}

func TestLoadLogLevels(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"DEBUG", slog.LevelDebug},
		{"Info", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"Error", slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			cfg, err := config.Load(getenv(map[string]string{"AGENT_BRIDGE_LOG_LEVEL": tt.in}))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.LogLevel != tt.want {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, tt.want)
			}
		})
	}
}

func TestLoadAgentArgs(t *testing.T) {
	cfg, err := config.Load(getenv(map[string]string{
		"AGENT_BRIDGE_OPENCODE_BIN":  "/usr/bin/oc",
		"AGENT_BRIDGE_OPENCODE_ARGS": `["--flag", "two words", ""]`,
		"AGENT_BRIDGE_CLAUDE_ARGS":   `[]`,
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got := cfg.Agents["opencode"].Binary; got != "/usr/bin/oc" {
		t.Errorf("opencode binary = %q, want %q", got, "/usr/bin/oc")
	}
	want := []string{"--flag", "two words", ""}
	if got := cfg.Agents["opencode"].Args; !reflect.DeepEqual(got, want) {
		t.Errorf("opencode args = %#v, want %#v", got, want)
	}
	if got := cfg.Agents["claude"].Args; got == nil || len(got) != 0 {
		t.Errorf("claude args = %#v, want non-nil empty slice", got)
	}
}

func TestLoadAgentArgsOwnership(t *testing.T) {
	first, err := config.Load(getenv(nil))
	if err != nil {
		t.Fatalf("first Load() error = %v", err)
	}
	if len(first.Agents["opencode"].Args) == 0 {
		t.Fatalf("opencode args = %#v, want at least one element", first.Agents["opencode"].Args)
	}
	first.Agents["opencode"].Args[0] = "mutated"
	first.Agents["claude"] = config.AgentCommand{Binary: "evil"}

	second, err := config.Load(getenv(nil))
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}
	if got := second.Agents["opencode"].Args; len(got) == 0 || got[0] != "acp" {
		t.Errorf("opencode args aliased across loads: got %#v, want [acp]", got)
	}
	if got := second.Agents["claude"].Binary; got != "claude-agent-acp" {
		t.Errorf("claude binary aliased across loads: got %q, want %q", got, "claude-agent-acp")
	}
}

func TestLoadRejections(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		varName string
	}{
		{"port malformed", map[string]string{"AGENT_BRIDGE_PORT": "abc"}, "AGENT_BRIDGE_PORT"},
		{"port zero", map[string]string{"AGENT_BRIDGE_PORT": "0"}, "AGENT_BRIDGE_PORT"},
		{"port too high", map[string]string{"AGENT_BRIDGE_PORT": "65536"}, "AGENT_BRIDGE_PORT"},
		{"port negative", map[string]string{"AGENT_BRIDGE_PORT": "-1"}, "AGENT_BRIDGE_PORT"},
		{"request timeout zero", map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "0"}, "AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"},
		{"request timeout negative", map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "-1"}, "AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"},
		{"request timeout over one hour", map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "3600001"}, "AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"},
		{"request timeout non-integer", map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "abc"}, "AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"},
		{"request timeout overflow", map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "9223372036854775807"}, "AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"},
		{"idle ttl negative", map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "-1"}, "AGENT_BRIDGE_IDLE_TTL_MS"},
		{"idle ttl over thirty days", map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "2592000001"}, "AGENT_BRIDGE_IDLE_TTL_MS"},
		{"idle ttl non-integer", map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "abc"}, "AGENT_BRIDGE_IDLE_TTL_MS"},
		{"idle ttl overflow", map[string]string{"AGENT_BRIDGE_IDLE_TTL_MS": "9223372036854775807"}, "AGENT_BRIDGE_IDLE_TTL_MS"},
		{"log level unsupported", map[string]string{"AGENT_BRIDGE_LOG_LEVEL": "verbose"}, "AGENT_BRIDGE_LOG_LEVEL"},
		{"insecure remote invalid", map[string]string{"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE": "true"}, "AGENT_BRIDGE_ALLOW_INSECURE_REMOTE"},
		{"agent args non-array", map[string]string{"AGENT_BRIDGE_CLAUDE_ARGS": `"x"`}, "AGENT_BRIDGE_CLAUDE_ARGS"},
		{"agent args non-string element", map[string]string{"AGENT_BRIDGE_CLAUDE_ARGS": `[1]`}, "AGENT_BRIDGE_CLAUDE_ARGS"},
		{"agent args null", map[string]string{"AGENT_BRIDGE_CLAUDE_ARGS": `null`}, "AGENT_BRIDGE_CLAUDE_ARGS"},
		{"agent args malformed", map[string]string{"AGENT_BRIDGE_CLAUDE_ARGS": `["a"`}, "AGENT_BRIDGE_CLAUDE_ARGS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Load(getenv(tt.env))
			if err == nil {
				t.Fatalf("Load() error = nil, want error naming %s", tt.varName)
			}
			if !strings.Contains(err.Error(), tt.varName) {
				t.Errorf("error %q does not name %s", err, tt.varName)
			}
		})
	}
}

func TestLoadRemoteAuth(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		token    string
		insecure string
		wantErr  bool
	}{
		{"wildcard ipv4 without token", "0.0.0.0", "", "", true},
		{"wildcard ipv6 without token", "::", "", "", true},
		{"unknown hostname without token", "example.invalid", "", "", true},
		{"wildcard ipv4 with token", "0.0.0.0", "s3cr3t", "", false},
		{"wildcard ipv4 with override", "0.0.0.0", "", "1", false},
		{"loopback ipv4 without token", "127.0.0.1", "", "", false},
		{"other loopback ipv4 without token", "127.0.0.5", "", "", false},
		{"loopback ipv6 without token", "::1", "", "", false},
		{"localhost without token", "localhost", "", "", false},
		{"mixed-case localhost without token", "LOCALHOST", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Load(getenv(map[string]string{
				"AGENT_BRIDGE_HOST":                  tt.host,
				"AGENT_BRIDGE_TOKEN":                 tt.token,
				"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE": tt.insecure,
			}))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() error = nil, want remote-auth rejection")
				}
				if !strings.Contains(err.Error(), "AGENT_BRIDGE_HOST") && !strings.Contains(err.Error(), "AGENT_BRIDGE_TOKEN") {
					t.Errorf("error %q does not name the responsible variable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
		})
	}
}

func TestLoadErrorOmitsToken(t *testing.T) {
	const secret = "sup3r-s3cr3t-token"
	_, err := config.Load(getenv(map[string]string{
		"AGENT_BRIDGE_TOKEN": secret,
		"AGENT_BRIDGE_PORT":  "not-a-port",
	}))
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked token value: %v", err)
	}
}

func TestLoadAddress(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"default", nil, "127.0.0.1:2468"},
		{"ipv6 loopback", map[string]string{"AGENT_BRIDGE_HOST": "::1", "AGENT_BRIDGE_PORT": "8080"}, "[::1]:8080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.Load(getenv(tt.env))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := cfg.Address(); got != tt.want {
				t.Errorf("Address() = %q, want %q", got, tt.want)
			}
		})
	}
}
