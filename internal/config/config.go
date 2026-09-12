// Package config parses and validates the agent-bridge startup environment.
//
// Load is the only interpreter of public AGENT_BRIDGE_* variables. It applies
// the documented defaults, rejects malformed or out-of-range values, and
// enforces the loopback/token safety rule before the server starts.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

// Defaults and bounds for the authoritative startup environment contract.
const (
	defaultHost              = "127.0.0.1"
	defaultPort              = 2468
	defaultDBPath            = "./agent-bridge.db"
	defaultACPRequestTimeout = 600000 * time.Millisecond
	defaultIdleTTL           = 900000 * time.Millisecond
	maxACPRequestTimeout     = time.Hour
	maxIdleTTL               = 30 * 24 * time.Hour

	// maxDurationMillis is the largest millisecond count that fits in a
	// time.Duration without overflowing the nanosecond conversion.
	maxDurationMillis = math.MaxInt64 / int64(time.Millisecond)
)

// AgentCommand is the resolved binary and argument vector for one agent.
type AgentCommand struct {
	Binary string
	Args   []string
}

// Config is the validated startup configuration.
type Config struct {
	Host                string
	Port                int
	LogLevel            slog.Level
	DBPath              string
	Token               string
	AllowInsecureRemote bool
	PIDFile             string
	ACPRequestTimeout   time.Duration
	IdleTTL             time.Duration
	Agents              map[string]AgentCommand
}

// Load reads the startup environment through getenv and returns a validated
// Config. It returns an error naming the offending variable; token values are
// never included in errors.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Host:              orDefault(getenv("AGENT_BRIDGE_HOST"), defaultHost),
		Port:              defaultPort,
		LogLevel:          slog.LevelInfo,
		DBPath:            orDefault(getenv("AGENT_BRIDGE_DB"), defaultDBPath),
		Token:             getenv("AGENT_BRIDGE_TOKEN"),
		PIDFile:           getenv("AGENT_BRIDGE_PID_FILE"),
		ACPRequestTimeout: defaultACPRequestTimeout,
		IdleTTL:           defaultIdleTTL,
	}

	if v := getenv("AGENT_BRIDGE_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("AGENT_BRIDGE_PORT must be an integer in 1..65535, got %q", v)
		}
		cfg.Port = port
	}

	if v := getenv("AGENT_BRIDGE_LOG_LEVEL"); v != "" {
		level, ok := parseLogLevel(v)
		if !ok {
			return Config{}, fmt.Errorf("AGENT_BRIDGE_LOG_LEVEL must be debug, info, warn, or error, got %q", v)
		}
		cfg.LogLevel = level
	}

	if v := getenv("AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS"); v != "" {
		d, err := parseMilliseconds("AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS", v, time.Millisecond, maxACPRequestTimeout)
		if err != nil {
			return Config{}, err
		}
		cfg.ACPRequestTimeout = d
	}

	if v := getenv("AGENT_BRIDGE_IDLE_TTL_MS"); v != "" {
		d, err := parseMilliseconds("AGENT_BRIDGE_IDLE_TTL_MS", v, 0, maxIdleTTL)
		if err != nil {
			return Config{}, err
		}
		cfg.IdleTTL = d
	}

	switch v := getenv("AGENT_BRIDGE_ALLOW_INSECURE_REMOTE"); v {
	case "", "0":
	case "1":
		cfg.AllowInsecureRemote = true
	default:
		return Config{}, fmt.Errorf("AGENT_BRIDGE_ALLOW_INSECURE_REMOTE must be 0 or 1, got %q", v)
	}

	agents, err := loadAgents(getenv)
	if err != nil {
		return Config{}, err
	}
	cfg.Agents = agents

	if cfg.Token == "" && !cfg.AllowInsecureRemote && !isLoopback(cfg.Host) {
		return Config{}, fmt.Errorf("non-loopback host %q requires AGENT_BRIDGE_TOKEN or AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1", cfg.Host)
	}

	return cfg, nil
}

// Address returns the host:port listening address.
func (c Config) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// loadAgents resolves the three supported agents, applying binary and JSON
// argument overrides. Every returned map and argument slice is freshly
// allocated so callers cannot mutate shared state.
func loadAgents(getenv func(string) string) (map[string]AgentCommand, error) {
	agents := map[string]AgentCommand{
		"claude":   {Binary: "claude-agent-acp"},
		"codex":    {Binary: "codex-acp"},
		"opencode": {Binary: "opencode", Args: []string{"acp"}},
	}

	overrides := []struct {
		agent string
		bin   string
		args  string
	}{
		{"claude", "AGENT_BRIDGE_CLAUDE_BIN", "AGENT_BRIDGE_CLAUDE_ARGS"},
		{"codex", "AGENT_BRIDGE_CODEX_BIN", "AGENT_BRIDGE_CODEX_ARGS"},
		{"opencode", "AGENT_BRIDGE_OPENCODE_BIN", "AGENT_BRIDGE_OPENCODE_ARGS"},
	}

	for _, o := range overrides {
		cmd := agents[o.agent]
		if v := getenv(o.bin); v != "" {
			cmd.Binary = v
		}
		if v := getenv(o.args); v != "" {
			args, err := parseAgentArgs(o.args, v)
			if err != nil {
				return nil, err
			}
			cmd.Args = args
		}
		agents[o.agent] = cmd
	}

	return agents, nil
}

// parseAgentArgs decodes a JSON array of strings. It rejects null, objects,
// scalars, non-string elements, and malformed JSON.
func parseAgentArgs(name, value string) ([]string, error) {
	if !strings.HasPrefix(strings.TrimSpace(value), "[") {
		return nil, fmt.Errorf("%s must be a JSON array of strings", name)
	}
	var args []string
	if err := json.Unmarshal([]byte(value), &args); err != nil {
		return nil, fmt.Errorf("%s must be a JSON array of strings: %v", name, err)
	}
	return args, nil
}

// parseMilliseconds parses a base-10 millisecond count and converts it with a
// checked bound so the millisecond-to-nanosecond multiplication cannot
// overflow. The resulting duration must fall within [min, max].
func parseMilliseconds(name, value string, min, max time.Duration) (time.Duration, error) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer number of milliseconds, got %q", name, value)
	}
	if ms < -maxDurationMillis || ms > maxDurationMillis {
		return 0, fmt.Errorf("%s is too large to represent as a duration", name)
	}
	d := time.Duration(ms) * time.Millisecond
	if d < min || d > max {
		return 0, fmt.Errorf("%s must be between %d and %d milliseconds, got %q", name, min.Milliseconds(), max.Milliseconds(), value)
	}
	return d, nil
}

// parseLogLevel maps a case-insensitive level name to its slog value.
func parseLogLevel(value string) (slog.Level, bool) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return 0, false
	}
}

// isLoopback reports whether host is provably local: a loopback IP or the
// case-insensitive name localhost. Unknown hostnames are treated as remote.
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
