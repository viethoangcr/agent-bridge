// Package projectconfig validates and atomically replaces the per-project MCP
// and skills configuration files under
// {resolved directory}/.agent-bridge/config/{mcp,skills}.json.
package projectconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

const (
	mcpKind    = "mcp"
	skillsKind = "skills"
	configDir  = ".agent-bridge"
	configSub  = "config"
	configMode = 0o600
)

// MCPServer is one MCP server entry in the mcp.json object.
type MCPServer struct {
	Command string            `json:"command"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
}

// mcpServerWire captures the optional args and env fields as raw JSON so an
// explicit null stays distinguishable from an omitted field: encoding/json
// would otherwise decode both to a nil slice or map.
type mcpServerWire struct {
	Command string          `json:"command"`
	Args    json.RawMessage `json:"args"`
	Env     json.RawMessage `json:"env"`
}

// Service reads and writes project config files with atomic replacement. It
// shares the filesystem service's process-wide mutation mutex so config writes
// never interleave with other bridge-originated filesystem mutations.
type Service struct {
	files     *filesystem.Service
	mutations *sync.Mutex
}

// New constructs a Service from the shared path resolver and mutation mutex.
func New(files *filesystem.Service, mutations *sync.Mutex) *Service {
	return &Service{files: files, mutations: mutations}
}

// Get returns the stored config bytes, or a not_found error when the file is
// absent.
func (s *Service) Get(kind, directory string) (json.RawMessage, error) {
	path, err := s.configPath(kind, directory)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, mapPathError(err)
	}
	return json.RawMessage(data), nil
}

// Put validates body and atomically replaces the config file with canonical
// indented JSON. Writes serialize on the injected mutation mutex.
func (s *Service) Put(kind, directory string, body json.RawMessage) error {
	path, err := s.configPath(kind, directory)
	if err != nil {
		return err
	}

	var canonical []byte
	if kind == mcpKind {
		canonical, err = validateMCP(body)
	} else {
		canonical, err = validateSkills(body)
	}
	if err != nil {
		return err
	}

	s.mutations.Lock()
	defer s.mutations.Unlock()
	return writeAtomic(path, canonical)
}

// Delete removes the config file and syncs its parent directory. Deleting a
// missing file is not_found.
func (s *Service) Delete(kind, directory string) error {
	path, err := s.configPath(kind, directory)
	if err != nil {
		return err
	}

	s.mutations.Lock()
	defer s.mutations.Unlock()
	if err := os.Remove(path); err != nil {
		return mapPathError(err)
	}
	return syncDir(filepath.Dir(path))
}

// configPath resolves directory through the filesystem service and appends the
// fixed config suffix. It never accepts an arbitrary filename.
func (s *Service) configPath(kind, directory string) (string, error) {
	switch kind {
	case mcpKind, skillsKind:
	default:
		return "", invalidError("config kind is not supported")
	}
	resolved, err := s.files.Resolve(directory)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, configDir, configSub, kind+".json"), nil
}

// validateMCP rejects a non-object, unknown fields, empty commands, explicit
// null optional fields, and non-string args/env, then returns canonical
// indented JSON with a trailing newline.
func validateMCP(body json.RawMessage) ([]byte, error) {
	if !isJSONObject(body) {
		return nil, invalidError("mcp config must be a JSON object")
	}
	var wire map[string]mcpServerWire
	if err := decodeStrict(body, &wire); err != nil {
		return nil, err
	}
	servers := make(map[string]MCPServer, len(wire))
	for name, server := range wire {
		if server.Command == "" {
			return nil, invalidError("mcp server " + name + " requires a non-empty command")
		}
		args, err := optionalStrings(server.Args, name, "args")
		if err != nil {
			return nil, err
		}
		env, err := optionalStringMap(server.Env, name)
		if err != nil {
			return nil, err
		}
		servers[name] = MCPServer{Command: server.Command, Args: args, Env: env}
	}
	return marshalCanonical(servers)
}

// optionalStrings decodes an omitted-or-array raw field. An explicit null or
// any non-array value is invalid; an explicit [] yields a non-nil empty slice.
func optionalStrings(raw json.RawMessage, serverName, field string) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, invalidError("mcp server " + serverName + " " + field + " must be an array of strings")
	}
	return values, nil
}

// optionalStringMap decodes an omitted-or-object raw field. An explicit null or
// any non-object value is invalid; an explicit {} yields a non-nil empty map.
func optionalStringMap(raw json.RawMessage, serverName string) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	var values map[string]string
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, invalidError("mcp server " + serverName + " env must be an object of strings")
	}
	return values, nil
}

// validateSkills requires a JSON object with arbitrary values and returns
// canonical indented JSON with a trailing newline.
func validateSkills(body json.RawMessage) ([]byte, error) {
	if !isJSONObject(body) {
		return nil, invalidError("skills config must be a JSON object")
	}
	var skills map[string]json.RawMessage
	if err := decodeStrict(body, &skills); err != nil {
		return nil, err
	}
	return marshalCanonical(skills)
}

// isJSONObject reports whether body's first non-whitespace byte opens a JSON
// object, rejecting null, arrays, and scalars before decoding.
func isJSONObject(body []byte) bool {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// decodeStrict decodes exactly one JSON value into dst, rejecting unknown
// struct fields and trailing values.
func decodeStrict(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return invalidError("config is not valid JSON")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return invalidError("config must contain exactly one JSON value")
	}
	return nil
}

// marshalCanonical serializes validated values with indentation and one
// trailing newline.
func marshalCanonical(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, invalidError("config could not be encoded")
	}
	return append(data, '\n'), nil
}

// writeAtomic creates parents, writes data to a same-directory mode-0600
// temporary file, fsyncs it, renames it over path, and fsyncs the directory.
// The temporary file is always removed on failure, and the target is never
// partially written.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return internalError(err)
	}

	tmp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return internalError(err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := tmp.Chmod(configMode); err != nil {
		tmp.Close()
		return internalError(err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return internalError(err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return internalError(err)
	}
	if err := tmp.Close(); err != nil {
		return internalError(err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return internalError(err)
	}
	return syncDir(dir)
}

// syncDir flushes a directory's entries to stable storage.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return internalError(err)
	}
	if err := handle.Sync(); err != nil {
		handle.Close()
		return internalError(err)
	}
	if err := handle.Close(); err != nil {
		return internalError(err)
	}
	return nil
}

func mapPathError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return notFoundError()
	}
	return internalError(err)
}

func invalidError(message string) error {
	return &filesystem.Error{Kind: filesystem.ErrorKindInvalid, Message: message}
}

func notFoundError() error {
	return &filesystem.Error{Kind: filesystem.ErrorKindNotFound, Message: "config not found"}
}

func internalError(err error) error {
	return &filesystem.Error{Kind: filesystem.ErrorKindInternal, Message: "config operation failed", Err: err}
}
