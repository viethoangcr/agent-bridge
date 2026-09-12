package acpruntime

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"
)

const (
	// maxIDTokenBytes bounds every raw client JSON-RPC ID token, including a
	// string token's surrounding quotes and escapes.
	maxIDTokenBytes = 128

	// maxIDExponent bounds the magnitude of a numeric ID's base-10 exponent.
	maxIDExponent = 1_000_000
)

// ClientKind identifies one of the three authoritative client envelope forms.
type ClientKind string

const (
	ClientRequest      ClientKind = "request"
	ClientNotification ClientKind = "notification"
	ClientResponse     ClientKind = "response"
)

// Pending is the bounded metadata classified from a client envelope. ID is the
// raw ID token (nil for notifications); Lifecycle, SessionID, and CWD are
// request-only correlation metadata.
type Pending struct {
	ID        json.RawMessage
	Lifecycle Lifecycle
	SessionID *string
	CWD       *string
}

// ClassifyClientEnvelope reports the kind and bounded metadata of one client
// payload. It is a pure function: it performs no I/O and holds no writer or
// correlation state. Invalid UTF-8, a missing or wrong jsonrpc version, arrays
// and scalars, a null or malformed ID/method, an ambiguous response shape, and
// over-limit ID/session/cwd values are rejected with ErrInvalidEnvelope before
// any reservation or write.
func ClassifyClientEnvelope(payload json.RawMessage) (ClientKind, Pending, error) {
	if !utf8.Valid(payload) {
		return "", Pending{}, invalidEnvelope("payload is not valid UTF-8")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return "", Pending{}, invalidEnvelope("payload is not a JSON object")
	}
	if err := requireJSONRPC20(obj["jsonrpc"]); err != nil {
		return "", Pending{}, err
	}

	methodRaw, hasMethod := obj["method"]
	idRaw, hasID := obj["id"]
	_, hasResult := obj["result"]
	_, hasError := obj["error"]

	method := ""
	if hasMethod {
		if err := json.Unmarshal(methodRaw, &method); err != nil || method == "" {
			return "", Pending{}, invalidEnvelope("method is not a non-empty string")
		}
	}

	var kind ClientKind
	switch {
	case hasMethod && hasID:
		kind = ClientRequest
	case hasMethod:
		kind = ClientNotification
	case hasID:
		if hasResult == hasError {
			return "", Pending{}, invalidEnvelope("response must carry exactly one of result or error")
		}
		kind = ClientResponse
	default:
		return "", Pending{}, invalidEnvelope("envelope has neither method nor id")
	}

	pending := Pending{Lifecycle: LifecycleNone}
	if kind != ClientNotification {
		if _, err := idKey(idRaw); err != nil {
			return "", Pending{}, err
		}
		pending.ID = cloneRaw(idRaw)
	}
	if kind != ClientRequest && !sessionScopedMethods[method] {
		return kind, pending, nil
	}

	life, sessionID, cwd, err := clientSessionMeta(obj["params"], method, kind == ClientRequest)
	if err != nil {
		return "", Pending{}, err
	}
	pending.Lifecycle = life
	pending.SessionID = sessionID
	pending.CWD = cwd
	return kind, pending, nil
}

// requireJSONRPC20 enforces the JSON-RPC 2.0 version member on every client
// envelope. A missing, non-string, or non-"2.0" value is invalid.
func requireJSONRPC20(raw json.RawMessage) error {
	if len(raw) == 0 {
		return invalidEnvelope("jsonrpc is missing")
	}
	var version string
	if err := json.Unmarshal(raw, &version); err != nil || version != "2.0" {
		return invalidEnvelope(`jsonrpc must be exactly "2.0"`)
	}
	return nil
}

// clientSessionMeta inspects only the bounded ACP v1 §5 session fields: cwd on
// request lifecycle methods and sessionId on session-scoped messages.
func clientSessionMeta(params json.RawMessage, method string, isRequest bool) (Lifecycle, *string, *string, error) {
	life := LifecycleNone
	if isRequest {
		switch method {
		case "session/new":
			life = LifecycleNew
		case "session/load":
			life = LifecycleLoad
		case "session/resume":
			life = LifecycleResume
		}
	}

	var sessionID *string
	if sessionScopedMethods[method] || life != LifecycleNone {
		var err error
		sessionID, err = inspectStringParam(params, "sessionId", maxSessionIDBytes)
		if err != nil {
			return "", nil, nil, err
		}
	}

	var cwd *string
	if life == LifecycleNew || life == LifecycleLoad || life == LifecycleResume {
		var err error
		cwd, err = inspectStringParam(params, "cwd", maxCWDBytes)
		if err != nil {
			return "", nil, nil, err
		}
	}
	return life, sessionID, cwd, nil
}

// inspectStringParam returns a bounded string field of a JSON object params. It
// returns nil when the field is absent or null, and ErrInvalidEnvelope when the
// field exists but is not a string or exceeds limit bytes.
func inspectStringParam(params json.RawMessage, key string, limit int) (*string, error) {
	if len(params) == 0 || isJSONNull(params) {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(params, &fields); err != nil {
		return nil, nil
	}
	raw, ok := fields[key]
	if !ok || isJSONNull(raw) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, invalidEnvelope(key + " is not a string")
	}
	if len(s) > limit {
		return nil, invalidEnvelope(fmt.Sprintf("%s exceeds %d bytes", key, limit))
	}
	return &s, nil
}

// idKey returns the bounded canonical correlation key of one raw ID token.
// String IDs key on their exact decoded value and never equal numeric IDs.
// Numeric IDs key on a lexical sign/significant-digits/scale tuple so that 1,
// 1.0, and 1e0 correlate; the canonicalizer is O(token length) and never uses
// float64, arbitrary precision, or power expansion.
func idKey(id json.RawMessage) (string, error) {
	if len(id) == 0 || len(id) > maxIDTokenBytes {
		return "", invalidEnvelope("ID token is empty or exceeds 128 bytes")
	}
	if id[0] == '"' {
		var s string
		if err := json.Unmarshal(id, &s); err != nil {
			return "", invalidEnvelope("string ID is not a valid JSON string")
		}
		return "s:" + s, nil
	}
	return canonicalNumber(id)
}

// canonicalNumber returns the canonical key of one raw JSON number token.
func canonicalNumber(raw []byte) (string, error) {
	i := 0
	neg := false
	if raw[i] == '-' {
		neg = true
		i++
		if i == len(raw) {
			return "", invalidEnvelope("numeric ID has no digits")
		}
	}

	intStart := i
	for i < len(raw) && isDigit(raw[i]) {
		i++
	}
	intEnd := i
	if intEnd == intStart {
		return "", invalidEnvelope("numeric ID has no integer digits")
	}
	if intEnd-intStart > 1 && raw[intStart] == '0' {
		return "", invalidEnvelope("numeric ID has a leading zero")
	}

	fracStart, fracEnd := i, i
	if i < len(raw) && raw[i] == '.' {
		i++
		fracStart = i
		for i < len(raw) && isDigit(raw[i]) {
			i++
		}
		fracEnd = i
		if fracEnd == fracStart {
			return "", invalidEnvelope("numeric ID has an empty fraction")
		}
	}

	exp := 0
	if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
		i++
		expNeg := false
		if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
			expNeg = raw[i] == '-'
			i++
		}
		expStart := i
		for i < len(raw) && isDigit(raw[i]) {
			exp = exp*10 + int(raw[i]-'0')
			if exp > maxIDExponent {
				return "", invalidEnvelope("numeric ID exponent exceeds 1000000")
			}
			i++
		}
		if i == expStart {
			return "", invalidEnvelope("numeric ID has an empty exponent")
		}
		if expNeg {
			exp = -exp
		}
	}
	if i != len(raw) {
		return "", invalidEnvelope("numeric ID has trailing characters")
	}

	// Significant digits span the integer and fraction digits with the decimal
	// point removed. Leading zeros are insignificant; trailing zeros fold into
	// the scale.
	digitsStart, digitsEnd := intStart, fracEnd
	first := -1
	for k := digitsStart; k < digitsEnd; k++ {
		if raw[k] != '.' && raw[k] != '0' {
			first = k
			break
		}
	}
	if first < 0 {
		return "n:0", nil
	}
	last := first
	trailingZeros := 0
	for k := digitsEnd - 1; k > first; k-- {
		if raw[k] == '.' {
			continue
		}
		if raw[k] != '0' {
			last = k
			break
		}
		trailingZeros++
	}

	scale := exp - (fracEnd - fracStart) + trailingZeros
	buf := make([]byte, 0, len(raw)+2)
	buf = append(buf, 'n', ':')
	if neg {
		buf = append(buf, '-')
	}
	for k := first; k <= last; k++ {
		if raw[k] != '.' {
			buf = append(buf, raw[k])
		}
	}
	buf = append(buf, 'e')
	buf = strconv.AppendInt(buf, int64(scale), 10)
	return string(buf), nil
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// cloneRaw copies a raw token so retained metadata does not alias the request
// payload's backing array.
func cloneRaw(raw json.RawMessage) json.RawMessage {
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

// invalidEnvelope wraps ErrInvalidEnvelope with a reason suitable for logs; the
// HTTP layer matches the sentinel with errors.Is.
func invalidEnvelope(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidEnvelope, reason)
}
