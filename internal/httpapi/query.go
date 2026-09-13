package httpapi

import (
	"errors"
	"net/http"
	"net/url"
)

// errUnknownQueryKey marks a query string that carries a key outside the
// caller's allowlist.
var errUnknownQueryKey = errors.New("unknown query key")

// parseQuery parses r's raw query string and rejects malformed escapes and any
// key outside allowed. Raw parsing is used so a malformed escape is rejected
// rather than silently dropped with its key.
func parseQuery(r *http.Request, allowed ...string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	permitted := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		permitted[key] = struct{}{}
	}
	for key := range values {
		if _, ok := permitted[key]; !ok {
			return nil, errUnknownQueryKey
		}
	}
	return values, nil
}

// singleQuery returns the one non-empty value for key. It reports false when
// the key is absent, repeated, or empty.
func singleQuery(values url.Values, key string) (string, bool) {
	list, present := values[key]
	if !present || len(list) != 1 || list[0] == "" {
		return "", false
	}
	return list[0], true
}

// requireNoQuery rejects any query string on endpoints that document none,
// writing a 400 problem with detail and returning false.
func requireNoQuery(w http.ResponseWriter, r *http.Request, detail string) bool {
	if r.URL.RawQuery != "" {
		writeProblem(w, http.StatusBadRequest, detail)
		return false
	}
	return true
}
