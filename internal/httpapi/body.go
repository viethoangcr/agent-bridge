package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// maxJSONBodyBytes is the master 10MiB JSON body ceiling shared by every JSON
// endpoint.
const maxJSONBodyBytes int64 = 10 << 20

// decodeJSONRequest enforces the application/json Content-Type contract and
// then decodes exactly one bounded JSON object into dst, writing the same
// problem responses as DecodeJSON.
func decodeJSONRequest(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	if !requireJSONContentType(w, r) {
		return false
	}
	return DecodeJSON(w, r, limit, dst)
}

// DecodeJSON decodes exactly one JSON object from r into dst while bounding the
// body to limit bytes. It rejects malformed or empty bodies and any trailing
// JSON value, writing an RFC 9457 problem response and returning false on
// failure. An over-limit body maps to 413.
func DecodeJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		writeDecodeProblem(w, err)
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeDecodeProblem(w, err)
		return false
	}
	return true
}

// writeDecodeProblem maps a decode failure to an RFC 9457 response.
func writeDecodeProblem(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeProblem(w, http.StatusRequestEntityTooLarge, detailBodyTooLarge)
		return
	}
	writeProblem(w, http.StatusBadRequest, "invalid JSON body")
}
