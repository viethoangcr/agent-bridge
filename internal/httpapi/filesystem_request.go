package httpapi

import (
	"errors"
	"io"
	"net/http"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

// maxFSFileBytes and maxFSUploadBytes are the production raw file PUT and
// upload limits, owned by the filesystem package. Server copies them into
// overridable fields so tests can inject small limits.
const (
	maxFSFileBytes   int64 = filesystem.MaxFileBytes
	maxFSUploadBytes int64 = filesystem.MaxFileBytes
)

// errFSBodyTooLarge marks a request body that exceeded the injected limit. The
// body reader returns it at limit+1 so a mutation is aborted before it can
// touch the destination.
var errFSBodyTooLarge = errors.New("filesystem request body too large")

// detailInvalidFilesystemQuery is the canonical 400 detail for a malformed
// filesystem query.
const detailInvalidFilesystemQuery = "invalid filesystem query"

type fsMkdirRequest struct {
	Directory string `json:"directory"`
	Name      string `json:"name"`
}

type fsMoveRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// fsRequiredQuery parses an allowlisted query whose first key is required.
// Every supplied key may appear at most once with a non-empty value; unknown,
// repeated, empty, or missing-required values write a 400 problem and return
// false. Raw parsing is used so a malformed escape is rejected rather than
// silently dropped with its key.
func fsRequiredQuery(w http.ResponseWriter, r *http.Request, required string, optional ...string) (map[string]string, bool) {
	values, err := parseQuery(r, append([]string{required}, optional...)...)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, detailInvalidFilesystemQuery)
		return nil, false
	}
	for _, list := range values {
		if len(list) != 1 || list[0] == "" {
			writeProblem(w, http.StatusBadRequest, detailInvalidFilesystemQuery)
			return nil, false
		}
	}
	if _, present := values[required]; !present {
		writeProblem(w, http.StatusBadRequest, detailInvalidFilesystemQuery)
		return nil, false
	}
	out := make(map[string]string, len(values))
	for key, list := range values {
		out[key] = list[0]
	}
	return out, true
}

// fsBodyCounter bounds a request body to limit bytes. It returns
// errFSBodyTooLarge at limit+1 so callers can abort a mutation before it
// commits and map the excess to 413.
type fsBodyCounter struct {
	r     io.Reader
	n     int64
	limit int64
}

func (c *fsBodyCounter) Read(p []byte) (int, error) {
	remaining := c.limit + 1 - c.n
	if remaining <= 0 {
		return 0, errFSBodyTooLarge
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := c.r.Read(p)
	c.n += int64(n)
	if c.n > c.limit {
		return n, errFSBodyTooLarge
	}
	return n, err
}

// exceeded reports whether more than the active limit was supplied.
func (c *fsBodyCounter) exceeded() bool { return c.n > c.limit }

// limitFSBody wraps r's body with http.MaxBytesReader as a hard backstop and
// returns a counter that reports the exact over-limit at limit+1.
func limitFSBody(w http.ResponseWriter, r *http.Request, limit int64) *fsBodyCounter {
	r.Body = http.MaxBytesReader(w, r.Body, limit+1)
	return &fsBodyCounter{r: r.Body, limit: limit}
}
