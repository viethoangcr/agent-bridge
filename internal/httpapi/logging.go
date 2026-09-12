package httpapi

import (
	"log/slog"
	"net/http"
	"time"
)

// statusWriter wraps a ResponseWriter to capture the final response status for
// request logging. It preserves http.Flusher so streamed responses (future SSE)
// keep working through the logging middleware. It never exposes request or
// response headers or bodies to the logger.
type statusWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the first status and forwards it to the underlying writer.
// Later calls are ignored so a handler cannot overwrite the logged status.
func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write records an implicit 200 when the handler has not set a status.
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Flush lets streaming handlers flush through the wrapper. It is a no-op when
// the underlying writer does not implement http.Flusher.
func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// requestLogger returns middleware that logs one structured record per request
// with method, full request URI, final status, and integer latency in
// milliseconds. A nil log sinks to io.Discard so callers without a configured
// logger stay safe. Authorization, headers, and bodies are never logged.
func requestLogger(log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		log.Info("request",
			"method", r.Method,
			"uri", r.URL.RequestURI(),
			"status", sw.status,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}
