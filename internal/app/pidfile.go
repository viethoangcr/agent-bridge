package app

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
)

// writePIDFile creates path exclusively with mode 0600 and writes the decimal
// process id followed by a newline. It fails rather than replacing an existing
// file so a second bridge cannot claim the same PID file.
func writePIDFile(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// removePIDFile deletes path, treating an already-absent file as success.
func removePIDFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removePIDFileLogged removes path when configured, logging any failure.
func removePIDFileLogged(logger *slog.Logger, path string) {
	if path == "" {
		return
	}
	if err := removePIDFile(path); err != nil {
		logger.Error("removing pid file", "error", err)
	}
}
