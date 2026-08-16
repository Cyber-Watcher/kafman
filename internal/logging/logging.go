// Package logging configures the application logger.
//
// A TUI owns the terminal, so nothing may be written to stdout or stderr while
// the program runs: any stray line corrupts the rendered frame. All logging
// therefore goes to a file.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Cyber-Watcher/kafman/internal/version"
)

// DefaultPath returns the log file path, following the XDG state directory
// convention: $XDG_STATE_HOME/kafman/kafman.log, or ~/.local/state/kafman/kafman.log.
func DefaultPath() string {
	dir := os.Getenv("XDG_STATE_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return filepath.Join(os.TempDir(), version.Name+".log")
		}
		dir = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(dir, version.Name, version.Name+".log")
}

// Setup opens the log file and installs it as the default slog handler.
// The returned closer must be called on shutdown.
//
// An empty path discards all output, which is what tests and one-shot commands want.
func Setup(path string, level slog.Level) (io.Closer, error) {
	if path == "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
		return io.NopCloser(nil), nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", path, err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: level})))
	slog.Info("logging started", "version", version.Version, "pid", os.Getpid())

	return f, nil
}
