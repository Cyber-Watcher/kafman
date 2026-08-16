package logging

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupWritesToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "kafman.log")

	closer, err := Setup(path, slog.LevelInfo)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	slog.Info("hello from the test")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the log file was not created: %v", err)
	}
	if !strings.Contains(string(content), "hello from the test") {
		t.Errorf("log entry missing from %s:\n%s", path, content)
	}
}

func TestSetupWithEmptyPathDiscards(t *testing.T) {
	closer, err := Setup("", slog.LevelInfo)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	slog.Info("discarded")
	if err := closer.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestDefaultPathHonoursXDG(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/state")
	want := filepath.Join("/state", "kafman", "kafman.log")
	if got := DefaultPath(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
