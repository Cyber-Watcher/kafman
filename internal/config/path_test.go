package config

import (
	"path/filepath"
	"testing"
)

func TestResolvePath(t *testing.T) {
	t.Run("flag wins over everything", func(t *testing.T) {
		t.Setenv(EnvPath, "/from/env.yaml")
		if got := ResolvePath("/from/flag.yaml"); got != "/from/flag.yaml" {
			t.Errorf("got %q, want the flag value", got)
		}
	})

	t.Run("environment wins over the default", func(t *testing.T) {
		t.Setenv(EnvPath, "/from/env.yaml")
		if got := ResolvePath(""); got != "/from/env.yaml" {
			t.Errorf("got %q, want the environment value", got)
		}
	})

	t.Run("falls back to the default path", func(t *testing.T) {
		t.Setenv(EnvPath, "")
		t.Setenv("XDG_CONFIG_HOME", "/xdg")
		want := filepath.Join("/xdg", "kafman", FileName)
		if got := ResolvePath(""); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

func TestDefaultPath(t *testing.T) {
	t.Run("honours XDG_CONFIG_HOME", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/somewhere/config")
		want := filepath.Join("/somewhere/config", "kafman", FileName)
		if got := DefaultPath(); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("falls back to the home directory", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		t.Setenv("HOME", "/home/tester")
		want := filepath.Join("/home/tester", ".config", "kafman", FileName)
		if got := DefaultPath(); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
