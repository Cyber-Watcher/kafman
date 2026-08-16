// Package config loads and stores the kafman configuration.
//
// Stage 0 provides only path resolution. The configuration model, validation
// and persistence arrive in stage 1; see docs/project_plan.md.
package config

import (
	"os"
	"path/filepath"

	"github.com/Cyber-Watcher/kafman/internal/version"
)

// EnvPath is the environment variable that overrides the config file location.
const EnvPath = "KAFMAN_CONFIG"

// FileName is the config file name inside the application config directory.
const FileName = "config.yaml"

// DefaultPath returns $XDG_CONFIG_HOME/kafman/config.yaml, falling back to
// ~/.config/kafman/config.yaml.
func DefaultPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			// Last resort: the working directory, so the app still starts.
			return FileName
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, version.Name, FileName)
}

// ResolvePath picks the config file location, in order of precedence:
// the --config flag, then $KAFMAN_CONFIG, then the default path.
func ResolvePath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if env := os.Getenv(EnvPath); env != "" {
		return env
	}
	return DefaultPath()
}
