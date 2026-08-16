package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func logWarn(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...))
}

// FileMode is deliberately owner-only: the config file holds SASL passwords and
// certificate passphrases.
const FileMode os.FileMode = 0o600

// DirMode is the mode used when creating the config directory.
const DirMode os.FileMode = 0o700

// Load reads and validates the configuration at path.
//
// When the file does not exist, a default configuration is written there and
// returned, so a first run works without the user preparing anything.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the path comes from the user, by design
	switch {
	case err == nil:
		// fall through to parsing
	case errors.Is(err, os.ErrNotExist):
		cfg := New()
		cfg.path = path
		if err := cfg.Save(); err != nil {
			return nil, fmt.Errorf("create default config: %w", err)
		}
		cfg.finalise()
		return cfg, nil
	default:
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	cfg := &Config{path: path}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s:\n%w", path, err)
	}
	cfg.finalise()

	warnIfWorldReadable(path)
	return cfg, nil
}

// LoadFrom parses a configuration from bytes, without touching the filesystem.
// dir stands in for the config directory when resolving relative certificate
// paths. Intended for tests.
func LoadFrom(data []byte, dir string) (*Config, error) {
	cfg := &Config{path: filepath.Join(dir, FileName)}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.finalise()
	return cfg, nil
}

// finalise runs the steps that must happen after validation succeeds.
func (c *Config) finalise() {
	dir := c.Dir()
	for i := range c.Clusters {
		c.Clusters[i].resolvePaths(dir)
	}
}

// Save writes the configuration back to its file.
//
// The write is atomic: a temporary file in the same directory is renamed over
// the target, so an interrupted write cannot leave a truncated config behind.
func (c *Config) Save() error {
	if c.path == "" {
		return errors.New("config has no path to save to")
	}

	dir := c.Dir()
	if err := os.MkdirAll(dir, DirMode); err != nil {
		return fmt.Errorf("create config directory %s: %w", dir, err)
	}

	data, err := c.Marshal()
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".config-*.yaml.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		// No-op once the rename succeeded; cleans up on every failure path.
		_ = os.Remove(tmpName)
	}()

	if err := tmp.Chmod(FileMode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set permissions on %s: %w", tmpName, err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmpName, err)
	}
	// Flush to disk before the rename, otherwise a crash can leave an empty file.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, c.path); err != nil {
		return fmt.Errorf("replace %s: %w", c.path, err)
	}
	return nil
}

// Marshal renders the configuration as YAML, with a header explaining where the
// file came from.
func (c *Config) Marshal() ([]byte, error) {
	body, err := yaml.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	header := "" +
		"# kafman configuration.\n" +
		"# Written by kafman; hand edits are preserved except for comments.\n" +
		"# This file contains passwords: keep it readable only by you (chmod 600).\n\n"
	return append([]byte(header), body...), nil
}

// warnIfWorldReadable logs when the config is more permissive than it should be.
// It does not change the file: the user may have chosen those permissions.
func warnIfWorldReadable(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if info.Mode().Perm()&0o077 != 0 {
		logWarn("config file %s is readable by other users (mode %04o); it contains passwords — consider: chmod 600 %s",
			path, info.Mode().Perm(), path)
	}
}
