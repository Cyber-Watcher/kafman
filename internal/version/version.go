// Package version exposes build metadata, injected at link time.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Overridden via -ldflags "-X github.com/Cyber-Watcher/kafman/internal/version.Version=..."
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Name is the application name used in the UI, the client ID and the config path.
const Name = "kafman"

// String returns a single-line version summary.
func String() string {
	return fmt.Sprintf("%s %s (commit %s, built %s, %s)", Name, Version, Commit, Date, runtime.Version())
}

// Full returns a multi-line version report.
func Full() string {
	s := fmt.Sprintf(""+
		"%s %s\n"+
		"  commit  %s\n"+
		"  built   %s\n"+
		"  go      %s\n"+
		"  os/arch %s/%s\n",
		Name, Version, Commit, Date, runtime.Version(), runtime.GOOS, runtime.GOARCH)

	// The Kafka client is linked in from stage 2 onwards.
	if v, ok := dependencyVersion("github.com/IBM/sarama"); ok {
		s += fmt.Sprintf("  sarama  %s\n", v)
	}
	return s
}

// dependencyVersion reports the version of a module from the build info embedded
// in the binary. The second result is false when the module is not linked in or
// the binary was built without module information.
func dependencyVersion(path string) (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, dep := range info.Deps {
		if dep.Path == path {
			return dep.Version, true
		}
	}
	return "", false
}
