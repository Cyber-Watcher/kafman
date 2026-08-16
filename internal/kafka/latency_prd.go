//go:build !dev

package kafka

// maybeDelay is a no-op outside dev builds; the compiler removes the calls.
// The dev counterpart lives in latency_dev.go.
func maybeDelay(string) {}
