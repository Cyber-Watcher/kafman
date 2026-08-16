//go:build dev

package kafka

import (
	"log/slog"
	"time"
)

// DevLatency is the delay injected before every Kafka call in dev builds.
const DevLatency = 2 * time.Second

// maybeDelay slows every operation down so spinners, loading states and
// cancellation can actually be seen while developing.
//
//	make run-dev
//
// The production build compiles this to nothing; see latency_prd.go.
func maybeDelay(operation string) {
	slog.Debug("dev latency", "operation", operation, "delay", DevLatency)
	time.Sleep(DevLatency)
}
