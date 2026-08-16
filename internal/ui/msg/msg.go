// Package msg holds the messages exchanged between the root model and the tabs.
//
// They live in their own package so a tab can emit one without importing the ui
// package, which would be an import cycle.
package msg

import (
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// Tabs never touch the Kafka client or the config file directly: they ask by
// emitting one of these, and the root performs the change. That keeps ownership
// of the connection and of the config in one place, which matters because a
// cluster switch has to tear down and rebuild everything.

// SwitchCluster asks the root to make a cluster current.
type SwitchCluster struct {
	Name string
}

// ClusterSwitched announces that the switch completed and clients were
// rebuilt. Tabs reset their state when they see it.
type ClusterSwitched struct {
	Cluster *config.Cluster
}

// Connected reports a successful connection to the current cluster, carrying
// the live client.
type Connected struct {
	Client  kafka.Facade
	Info    kafka.ClusterInfo
	Latency time.Duration
}

// ConnectFailed reports a failed connection to the current cluster.
type ConnectFailed struct {
	Err       error
	Diagnosis string
}

// TestCluster asks the root to test a cluster other than the current one.
type TestCluster struct {
	Name string

	// Cluster, when set, is tested instead of the stored definition — this is
	// how the edit form tests changes before they are saved.
	Cluster config.Cluster
}

// ClusterTested carries the result of such a test.
type ClusterTested struct {
	Name   string
	Result kafka.ConnCheck
}

// ConfigChanged announces that the configuration was modified and saved, so
// screens showing it can refresh.
type ConfigChanged struct{}

// Notify asks the root to show a message on the context line.
type Notify struct {
	Text  string
	Level NotifyLevel
}

// NotifyLevel mirrors chrome.Level without importing it into every tab.
type NotifyLevel int

// Notification levels.
const (
	NotifyInfo NotifyLevel = iota
	NotifySuccess
	NotifyWarning
	NotifyError
)

// ---------------------------------------------------------------- overview ---

// The Overview tab loads its panels independently rather than waiting for the
// slowest one: cluster metadata arrives in milliseconds, log-dir sizes and
// consumer lag take considerably longer.

// HealthLoaded carries the replication summary.
type HealthLoaded struct {
	Health kafka.Health
	Err    error
}

// TopicsLoaded carries the topic list built from cached metadata.
type TopicsLoaded struct {
	Topics []kafka.Topic
	Err    error
}

// SizesLoaded carries on-disk sizes per topic and per broker.
type SizesLoaded struct {
	Topics  map[string]int64
	Brokers map[int32]int64
	Err     error
}

// GroupsLoaded carries consumer groups with their lag.
type GroupsLoaded struct {
	Groups []kafka.ConsumerGroup
	Err    error
}

// Refresh asks the active tab to reload.
type Refresh struct{}

// FocusTopic asks the Messages tab to open a topic (used by Enter on Overview).
type FocusTopic struct{ Topic string }

// FocusGroup asks the Manage tab to open a consumer group.
type FocusGroup struct{ Group string }
