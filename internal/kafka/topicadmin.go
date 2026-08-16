package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/IBM/sarama"
)

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16

	// Configs are topic-level overrides such as cleanup.policy or retention.ms.
	Configs map[string]string
}

// ConfigEntry is one topic or broker setting.
type ConfigEntry struct {
	Name  string
	Value string

	// Default reports whether the value comes from the broker default rather
	// than an explicit override on this topic.
	Default bool

	// ReadOnly settings cannot be changed at runtime.
	ReadOnly  bool
	Sensitive bool

	// Source names where the value came from, for the details panel.
	Source string
}

// CreateTopic creates a topic.
func (c *Client) CreateTopic(ctx context.Context, spec TopicSpec) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("create topic")

		detail := &sarama.TopicDetail{
			NumPartitions:     spec.Partitions,
			ReplicationFactor: spec.ReplicationFactor,
		}
		if len(spec.Configs) > 0 {
			detail.ConfigEntries = make(map[string]*string, len(spec.Configs))
			for key, value := range spec.Configs {
				detail.ConfigEntries[key] = &value
			}
		}

		if err := c.admin.CreateTopic(spec.Name, detail, false); err != nil {
			return struct{}{}, fmt.Errorf("create topic %s: %w", spec.Name, err)
		}

		// Topic creation is asynchronous: the controller accepts the request and
		// propagates it. Returning immediately means the caller's next question —
		// list the topics, read the config — is answered as though the topic did
		// not exist. Waiting here is what makes "create" behave the way the user
		// expects it to.
		if err := c.awaitTopic(spec.Name); err != nil {
			return struct{}{}, err
		}

		slog.Info("created topic", "cluster", c.cluster.Name, "topic", spec.Name,
			"partitions", spec.Partitions, "replication_factor", spec.ReplicationFactor)
		return struct{}{}, nil
	})
	return err
}

// Waiting for the cluster to catch up with a structural change.
const (
	awaitAttempts = 40
	awaitInterval = 100 * time.Millisecond
)

// awaitTopic waits until a topic is fully usable.
//
// Refreshing the metadata is not enough: it only proves the broker kafman asked
// knows about the topic, while a DescribeConfigs may be served by another one
// that does not yet. Checking the config too is what makes "created" mean
// "ready to work with".
func (c *Client) awaitTopic(name string) error {
	for range awaitAttempts {
		if err := c.client.RefreshMetadata(name); err == nil {
			if partitions, err := c.client.Partitions(name); err == nil && len(partitions) > 0 {
				if c.configReadable(name) {
					return nil
				}
			}
		}
		time.Sleep(awaitInterval)
	}

	return fmt.Errorf(
		"topic %s was accepted but has not become usable after %v; the controller may still be working",
		name, awaitAttempts*awaitInterval)
}

// configReadable reports whether every broker asked can describe the topic.
func (c *Client) configReadable(name string) bool {
	resource := sarama.ConfigResource{Type: sarama.TopicResource, Name: name}
	responses, err := c.admin.DescribeConfigs(
		[]*sarama.ConfigResource{&resource}, sarama.DescribeConfigsOptions{})
	if err != nil || len(responses) == 0 {
		return false
	}
	return responses[0].ErrorCode == 0
}

// awaitPartitionCount waits until a topic reports at least want partitions.
func (c *Client) awaitPartitionCount(name string, want int32) error {
	for range awaitAttempts {
		if err := c.client.RefreshMetadata(name); err == nil {
			if partitions, err := c.client.Partitions(name); err == nil && int32(len(partitions)) >= want {
				return nil
			}
		}
		time.Sleep(awaitInterval)
	}

	return fmt.Errorf("topic %s has not reached %d partitions after %v",
		name, want, awaitAttempts*awaitInterval)
}

// DeleteTopic deletes a topic.
//
// The broker refuses unless delete.topic.enable is on, which is worth surfacing
// rather than leaving as a bare error code.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("delete topic")

		if err := c.admin.DeleteTopic(name); err != nil {
			return struct{}{}, fmt.Errorf("delete topic %s: %w", name, err)
		}
		if err := c.client.RefreshMetadata(); err != nil {
			slog.Debug("metadata refresh after deleting a topic failed", "error", err)
		}
		slog.Warn("deleted topic", "cluster", c.cluster.Name, "topic", name)
		return struct{}{}, nil
	})
	return err
}

// AddPartitions raises a topic's partition count to total.
//
// Kafka cannot reduce it, and raising it changes which partition a key routes
// to, so existing per-key ordering is broken from that point on. The UI warns
// about this before calling.
func (c *Client) AddPartitions(ctx context.Context, topic string, total int32) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("create partitions")

		current, err := c.client.Partitions(topic)
		if err != nil {
			return struct{}{}, fmt.Errorf("partitions of %s: %w", topic, err)
		}
		if total <= int32(len(current)) {
			return struct{}{}, fmt.Errorf(
				"%s already has %d partitions; Kafka cannot reduce the count",
				topic, len(current))
		}

		// A nil assignment lets the controller place the new replicas.
		if err := c.admin.CreatePartitions(topic, total, nil, false); err != nil {
			return struct{}{}, fmt.Errorf("add partitions to %s: %w", topic, err)
		}

		// The new partitions take a moment to appear, and a caller that reads the
		// count immediately would see the old one.
		if err := c.awaitPartitionCount(topic, total); err != nil {
			return struct{}{}, err
		}

		slog.Warn("added partitions", "cluster", c.cluster.Name, "topic", topic,
			"from", len(current), "to", total)
		return struct{}{}, nil
	})
	return err
}

// TopicConfig returns a topic's settings.
//
// includeDefaults decides whether inherited broker defaults are listed; without
// it only explicit overrides come back, which is what someone auditing a topic
// usually wants to see.
func (c *Client) TopicConfig(ctx context.Context, topic string, includeDefaults bool) ([]ConfigEntry, error) {
	return run(ctx, func() ([]ConfigEntry, error) {
		maybeDelay("describe configs")

		response, err := c.describeTopicConfig(topic)
		if err != nil {
			return nil, err
		}

		var entries []ConfigEntry
		for _, entry := range response.Configs {
			isDefault := entry.Default || entry.Source == sarama.SourceDefault ||
				entry.Source == sarama.SourceStaticBroker
			if isDefault && !includeDefaults {
				continue
			}

			value := entry.Value
			if entry.Sensitive {
				value = "(hidden)"
			}

			entries = append(entries, ConfigEntry{
				Name:      entry.Name,
				Value:     value,
				Default:   isDefault,
				ReadOnly:  entry.ReadOnly,
				Sensitive: entry.Sensitive,
				Source:    configSource(entry.Source),
			})
		}

		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		return entries, nil
	})
}

// describeTopicConfig fetches a topic's configuration, retrying while the broker
// answering claims not to know the topic.
//
// Kafka metadata is eventually consistent across brokers: a topic created a
// moment ago exists on the controller but a request served by another broker can
// still come back with UNKNOWN_TOPIC_OR_PARTITION. Treating that as a hard
// failure makes a freshly created topic look broken.
func (c *Client) describeTopicConfig(topic string) (*sarama.ConfigResourceResult, error) {
	resource := sarama.ConfigResource{Type: sarama.TopicResource, Name: topic}

	var lastCode sarama.KError
	for attempt := range awaitAttempts {
		// DescribeConfigs is the current call; ClusterAdmin.DescribeConfig is
		// deprecated.
		responses, err := c.admin.DescribeConfigs(
			[]*sarama.ConfigResource{&resource}, sarama.DescribeConfigsOptions{})
		if err != nil {
			return nil, fmt.Errorf("describe config of %s: %w", topic, err)
		}
		if len(responses) == 0 {
			return nil, fmt.Errorf("no configuration returned for %s", topic)
		}

		// The call succeeds even when the resource itself failed; the reason is
		// carried per resource. Ignoring it returns an empty config that looks
		// like a topic with no settings at all.
		if responses[0].ErrorCode == 0 {
			return responses[0], nil
		}
		lastCode = responses[0].ErrorCode

		// Only an unknown topic is worth waiting out; anything else is a real
		// refusal, such as a missing permission.
		if lastCode != sarama.ErrUnknownTopicOrPartition {
			return nil, fmt.Errorf("describe config of %s: %s: %w",
				topic, responses[0].ErrorMsg, lastCode)
		}
		if attempt < awaitAttempts-1 {
			time.Sleep(awaitInterval)
		}
	}

	return nil, fmt.Errorf("describe config of %s: the brokers still report it as unknown after %v: %w",
		topic, awaitAttempts*awaitInterval, lastCode)
}

// SetTopicConfig changes one setting on a topic.
//
// IncrementalAlterConfig is used rather than AlterConfig: the latter replaces
// the whole configuration, so anything not listed reverts to its default. That
// is an easy way to lose a retention setting nobody meant to touch.
func (c *Client) SetTopicConfig(ctx context.Context, topic, key, value string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("alter config")

		entries := map[string]sarama.IncrementalAlterConfigsEntry{
			key: {Operation: sarama.IncrementalAlterConfigsOperationSet, Value: &value},
		}
		err := c.admin.IncrementalAlterConfig(sarama.TopicResource, topic, entries, false)
		if err != nil {
			return struct{}{}, fmt.Errorf("set %s on %s: %w", key, topic, err)
		}

		slog.Info("changed topic config", "cluster", c.cluster.Name,
			"topic", topic, "key", key, "value", value)
		return struct{}{}, nil
	})
	return err
}

// ResetTopicConfig removes an override, returning the setting to its default.
func (c *Client) ResetTopicConfig(ctx context.Context, topic, key string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("alter config")

		entries := map[string]sarama.IncrementalAlterConfigsEntry{
			key: {Operation: sarama.IncrementalAlterConfigsOperationDelete},
		}
		err := c.admin.IncrementalAlterConfig(sarama.TopicResource, topic, entries, false)
		if err != nil {
			return struct{}{}, fmt.Errorf("reset %s on %s: %w", key, topic, err)
		}

		slog.Info("reset topic config", "cluster", c.cluster.Name, "topic", topic, "key", key)
		return struct{}{}, nil
	})
	return err
}

// DeleteRecords discards records before the given offset on each partition.
//
// This is not the same as deleting a topic: the partitions stay, their start
// offset simply moves forward. Passing an offset beyond the end deletes
// everything currently retained.
func (c *Client) DeleteRecords(ctx context.Context, topic string, offsets map[int32]int64) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("delete records")

		if err := c.admin.DeleteRecords(topic, offsets); err != nil {
			// "Request parameters do not satisfy the configured policy" is what
			// a compacted topic returns, which explains nothing on its own.
			if mentions(err, "configured policy") {
				return struct{}{}, fmt.Errorf(
					"cannot delete records from %s: Kafka refuses this on a compacted topic "+
						"(cleanup.policy contains compact)", topic)
			}
			return struct{}{}, fmt.Errorf("delete records from %s: %w", topic, err)
		}

		slog.Warn("deleted records", "cluster", c.cluster.Name,
			"topic", topic, "partitions", len(offsets))
		return struct{}{}, nil
	})
	return err
}

// TruncateTopic discards every retained record of a topic by moving each
// partition's start offset to its end.
func (c *Client) TruncateTopic(ctx context.Context, topic string) error {
	// A stale view of the leaders makes the request go to a broker that does not
	// host the partition.
	if err := c.client.RefreshMetadata(topic); err != nil {
		slog.Warn("metadata refresh before truncating failed", "topic", topic, "error", err)
	}

	partitions, err := c.DescribePartitions(ctx, topic, true)
	if err != nil {
		return err
	}

	offsets := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		// An empty partition has nothing to delete, and asking anyway can fail
		// on a partition whose replicas were only just created.
		if partition.NewestOffset <= 0 {
			continue
		}
		offsets[partition.ID] = partition.NewestOffset
	}
	if len(offsets) == 0 {
		return nil
	}
	return c.DeleteRecords(ctx, topic, offsets)
}

// configSource renders a config source for display.
func configSource(source sarama.ConfigSource) string {
	switch source {
	case sarama.SourceTopic:
		return "topic"
	case sarama.SourceDynamicBroker:
		return "broker"
	case sarama.SourceDynamicDefaultBroker:
		return "broker default"
	case sarama.SourceStaticBroker:
		return "static"
	case sarama.SourceDefault:
		return "default"
	default:
		return "unknown"
	}
}

// CommonTopicConfigs are the settings offered when creating a topic, with a
// short description each. Kafka has well over a hundred; these are the ones
// people actually set.
var CommonTopicConfigs = []struct {
	Key         string
	Description string
	Example     string
}{
	{"cleanup.policy", "delete old records, or compact by key", "delete"},
	{"retention.ms", "how long records are kept", "604800000"},
	{"retention.bytes", "how much is kept per partition, -1 for unlimited", "-1"},
	{"min.insync.replicas", "replicas that must acknowledge a write", "2"},
	{"max.message.bytes", "largest record the topic accepts", "1048588"},
	{"segment.ms", "how often a new log segment is rolled", "604800000"},
	{"compression.type", "producer, gzip, snappy, lz4, zstd or uncompressed", "producer"},
}
