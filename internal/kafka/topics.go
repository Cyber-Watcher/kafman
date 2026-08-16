package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

// Topic is one topic as seen from the cluster metadata.
//
// The fields here all come from the metadata sarama already caches, so building
// the whole list costs one refresh rather than a request per topic. Anything
// that needs a round trip — offsets, sizes, configs — is fetched separately and
// only for what is on screen.
type Topic struct {
	Name       string
	Partitions int

	// ReplicationFactor is the smallest replica count across the partitions;
	// a topic mid-reassignment can have partitions that differ.
	ReplicationFactor int

	// InSync and TotalReplicas aggregate the ISR state over all partitions, and
	// are what the list column "ISR 36/36" is built from.
	InSync        int
	TotalReplicas int

	// UnderReplicated is the number of partitions whose ISR is short of their
	// replica list.
	UnderReplicated int

	// Offline is the number of partitions with no leader at all.
	Offline int

	Internal bool
}

// Healthy reports whether every partition of the topic is fully in sync.
func (t Topic) Healthy() bool { return t.UnderReplicated == 0 && t.Offline == 0 }

// Partition is the detail of a single partition.
type Partition struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32

	// OldestOffset and NewestOffset are only filled by the calls that ask for
	// them; they cost a request per partition.
	OldestOffset int64
	NewestOffset int64
}

// UnderReplicated reports whether the partition is missing in-sync replicas.
func (p Partition) UnderReplicated() bool { return len(p.ISR) < len(p.Replicas) }

// Offline reports whether the partition has no leader.
func (p Partition) Offline() bool { return p.Leader < 0 }

// Messages is the number of records currently retained, which is the offset
// range rather than everything ever written.
func (p Partition) Messages() int64 {
	if p.NewestOffset < p.OldestOffset {
		return 0
	}
	return p.NewestOffset - p.OldestOffset
}

// ListTopics returns every topic with its replication state.
//
// It refreshes the metadata once and then reads from sarama's cache, so the
// cost is one round trip regardless of how many topics exist.
func (c *Client) ListTopics(ctx context.Context, includeInternal bool) ([]Topic, error) {
	return run(ctx, func() ([]Topic, error) {
		maybeDelay("list topics")

		// See ClusterHealth: a leaderless partition makes RefreshMetadata report
		// an error even though the metadata is usable. Hiding the whole topic
		// list because one partition is in trouble is the wrong trade.
		if err := c.client.RefreshMetadata(); err != nil {
			slog.Warn("metadata refresh incomplete, listing what is available",
				"cluster", c.cluster.Name, "error", err)
		}

		names, err := c.client.Topics()
		if err != nil {
			return nil, fmt.Errorf("list topics: %w", err)
		}

		topics := make([]Topic, 0, len(names))
		for _, name := range names {
			internal := isInternalTopic(name)
			if internal && !includeInternal {
				continue
			}
			topic, err := c.topicFromMetadata(name)
			if err != nil {
				// One broken topic must not hide the rest of the cluster.
				continue
			}
			topic.Internal = internal
			topics = append(topics, topic)
		}

		sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
		return topics, nil
	})
}

// topicFromMetadata builds a Topic from the cached metadata. No network I/O.
func (c *Client) topicFromMetadata(name string) (Topic, error) {
	partitions, err := c.client.Partitions(name)
	if err != nil {
		return Topic{}, err
	}

	topic := Topic{Name: name, Partitions: len(partitions), ReplicationFactor: -1}

	for _, id := range partitions {
		replicas, err := c.client.Replicas(name, id)
		if err != nil {
			// A partition whose replicas cannot be read is treated as offline
			// rather than silently skipped: pretending it is fine would be worse.
			topic.Offline++
			continue
		}
		isr, err := c.client.InSyncReplicas(name, id)
		if err != nil {
			isr = nil
		}

		topic.TotalReplicas += len(replicas)
		topic.InSync += len(isr)
		if len(isr) < len(replicas) {
			topic.UnderReplicated++
		}
		if topic.ReplicationFactor < 0 || len(replicas) < topic.ReplicationFactor {
			topic.ReplicationFactor = len(replicas)
		}

		if _, err := c.client.Leader(name, id); err != nil {
			topic.Offline++
		}
	}

	if topic.ReplicationFactor < 0 {
		topic.ReplicationFactor = 0
	}
	return topic, nil
}

// DescribePartitions returns the partitions of a topic, optionally with their
// offset range.
//
// withOffsets costs two requests per partition, so callers pass false unless the
// user is actually looking at that topic.
func (c *Client) DescribePartitions(ctx context.Context, topic string, withOffsets bool) ([]Partition, error) {
	return run(ctx, func() ([]Partition, error) {
		maybeDelay("describe partitions")

		ids, err := c.client.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("partitions of %s: %w", topic, err)
		}

		partitions := make([]Partition, len(ids))

		// One goroutine per partition: the offsets are independent requests and
		// doing them in sequence makes a wide topic feel broken.
		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Add(1)
			go func(i int, id int32) {
				defer wg.Done()
				partitions[i] = c.describePartition(topic, id, withOffsets)
			}(i, id)
		}
		wg.Wait()

		sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
		return partitions, nil
	})
}

func (c *Client) describePartition(topic string, id int32, withOffsets bool) Partition {
	p := Partition{ID: id, Leader: -1}

	if leader, err := c.client.Leader(topic, id); err == nil {
		p.Leader = leader.ID()
	}
	if replicas, err := c.client.Replicas(topic, id); err == nil {
		p.Replicas = replicas
	}
	if isr, err := c.client.InSyncReplicas(topic, id); err == nil {
		p.ISR = isr
	}

	if withOffsets {
		if oldest, err := c.client.GetOffset(topic, id, sarama.OffsetOldest); err == nil {
			p.OldestOffset = oldest
		}
		if newest, err := c.client.GetOffset(topic, id, sarama.OffsetNewest); err == nil {
			p.NewestOffset = newest
		}
	}
	return p
}

// TopicSizes returns the on-disk size of each topic, summed over the brokers.
//
// DescribeLogDirs is one request per broker and returns every partition it
// hosts, which makes it far cheaper than asking per topic.
func (c *Client) TopicSizes(ctx context.Context) (map[string]int64, error) {
	return run(ctx, func() (map[string]int64, error) {
		maybeDelay("describe log dirs")

		brokers := c.client.Brokers()
		ids := make([]int32, 0, len(brokers))
		for _, b := range brokers {
			ids = append(ids, b.ID())
		}

		dirs, err := c.admin.DescribeLogDirs(ids)
		if err != nil {
			return nil, fmt.Errorf("describe log dirs: %w", err)
		}

		sizes := make(map[string]int64)
		for _, brokerDirs := range dirs {
			for _, dir := range brokerDirs {
				for _, topic := range dir.Topics {
					for _, partition := range topic.Partitions {
						sizes[topic.Topic] += partition.Size
					}
				}
			}
		}
		return sizes, nil
	})
}

// BrokerSizes returns the on-disk size held by each broker.
func (c *Client) BrokerSizes(ctx context.Context) (map[int32]int64, error) {
	return run(ctx, func() (map[int32]int64, error) {
		maybeDelay("describe log dirs")

		brokers := c.client.Brokers()
		ids := make([]int32, 0, len(brokers))
		for _, b := range brokers {
			ids = append(ids, b.ID())
		}

		dirs, err := c.admin.DescribeLogDirs(ids)
		if err != nil {
			return nil, fmt.Errorf("describe log dirs: %w", err)
		}

		sizes := make(map[int32]int64)
		for brokerID, brokerDirs := range dirs {
			for _, dir := range brokerDirs {
				for _, topic := range dir.Topics {
					for _, partition := range topic.Partitions {
						sizes[brokerID] += partition.Size
					}
				}
			}
		}
		return sizes, nil
	})
}

// isInternalTopic reports whether a topic is one of Kafka's own.
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "_")
}
