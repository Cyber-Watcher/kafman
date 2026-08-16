package kafka

import (
	"context"
	"fmt"
	"log/slog"
)

// Health summarises the replication state of the whole cluster.
//
// Every number here is derived from cached metadata, so refreshing the health
// panel costs one metadata request no matter how many topics exist.
type Health struct {
	Topics     int
	Partitions int

	// InSync is the number of partitions whose ISR matches their replica list.
	InSync int

	// UnderReplicated partitions have a leader but are missing in-sync replicas:
	// the cluster still serves them, but has lost redundancy.
	UnderReplicated int

	// Offline partitions have no leader and cannot be read or written.
	Offline int

	// TotalReplicas and InSyncReplicas are replica counts, not partition counts,
	// and give the average replication factor.
	TotalReplicas  int
	InSyncReplicas int

	// Worst names the single most degraded partition, so the user has somewhere
	// to start rather than just a count.
	Worst *PartitionIssue
}

// PartitionIssue identifies one degraded partition.
type PartitionIssue struct {
	Topic     string
	Partition int32
	ISR       int
	Replicas  int
	Offline   bool
}

// String renders the issue for the "worst:" line.
func (p PartitionIssue) String() string {
	if p.Offline {
		return fmt.Sprintf("%s p%d  no leader", p.Topic, p.Partition)
	}
	return fmt.Sprintf("%s p%d  ISR %d/%d", p.Topic, p.Partition, p.ISR, p.Replicas)
}

// OK reports whether every partition is fully replicated and online.
func (h Health) OK() bool { return h.UnderReplicated == 0 && h.Offline == 0 }

// ReplicationFactor is the average number of replicas per partition.
func (h Health) ReplicationFactor() float64 {
	if h.Partitions == 0 {
		return 0
	}
	return float64(h.TotalReplicas) / float64(h.Partitions)
}

// ClusterHealth walks every topic's metadata and aggregates the replication
// state. Internal topics are included: __consumer_offsets going under-replicated
// matters as much as anything else.
func (c *Client) ClusterHealth(ctx context.Context) (Health, error) {
	return run(ctx, func() (Health, error) {
		maybeDelay("cluster health")

		// A partial failure must not abort the health check: RefreshMetadata
		// returns an error when any partition is mid-election or leaderless,
		// which is exactly the situation this panel exists to report. The
		// metadata it did fetch is still usable.
		if err := c.client.RefreshMetadata(); err != nil {
			slog.Warn("metadata refresh incomplete, reporting on what is available",
				"cluster", c.cluster.Name, "error", err)
		}

		names, err := c.client.Topics()
		if err != nil {
			return Health{}, fmt.Errorf("list topics: %w", err)
		}

		health := Health{Topics: len(names)}

		for _, name := range names {
			partitions, err := c.client.Partitions(name)
			if err != nil {
				continue
			}
			for _, id := range partitions {
				health.Partitions++
				health.inspect(c, name, id)
			}
		}
		return health, nil
	})
}

// inspect classifies one partition and keeps track of the worst one seen.
func (h *Health) inspect(c *Client, topic string, id int32) {
	replicas, err := c.client.Replicas(topic, id)
	if err != nil {
		h.Offline++
		h.consider(PartitionIssue{Topic: topic, Partition: id, Offline: true})
		return
	}
	h.TotalReplicas += len(replicas)

	isr, err := c.client.InSyncReplicas(topic, id)
	if err != nil {
		isr = nil
	}
	h.InSyncReplicas += len(isr)

	if _, err := c.client.Leader(topic, id); err != nil {
		h.Offline++
		h.consider(PartitionIssue{Topic: topic, Partition: id, ISR: len(isr),
			Replicas: len(replicas), Offline: true})
		return
	}

	if len(isr) < len(replicas) {
		h.UnderReplicated++
		h.consider(PartitionIssue{Topic: topic, Partition: id,
			ISR: len(isr), Replicas: len(replicas)})
		return
	}
	h.InSync++
}

// consider keeps the most degraded partition: offline beats under-replicated,
// and among those the one with the fewest in-sync replicas.
func (h *Health) consider(issue PartitionIssue) {
	if h.Worst == nil {
		h.Worst = &issue
		return
	}
	switch {
	case issue.Offline && !h.Worst.Offline:
		h.Worst = &issue
	case issue.Offline == h.Worst.Offline && issue.ISR < h.Worst.ISR:
		h.Worst = &issue
	}
}
