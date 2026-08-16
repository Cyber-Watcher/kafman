package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

// BrokerLoad is how much of the cluster one broker carries.
//
// It comes from the cached metadata, so a collector can ask for it every poll
// without adding a round trip.
type BrokerLoad struct {
	ID   int32
	Addr string
	Rack string

	// Leaders is the number of partitions this broker leads, Replicas the number
	// it stores at all. The first is what decides the traffic it takes.
	Leaders  int
	Replicas int

	// OutOfSync is the number of its replicas that are behind.
	OutOfSync int

	// UnderReplicated is the number of partitions this broker leads whose ISR
	// is missing replicas; NotPreferred the number it leads while not being the
	// preferred replica (replicas[0]). Each partition has exactly one leader,
	// so summing these over the brokers gives honest cluster totals.
	UnderReplicated int
	NotPreferred    int

	// Roles and IsController carry the same honest semantics as Broker's: on
	// KRaft the node's own role, never the metadata "controller id".
	Roles        string
	IsController bool
}

// TopicEndOffsets returns the sum of every partition's end offset, per topic.
//
// The difference between two calls is exactly how many records were written in
// between, which is the only throughput figure the Kafka API can give without
// JMX. One ListOffsets covers the whole cluster: it is batched per partition
// leader, so this costs one request per broker.
func (c *Client) TopicEndOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error) {
	return run(ctx, func() (map[string]int64, error) {
		maybeDelay("end offsets")
		return c.topicOffsetSums(true, includeInternal, sarama.OffsetNewest)
	})
}

// TopicStartOffsets returns the sum of every partition's oldest offset, per
// topic — the retention head. The difference between two calls is how many
// records deletion (retention, compaction or a truncate) removed in between;
// end minus start is what is currently retained.
//
// It reuses the metadata TopicEndOffsets refreshed: the collector calls the
// ends first, and a second refresh in the same poll would buy nothing.
func (c *Client) TopicStartOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error) {
	return run(ctx, func() (map[string]int64, error) {
		maybeDelay("start offsets")
		return c.topicOffsetSums(false, includeInternal, sarama.OffsetOldest)
	})
}

// topicOffsetSums is the shared body of the two offset sweeps.
func (c *Client) topicOffsetSums(refresh, includeInternal bool, at int64) (map[string]int64, error) {
	// The partition list has to keep up with topics created while kafman is
	// running. A refresh that fails because some partition has no leader is
	// still worth using: the rest of the cluster is measurable.
	if refresh {
		if err := c.client.RefreshMetadata(); err != nil {
			slog.Warn("metadata refresh failed, measuring with what is cached", "error", err)
		}
	}

	topics, err := c.client.Topics()
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	request := make(map[string]map[int32]int64, len(topics))
	for _, topic := range topics {
		if !includeInternal && isInternalTopic(topic) {
			continue
		}
		partitions, err := c.client.Partitions(topic)
		if err != nil || len(partitions) == 0 {
			continue
		}
		request[topic] = make(map[int32]int64, len(partitions))
		for _, partition := range partitions {
			request[topic][partition] = at
		}
	}
	if len(request) == 0 {
		return map[string]int64{}, nil
	}

	response, err := c.admin.ListOffsets(request, nil)
	if err != nil && len(response) == 0 {
		return nil, fmt.Errorf("list offsets: %w", err)
	}

	// A partition whose leader is down answers with an error; it is skipped
	// rather than counted as zero, which would look like the topic lost
	// records.
	sums := make(map[string]int64, len(response))
	for topic, partitions := range response {
		for _, result := range partitions {
			if result == nil || !isNoError(result.Err) || result.Offset < 0 {
				continue
			}
			sums[topic] += result.Offset
		}
	}
	return sums, nil
}

// BrokerLoads describes what each broker carries, from cached metadata.
func (c *Client) BrokerLoads(ctx context.Context) ([]BrokerLoad, error) {
	return run(ctx, func() ([]BrokerLoad, error) {
		maybeDelay("broker loads")

		controller, err := c.client.Controller()
		controllerID := int32(-1)
		if err == nil && controller != nil {
			controllerID = controller.ID()
		}

		ids := make([]int32, 0, len(c.client.Brokers()))
		for _, broker := range c.client.Brokers() {
			ids = append(ids, broker.ID())
		}
		roles := c.brokerRoles(ids)

		loads := make(map[int32]*BrokerLoad)
		for _, broker := range c.client.Brokers() {
			loads[broker.ID()] = &BrokerLoad{
				ID:           broker.ID(),
				Addr:         broker.Addr(),
				Rack:         broker.Rack(),
				Roles:        roles[broker.ID()],
				IsController: resolveController(roles, broker.ID(), controllerID),
			}
		}

		topics, err := c.client.Topics()
		if err != nil {
			return nil, fmt.Errorf("list topics: %w", err)
		}

		for _, topic := range topics {
			partitions, err := c.client.Partitions(topic)
			if err != nil {
				continue
			}
			for _, partition := range partitions {
				replicas, err := c.client.Replicas(topic, partition)
				if err != nil {
					continue
				}
				inSync, _ := c.client.InSyncReplicas(topic, partition)
				synced := make(map[int32]bool, len(inSync))
				for _, id := range inSync {
					synced[id] = true
				}

				for _, id := range replicas {
					load, ok := loads[id]
					if !ok {
						// A replica on a broker that is currently down still counts
						// as work the cluster expects of it.
						load = &BrokerLoad{ID: id}
						loads[id] = load
					}
					load.Replicas++
					if !synced[id] {
						load.OutOfSync++
					}
				}

				if leader, err := c.client.Leader(topic, partition); err == nil && leader != nil {
					if load, ok := loads[leader.ID()]; ok {
						load.Leaders++
						if len(inSync) < len(replicas) {
							load.UnderReplicated++
						}
						if len(replicas) > 0 && leader.ID() != replicas[0] {
							load.NotPreferred++
						}
					}
				}
			}
		}

		out := make([]BrokerLoad, 0, len(loads))
		for _, load := range loads {
			out = append(out, *load)
		}
		sortBrokerLoads(out)
		return out, nil
	})
}

func sortBrokerLoads(loads []BrokerLoad) {
	for i := 1; i < len(loads); i++ {
		for j := i; j > 0 && loads[j].ID < loads[j-1].ID; j-- {
			loads[j], loads[j-1] = loads[j-1], loads[j]
		}
	}
}
