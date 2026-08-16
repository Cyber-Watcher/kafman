// Package kafka wraps the Kafka client and exposes the operations kafman needs.
//
// It deliberately does not import bubbletea: everything here takes a context and
// returns values or errors, and internal/ui adapts that into tea.Cmd. Keeping
// the domain layer free of the UI framework is what makes a headless mode, and
// plain unit tests, possible.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// Broker describes one node of the cluster.
type Broker struct {
	ID   int32
	Addr string
	Rack string

	// Roles is the node's process.roles ("broker", "controller",
	// "broker,controller"), read from its static configuration. Empty on a
	// ZooKeeper-era broker, or when the configuration could not be read.
	Roles string

	// IsController means the node's own controller role on KRaft; on a
	// ZooKeeper cluster it is the elected controller. The KRaft quorum
	// *leader* is deliberately not claimed anywhere: brokers do not expose it
	// to clients — the metadata "controller id" there is a random live broker.
	IsController bool
}

// ClusterInfo is the cluster-level metadata shown on the Overview tab.
type ClusterInfo struct {
	// ClusterID is empty when the broker does not report one.
	ClusterID    string
	ControllerID int32
	Brokers      []Broker
}

// Facade is the set of operations the UI depends on. It exists so screens can be
// tested against Mock instead of a live cluster; it grows as stages land.
type Facade interface {
	Cluster() *config.Cluster
	Close() error

	// Cluster shape and health, all derived from cached metadata.
	DescribeCluster(ctx context.Context) (ClusterInfo, error)
	ClusterHealth(ctx context.Context) (Health, error)

	// DescribeQuorum reports the KRaft metadata quorum; ErrNoQuorum on a
	// cluster that has none to describe.
	DescribeQuorum(ctx context.Context) (QuorumInfo, error)
	ListTopics(ctx context.Context, includeInternal bool) ([]Topic, error)
	DescribePartitions(ctx context.Context, topic string, withOffsets bool) ([]Partition, error)

	// Throughput sources: end offsets for msg/s, start offsets for the
	// retention head, broker loads for the distribution of partitions.
	TopicEndOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error)
	TopicStartOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error)
	BrokerLoads(ctx context.Context) ([]BrokerLoad, error)

	// Sizes and lag, which cost real round trips.
	TopicSizes(ctx context.Context) (map[string]int64, error)
	BrokerSizes(ctx context.Context) (map[int32]int64, error)

	// BrokerLogDirs returns each broker's log.dirs config, split on commas.
	// Cached for the life of the connection: the value only changes with a
	// restart.
	BrokerLogDirs(ctx context.Context) (map[int32][]string, error)
	ListConsumerGroups(ctx context.Context) ([]ConsumerGroup, error)
	ConsumerGroupsWithLag(ctx context.Context) ([]ConsumerGroup, error)
	DescribeConsumerGroup(ctx context.Context, name string) (ConsumerGroup, []GroupOffset, error)

	// Consumer group administration. A reset is previewed and applied as two
	// calls so the user approves the exact positions that get committed.
	PreviewOffsetReset(ctx context.Context, spec ResetSpec) ([]OffsetChange, error)
	ApplyOffsetReset(ctx context.Context, group string, changes []OffsetChange) error
	DeleteGroupOffsets(ctx context.Context, group, topic string) error
	DeleteConsumerGroup(ctx context.Context, name string) error

	// Reading records. Always by direct partition assignment, never as part of a
	// consumer group, and offsets are never committed.
	ReadRecords(ctx context.Context, opts ReadOptions) (*ReadSession, error)

	// Produce publishes one record.
	Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error)

	// Topic administration.
	CreateTopic(ctx context.Context, spec TopicSpec) error
	DeleteTopic(ctx context.Context, name string) error
	AddPartitions(ctx context.Context, topic string, total int32) error
	TopicConfig(ctx context.Context, topic string, includeDefaults bool) ([]ConfigEntry, error)
	SetTopicConfig(ctx context.Context, topic, key, value string) error
	ResetTopicConfig(ctx context.Context, topic, key string) error
	TruncateTopic(ctx context.Context, topic string) error

	// SCRAM principals and access control.
	ListScramUsers(ctx context.Context) ([]ScramUser, error)
	UpsertScramUser(ctx context.Context, spec ScramUserSpec) error
	DeleteScramUser(ctx context.Context, name string, mechanisms []string) error
	ListACLs(ctx context.Context, filter ACLFilter) ([]ACL, error)
	CreateACL(ctx context.Context, acl ACL) error
	DeleteACL(ctx context.Context, acl ACL) (int, error)
}

// Instantiator creates a Facade for a cluster. Injecting it into the root model
// keeps the UI testable without a broker (pattern borrowed from ktea).
type Instantiator func(*config.Cluster) (Facade, error)

// Client is the live implementation of Facade, backed by sarama.
type Client struct {
	cluster *config.Cluster
	conf    *sarama.Config

	client sarama.Client
	admin  sarama.ClusterAdmin

	// The producer is created lazily: a read-only session should not pay the
	// metadata round trip it costs.
	producerState

	// rolesCache keeps each broker's process.roles for the life of the
	// connection; see roles.go. logDirsCache does the same for log.dirs.
	rolesCache   rolesCache
	logDirsCache logDirsCache

	closeOnce sync.Once
	closeErr  error
}

var _ Facade = (*Client)(nil)

// New connects to a cluster and returns a ready client.
//
// Connecting is synchronous and can take up to the cluster's request-timeout;
// callers on the UI thread must run it off the main goroutine.
func New(c *config.Cluster) (*Client, error) {
	maybeDelay("connect")

	conf, err := BuildSaramaConfig(c)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(c.Brokers, conf)
	if err != nil {
		return nil, connectError(c, err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, connectError(c, err)
	}

	slog.Info("connected", "cluster", c.Name, "brokers", len(client.Brokers()))
	return &Client{cluster: c, conf: conf, client: client, admin: admin}, nil
}

// NewFacade adapts New to the Instantiator signature.
func NewFacade(c *config.Cluster) (Facade, error) { return New(c) }

// Cluster returns the configuration this client was built from.
func (c *Client) Cluster() *config.Cluster { return c.cluster }

// DescribeCluster reports the brokers and which one is the controller.
func (c *Client) DescribeCluster(ctx context.Context) (ClusterInfo, error) {
	return run(ctx, func() (ClusterInfo, error) {
		maybeDelay("describe cluster")

		brokers, controllerID, err := c.admin.DescribeCluster()
		if err != nil {
			return ClusterInfo{}, fmt.Errorf("describe cluster: %w", err)
		}

		ids := make([]int32, 0, len(brokers))
		for _, b := range brokers {
			ids = append(ids, b.ID())
		}
		roles := c.brokerRoles(ids)

		info := ClusterInfo{
			ControllerID: controllerID,
			ClusterID:    c.clusterID(),
			Brokers:      make([]Broker, 0, len(brokers)),
		}
		for _, b := range brokers {
			info.Brokers = append(info.Brokers, Broker{
				ID:           b.ID(),
				Addr:         b.Addr(),
				Rack:         b.Rack(),
				Roles:        roles[b.ID()],
				IsController: resolveController(roles, b.ID(), controllerID),
			})
		}
		sort.Slice(info.Brokers, func(i, j int) bool { return info.Brokers[i].ID < info.Brokers[j].ID })
		return info, nil
	})
}

// clusterID fetches the cluster id, which ClusterAdmin does not expose. It comes
// back in the metadata response from any broker. Best effort: an empty string is
// preferable to failing the whole describe over a cosmetic field.
func (c *Client) clusterID() string {
	// Controller() hands back a broker with an open connection; the entries from
	// Brokers() are only metadata and may never have been dialled.
	broker, err := c.client.Controller()
	if err != nil {
		slog.Debug("cluster id unavailable: no controller", "error", err)
		return ""
	}

	// Metadata v2 is the first version carrying the cluster id. An empty (not
	// nil) topic list asks for no topics at all — nil would mean "every topic",
	// which is expensive on a large cluster.
	resp, err := broker.GetMetadata(&sarama.MetadataRequest{Version: 2, Topics: []string{}})
	if err != nil {
		slog.Debug("cluster id unavailable", "error", err)
		return ""
	}
	if resp.ClusterID == nil {
		return ""
	}
	return *resp.ClusterID
}

// Close releases the connections. Safe to call more than once.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		var errs []error
		if c.producer != nil {
			if err := c.producer.Close(); err != nil && !errors.Is(err, sarama.ErrClosedClient) {
				errs = append(errs, err)
			}
		}
		// Closing the admin also closes the client it was created from.
		if c.admin != nil {
			if err := c.admin.Close(); err != nil && !errors.Is(err, sarama.ErrClosedClient) {
				errs = append(errs, err)
			}
		}
		if c.client != nil && !c.client.Closed() {
			if err := c.client.Close(); err != nil && !errors.Is(err, sarama.ErrClosedClient) {
				errs = append(errs, err)
			}
		}
		c.closeErr = errors.Join(errs...)
		slog.Info("disconnected", "cluster", c.cluster.Name)
	})
	return c.closeErr
}

// run executes a blocking sarama call on a separate goroutine so the caller's
// context can cancel the wait.
//
// sarama's API is synchronous and context-unaware; when the context fires, the
// call itself keeps running to completion in the background and its result is
// discarded. Timeouts still bound it, because every network operation is
// configured with request-timeout.
func run[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)

	go func() {
		value, err := fn()
		done <- result{value, err}
	}()

	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// connectError adds the information a user needs to fix a failed connection:
// which cluster, which brokers, and what the failure actually means.
func connectError(c *config.Cluster, err error) error {
	return fmt.Errorf("connect to cluster %q (%s, %s): %w",
		c.Name, joinBrokers(c.Brokers), c.SecurityProfile(), err)
}

func joinBrokers(brokers []string) string {
	const maxShown = 3
	if len(brokers) <= maxShown {
		return strings.Join(brokers, ", ")
	}
	return fmt.Sprintf("%s +%d more", strings.Join(brokers[:maxShown], ", "), len(brokers)-maxShown)
}

// DefaultConnectTimeout bounds a connectivity check when the caller gives no
// deadline of its own.
const DefaultConnectTimeout = 15 * time.Second
