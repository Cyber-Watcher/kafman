package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// Mock is a Facade for tests: UI screens can be driven without a broker.
//
// Every operation is backed by a function field. Leaving one nil returns the
// zero value, so a test only has to set what it actually exercises. NewMock
// fills them with a small healthy cluster.
type Mock struct {
	ClusterFunc               func() *config.Cluster
	DescribeClusterFunc       func(ctx context.Context) (ClusterInfo, error)
	ClusterHealthFunc         func(ctx context.Context) (Health, error)
	DescribeQuorumFunc        func(ctx context.Context) (QuorumInfo, error)
	ListTopicsFunc            func(ctx context.Context, includeInternal bool) ([]Topic, error)
	DescribePartitionsFunc    func(ctx context.Context, topic string, withOffsets bool) ([]Partition, error)
	TopicSizesFunc            func(ctx context.Context) (map[string]int64, error)
	TopicEndOffsetsFunc       func(ctx context.Context, includeInternal bool) (map[string]int64, error)
	TopicStartOffsetsFunc     func(ctx context.Context, includeInternal bool) (map[string]int64, error)
	BrokerLoadsFunc           func(ctx context.Context) ([]BrokerLoad, error)
	BrokerSizesFunc           func(ctx context.Context) (map[int32]int64, error)
	BrokerLogDirsFunc         func(ctx context.Context) (map[int32][]string, error)
	ListConsumerGroupsFunc    func(ctx context.Context) ([]ConsumerGroup, error)
	ConsumerGroupsWithLagFunc func(ctx context.Context) ([]ConsumerGroup, error)
	DescribeConsumerGroupFunc func(ctx context.Context, name string) (ConsumerGroup, []GroupOffset, error)
	PreviewOffsetResetFunc    func(ctx context.Context, spec ResetSpec) ([]OffsetChange, error)
	ApplyOffsetResetFunc      func(ctx context.Context, group string, changes []OffsetChange) error
	DeleteGroupOffsetsFunc    func(ctx context.Context, group, topic string) error
	DeleteConsumerGroupFunc   func(ctx context.Context, name string) error
	ReadRecordsFunc           func(ctx context.Context, opts ReadOptions) (*ReadSession, error)
	ProduceFunc               func(ctx context.Context, req ProduceRequest) (ProduceResult, error)
	CreateTopicFunc           func(ctx context.Context, spec TopicSpec) error
	DeleteTopicFunc           func(ctx context.Context, name string) error
	AddPartitionsFunc         func(ctx context.Context, topic string, total int32) error
	TopicConfigFunc           func(ctx context.Context, topic string, includeDefaults bool) ([]ConfigEntry, error)
	SetTopicConfigFunc        func(ctx context.Context, topic, key, value string) error
	ResetTopicConfigFunc      func(ctx context.Context, topic string, key string) error
	TruncateTopicFunc         func(ctx context.Context, topic string) error
	ListScramUsersFunc        func(ctx context.Context) ([]ScramUser, error)
	UpsertScramUserFunc       func(ctx context.Context, spec ScramUserSpec) error
	DeleteScramUserFunc       func(ctx context.Context, name string, mechanisms []string) error
	ListACLsFunc              func(ctx context.Context, filter ACLFilter) ([]ACL, error)
	CreateACLFunc             func(ctx context.Context, acl ACL) error
	DeleteACLFunc             func(ctx context.Context, acl ACL) (int, error)
	CloseFunc                 func() error

	// Created and Deleted record what the tab asked for, for assertions.
	Created []TopicSpec
	Deleted []string

	// Applied records the offset resets that were carried out, DeletedGroups the
	// groups removed and DeletedOffsets the group/topic pairs cleared.
	Applied        []AppliedReset
	DeletedGroups  []string
	DeletedOffsets []string

	// Upserted records the SCRAM users written and DeletedUsers those removed;
	// CreatedACLs and DeletedACLs the access rules.
	Upserted     []ScramUserSpec
	DeletedUsers []string
	CreatedACLs  []ACL
	DeletedACLs  []ACL

	// Produced records the requests passed to Produce, for assertions.
	Produced []ProduceRequest

	mu    sync.Mutex
	calls []string
}

var _ Facade = (*Mock)(nil)

// MockTopics is the topic set NewMock reports: healthy, plus one topic that is
// deliberately under-replicated so health rendering can be exercised.
func MockTopics() []Topic {
	return []Topic{
		{Name: "orders.events", Partitions: 12, ReplicationFactor: 3, InSync: 36, TotalReplicas: 36},
		{Name: "payments.tx", Partitions: 6, ReplicationFactor: 3, InSync: 18, TotalReplicas: 18},
		{Name: "audit.log", Partitions: 24, ReplicationFactor: 3, InSync: 71, TotalReplicas: 72,
			UnderReplicated: 1},
		{Name: "user.clicks", Partitions: 12, ReplicationFactor: 3, InSync: 36, TotalReplicas: 36},
	}
}

// MockGroups is the consumer group set NewMock reports, sorted by lag.
func MockGroups() []ConsumerGroup {
	return []ConsumerGroup{
		{Name: "billing-worker", State: GroupStable, Members: 6, TotalLag: 38104, LagKnown: true,
			Topics:       []string{"orders.events"},
			CommittedSum: 4_143_900, CommittedParts: 12, CommittedKnown: true},
		{Name: "search-indexer", State: GroupPreparingRebalance, Members: 4, TotalLag: 3512,
			LagKnown: true, Topics: []string{"user.clicks"},
			CommittedSum: 12_397_265, CommittedParts: 8, CommittedKnown: true},
		{Name: "audit-archiver", State: GroupStable, Members: 2, TotalLag: 266, LagKnown: true,
			Topics:       []string{"audit.log"},
			CommittedSum: 2_004_652, CommittedParts: 3, CommittedKnown: true, NoCommit: 3},
		{Name: "old-consumer", State: GroupEmpty, Members: 0, TotalLag: 0, LagKnown: true,
			CommittedKnown: true},
	}
}

// AppliedReset is one reset the mock carried out.
type AppliedReset struct {
	Group   string
	Changes []OffsetChange
}

// MockGroupOffsets is the per-partition detail NewMock reports for a group.
func MockGroupOffsets() []GroupOffset {
	return []GroupOffset{
		{Topic: "orders.events", Partition: 0, Current: 391755, OldestOffset: 100,
			NewestOffset: 399012, Lag: 7257, Lead: 391655, Host: "10.0.0.2", ClientID: "worker-1"},
		{Topic: "orders.events", Partition: 1, Current: 402881, OldestOffset: 0,
			NewestOffset: 410004, Lag: 7123, Lead: 402881, Host: "10.0.0.2", ClientID: "worker-1"},
		{Topic: "orders.events", Partition: 2, Current: -1, OldestOffset: 0,
			NewestOffset: 395881, Lag: -1, Lead: -1},
	}
}

// MockOffsetChanges is the reset preview NewMock reports: one partition that
// moves back, one that is clamped to the start of the partition.
func MockOffsetChanges() []OffsetChange {
	return []OffsetChange{
		{Topic: "orders.events", Partition: 0, Current: 391755, New: 100,
			Oldest: 100, Newest: 399012, Clamped: true},
		{Topic: "orders.events", Partition: 1, Current: 402881, New: 0,
			Oldest: 0, Newest: 410004},
	}
}

// MockUsers is the SCRAM principal set NewMock reports: one user with both
// mechanisms, so the list has something to render beyond a single row.
func MockUsers() []ScramUser {
	return []ScramUser{
		{Name: "admin", Credentials: []ScramCredential{
			{Mechanism: ScramSHA256, Iterations: 8192},
			{Mechanism: ScramSHA512, Iterations: 8192},
		}},
		{Name: "billing-worker", Credentials: []ScramCredential{
			{Mechanism: ScramSHA512, Iterations: 8192},
		}},
		{Name: "readonly-audit", Credentials: []ScramCredential{
			{Mechanism: ScramSHA256, Iterations: 4096},
		}},
	}
}

// MockACLs is the rule set NewMock reports, including one denial.
func MockACLs() []ACL {
	return []ACL{
		{Principal: "User:readonly-audit", Host: "*", Operation: "Write", Permission: "Deny",
			ResourceType: "Topic", ResourceName: "payments.tx", PatternType: "Literal"},
		{Principal: "User:billing-worker", Host: "*", Operation: "Read", Permission: "Allow",
			ResourceType: "Topic", ResourceName: "orders.events", PatternType: "Literal"},
		{Principal: "User:billing-worker", Host: "*", Operation: "Read", Permission: "Allow",
			ResourceType: "Group", ResourceName: "billing-", PatternType: "Prefixed"},
		{Principal: "User:readonly-audit", Host: "*", Operation: "Describe", Permission: "Allow",
			ResourceType: "Topic", ResourceName: "*", PatternType: "Literal"},
	}
}

// NewMock returns a mock reporting a three-broker cluster with one
// under-replicated partition.
func NewMock(cluster *config.Cluster) *Mock {
	if cluster == nil {
		cluster = &config.Cluster{Name: "mock", Brokers: []string{"localhost:9092"}}
	}

	return &Mock{
		ClusterFunc: func() *config.Cluster { return cluster },

		// The quorum matches the broker fixtures: node 2 is the combined
		// controller and leads, the plain brokers follow as observers.
		DescribeQuorumFunc: func(context.Context) (QuorumInfo, error) {
			return QuorumInfo{
				LeaderID: 2, LeaderEpoch: 5, HighWatermark: 28_400,
				Voters: []QuorumReplica{{ID: 2, LogEndOffset: 28_412, Lag: 0}},
				Observers: []QuorumReplica{
					{ID: 1, LogEndOffset: 28_412, Lag: 0},
					{ID: 3, LogEndOffset: 28_398, Lag: 14},
				},
			}, nil
		},

		DescribeClusterFunc: func(context.Context) (ClusterInfo, error) {
			return ClusterInfo{
				ClusterID:    "mock-cluster",
				ControllerID: 2,
				Brokers: []Broker{
					{ID: 1, Addr: "kafka-1:9092", Rack: "rack-a", Roles: "broker"},
					{ID: 2, Addr: "kafka-2:9092", Rack: "rack-b", Roles: "broker,controller", IsController: true},
					{ID: 3, Addr: "kafka-3:9092", Rack: "rack-c", Roles: "broker"},
				},
			}, nil
		},

		ClusterHealthFunc: func(context.Context) (Health, error) {
			return Health{
				Topics: 5, Partitions: 54, InSync: 53, UnderReplicated: 1,
				TotalReplicas: 162, InSyncReplicas: 161,
				Worst: &PartitionIssue{Topic: "audit.log", Partition: 7, ISR: 2, Replicas: 3},
			}, nil
		},

		ListTopicsFunc: func(_ context.Context, includeInternal bool) ([]Topic, error) {
			topics := MockTopics()
			if includeInternal {
				topics = append(topics, Topic{
					Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 3,
					InSync: 150, TotalReplicas: 150, Internal: true,
				})
			}
			return topics, nil
		},

		DescribePartitionsFunc: func(_ context.Context, _ string, withOffsets bool) ([]Partition, error) {
			partitions := []Partition{
				{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
				{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3}},
			}
			if withOffsets {
				partitions[0].OldestOffset, partitions[0].NewestOffset = 100, 4200
				partitions[1].OldestOffset, partitions[1].NewestOffset = 0, 980
			}
			return partitions, nil
		},

		TopicSizesFunc: func(context.Context) (map[string]int64, error) {
			return map[string]int64{
				"orders.events": 40_912_000_000,
				"payments.tx":   13_312_000_000,
				"audit.log":     99_614_720_000,
				"user.clicks":   9_555_000_000,
			}, nil
		},

		TopicEndOffsetsFunc: func(_ context.Context, includeInternal bool) (map[string]int64, error) {
			ends := map[string]int64{
				"orders.events": 4_182_004,
				"payments.tx":   988_120,
				"audit.log":     2_004_918,
				"user.clicks":   12_400_777,
			}
			if includeInternal {
				ends["__consumer_offsets"] = 55_120
			}
			return ends, nil
		},

		TopicStartOffsetsFunc: func(_ context.Context, includeInternal bool) (map[string]int64, error) {
			starts := map[string]int64{
				"orders.events": 3_900_000,
				"payments.tx":   0,
				"audit.log":     1_200_000,
				"user.clicks":   11_000_000,
			}
			if includeInternal {
				starts["__consumer_offsets"] = 0
			}
			return starts, nil
		},

		BrokerLoadsFunc: func(context.Context) ([]BrokerLoad, error) {
			return []BrokerLoad{
				{ID: 1, Addr: "kafka-1:9092", Rack: "rack-a", Leaders: 18, Replicas: 54,
					Roles: "broker"},
				{ID: 2, Addr: "kafka-2:9092", Rack: "rack-b", Leaders: 18, Replicas: 54,
					Roles: "broker,controller", IsController: true, NotPreferred: 2},
				{ID: 3, Addr: "kafka-3:9092", Rack: "rack-c", Leaders: 18, Replicas: 54,
					Roles: "broker", OutOfSync: 1, UnderReplicated: 1},
			}, nil
		},

		BrokerSizesFunc: func(context.Context) (map[int32]int64, error) {
			return map[int32]int64{1: 442_000_000_000, 2: 449_000_000_000, 3: 439_000_000_000}, nil
		},

		BrokerLogDirsFunc: func(context.Context) (map[int32][]string, error) {
			return map[int32][]string{
				1: {"/var/lib/kafka/data"},
				2: {"/var/lib/kafka/data"},
				3: {"/var/lib/kafka/data"},
			}, nil
		},

		ListConsumerGroupsFunc: func(context.Context) ([]ConsumerGroup, error) {
			return MockGroups(), nil
		},

		ConsumerGroupsWithLagFunc: func(context.Context) ([]ConsumerGroup, error) {
			return MockGroups(), nil
		},

		DescribeConsumerGroupFunc: func(_ context.Context, name string) (ConsumerGroup, []GroupOffset, error) {
			for _, g := range MockGroups() {
				if g.Name != name {
					continue
				}
				g.Coordinator = 2
				g.Protocol = "range"
				for i := 0; i < g.Members; i++ {
					g.MemberList = append(g.MemberList, GroupMember{
						MemberID: fmt.Sprintf("%s-%d-uuid", name, i),
						ClientID: fmt.Sprintf("worker-%d", i+1),
						Host:     "10.0.0.2",
						Assignment: []PartitionRef{
							{Topic: "orders.events", Partition: int32(i)},
						},
					})
				}
				return g, MockGroupOffsets(), nil
			}
			return ConsumerGroup{}, nil, fmt.Errorf("consumer group %s does not exist", name)
		},

		ListScramUsersFunc: func(context.Context) ([]ScramUser, error) { return MockUsers(), nil },

		ListACLsFunc: func(_ context.Context, filter ACLFilter) ([]ACL, error) {
			acls := MockACLs()
			if filter.Principal == "" && filter.ResourceName == "" && filter.ResourceType == "" {
				return acls, nil
			}
			var kept []ACL
			for _, acl := range acls {
				if filter.Principal != "" && acl.Principal != filter.Principal {
					continue
				}
				if filter.ResourceName != "" && acl.ResourceName != filter.ResourceName {
					continue
				}
				if filter.ResourceType != "" && acl.ResourceType != filter.ResourceType {
					continue
				}
				kept = append(kept, acl)
			}
			return kept, nil
		},

		PreviewOffsetResetFunc: func(_ context.Context, spec ResetSpec) ([]OffsetChange, error) {
			for _, g := range MockGroups() {
				if g.Name == spec.Group {
					return MockOffsetChanges(), nil
				}
			}
			return nil, fmt.Errorf("consumer group %s does not exist", spec.Group)
		},
	}
}

func (m *Mock) Cluster() *config.Cluster {
	m.record("Cluster")
	if m.ClusterFunc == nil {
		return nil
	}
	return m.ClusterFunc()
}

func (m *Mock) DescribeCluster(ctx context.Context) (ClusterInfo, error) {
	m.record("DescribeCluster")
	if m.DescribeClusterFunc == nil {
		return ClusterInfo{}, nil
	}
	return m.DescribeClusterFunc(ctx)
}

func (m *Mock) DescribeQuorum(ctx context.Context) (QuorumInfo, error) {
	m.record("DescribeQuorum")
	if m.DescribeQuorumFunc == nil {
		return QuorumInfo{}, ErrNoQuorum
	}
	return m.DescribeQuorumFunc(ctx)
}

func (m *Mock) ClusterHealth(ctx context.Context) (Health, error) {
	m.record("ClusterHealth")
	if m.ClusterHealthFunc == nil {
		return Health{}, nil
	}
	return m.ClusterHealthFunc(ctx)
}

func (m *Mock) ListTopics(ctx context.Context, includeInternal bool) ([]Topic, error) {
	m.record("ListTopics")
	if m.ListTopicsFunc == nil {
		return nil, nil
	}
	return m.ListTopicsFunc(ctx, includeInternal)
}

func (m *Mock) DescribePartitions(ctx context.Context, topic string, withOffsets bool) ([]Partition, error) {
	m.record("DescribePartitions")
	if m.DescribePartitionsFunc == nil {
		return nil, nil
	}
	return m.DescribePartitionsFunc(ctx, topic, withOffsets)
}

func (m *Mock) TopicSizes(ctx context.Context) (map[string]int64, error) {
	m.record("TopicSizes")
	if m.TopicSizesFunc == nil {
		return nil, nil
	}
	return m.TopicSizesFunc(ctx)
}

func (m *Mock) TopicEndOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error) {
	m.record("TopicEndOffsets")
	if m.TopicEndOffsetsFunc == nil {
		return nil, nil
	}
	return m.TopicEndOffsetsFunc(ctx, includeInternal)
}

func (m *Mock) TopicStartOffsets(ctx context.Context, includeInternal bool) (map[string]int64, error) {
	m.record("TopicStartOffsets")
	if m.TopicStartOffsetsFunc == nil {
		return nil, nil
	}
	return m.TopicStartOffsetsFunc(ctx, includeInternal)
}

func (m *Mock) BrokerLoads(ctx context.Context) ([]BrokerLoad, error) {
	m.record("BrokerLoads")
	if m.BrokerLoadsFunc == nil {
		return nil, nil
	}
	return m.BrokerLoadsFunc(ctx)
}

func (m *Mock) BrokerSizes(ctx context.Context) (map[int32]int64, error) {
	m.record("BrokerSizes")
	if m.BrokerSizesFunc == nil {
		return nil, nil
	}
	return m.BrokerSizesFunc(ctx)
}

func (m *Mock) BrokerLogDirs(ctx context.Context) (map[int32][]string, error) {
	if m.BrokerLogDirsFunc == nil {
		return nil, nil
	}
	return m.BrokerLogDirsFunc(ctx)
}

func (m *Mock) ListConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) {
	m.record("ListConsumerGroups")
	if m.ListConsumerGroupsFunc == nil {
		return nil, nil
	}
	return m.ListConsumerGroupsFunc(ctx)
}

func (m *Mock) ConsumerGroupsWithLag(ctx context.Context) ([]ConsumerGroup, error) {
	m.record("ConsumerGroupsWithLag")
	if m.ConsumerGroupsWithLagFunc == nil {
		return nil, nil
	}
	return m.ConsumerGroupsWithLagFunc(ctx)
}

func (m *Mock) DescribeConsumerGroup(ctx context.Context, name string) (ConsumerGroup, []GroupOffset, error) {
	m.record("DescribeConsumerGroup")
	if m.DescribeConsumerGroupFunc == nil {
		return ConsumerGroup{}, nil, nil
	}
	return m.DescribeConsumerGroupFunc(ctx, name)
}

func (m *Mock) PreviewOffsetReset(ctx context.Context, spec ResetSpec) ([]OffsetChange, error) {
	m.record("PreviewOffsetReset")
	if m.PreviewOffsetResetFunc == nil {
		return nil, nil
	}
	return m.PreviewOffsetResetFunc(ctx, spec)
}

func (m *Mock) ApplyOffsetReset(ctx context.Context, group string, changes []OffsetChange) error {
	m.record("ApplyOffsetReset")

	m.mu.Lock()
	m.Applied = append(m.Applied, AppliedReset{Group: group, Changes: changes})
	m.mu.Unlock()

	if m.ApplyOffsetResetFunc == nil {
		return nil
	}
	return m.ApplyOffsetResetFunc(ctx, group, changes)
}

func (m *Mock) DeleteGroupOffsets(ctx context.Context, group, topic string) error {
	m.record("DeleteGroupOffsets")

	m.mu.Lock()
	m.DeletedOffsets = append(m.DeletedOffsets, group+"/"+topic)
	m.mu.Unlock()

	if m.DeleteGroupOffsetsFunc == nil {
		return nil
	}
	return m.DeleteGroupOffsetsFunc(ctx, group, topic)
}

func (m *Mock) DeleteConsumerGroup(ctx context.Context, name string) error {
	m.record("DeleteConsumerGroup")

	m.mu.Lock()
	m.DeletedGroups = append(m.DeletedGroups, name)
	m.mu.Unlock()

	if m.DeleteConsumerGroupFunc == nil {
		return nil
	}
	return m.DeleteConsumerGroupFunc(ctx, name)
}

func (m *Mock) ReadRecords(ctx context.Context, opts ReadOptions) (*ReadSession, error) {
	m.record("ReadRecords")
	if m.ReadRecordsFunc == nil {
		return MockSession(ctx, MockRecords(opts.Topics)), nil
	}
	return m.ReadRecordsFunc(ctx, opts)
}

func (m *Mock) Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	m.record("Produce")

	m.mu.Lock()
	m.Produced = append(m.Produced, req)
	m.mu.Unlock()

	if m.ProduceFunc == nil {
		return ProduceResult{Topic: req.Topic, Partition: req.Partition, Offset: 4242}, nil
	}
	return m.ProduceFunc(ctx, req)
}

func (m *Mock) CreateTopic(ctx context.Context, spec TopicSpec) error {
	m.record("CreateTopic")

	m.mu.Lock()
	m.Created = append(m.Created, spec)
	m.mu.Unlock()

	if m.CreateTopicFunc == nil {
		return nil
	}
	return m.CreateTopicFunc(ctx, spec)
}

func (m *Mock) DeleteTopic(ctx context.Context, name string) error {
	m.record("DeleteTopic")

	m.mu.Lock()
	m.Deleted = append(m.Deleted, name)
	m.mu.Unlock()

	if m.DeleteTopicFunc == nil {
		return nil
	}
	return m.DeleteTopicFunc(ctx, name)
}

func (m *Mock) AddPartitions(ctx context.Context, topic string, total int32) error {
	m.record("AddPartitions")
	if m.AddPartitionsFunc == nil {
		return nil
	}
	return m.AddPartitionsFunc(ctx, topic, total)
}

func (m *Mock) TopicConfig(ctx context.Context, topic string, includeDefaults bool) ([]ConfigEntry, error) {
	m.record("TopicConfig")
	if m.TopicConfigFunc == nil {
		return MockTopicConfig(includeDefaults), nil
	}
	return m.TopicConfigFunc(ctx, topic, includeDefaults)
}

func (m *Mock) SetTopicConfig(ctx context.Context, topic, key, value string) error {
	m.record("SetTopicConfig")
	if m.SetTopicConfigFunc == nil {
		return nil
	}
	return m.SetTopicConfigFunc(ctx, topic, key, value)
}

func (m *Mock) ResetTopicConfig(ctx context.Context, topic, key string) error {
	m.record("ResetTopicConfig")
	if m.ResetTopicConfigFunc == nil {
		return nil
	}
	return m.ResetTopicConfigFunc(ctx, topic, key)
}

func (m *Mock) TruncateTopic(ctx context.Context, topic string) error {
	m.record("TruncateTopic")
	if m.TruncateTopicFunc == nil {
		return nil
	}
	return m.TruncateTopicFunc(ctx, topic)
}

func (m *Mock) ListScramUsers(ctx context.Context) ([]ScramUser, error) {
	m.record("ListScramUsers")
	if m.ListScramUsersFunc == nil {
		return nil, nil
	}
	return m.ListScramUsersFunc(ctx)
}

func (m *Mock) UpsertScramUser(ctx context.Context, spec ScramUserSpec) error {
	m.record("UpsertScramUser")

	m.mu.Lock()
	m.Upserted = append(m.Upserted, spec)
	m.mu.Unlock()

	if m.UpsertScramUserFunc == nil {
		return nil
	}
	return m.UpsertScramUserFunc(ctx, spec)
}

func (m *Mock) DeleteScramUser(ctx context.Context, name string, mechanisms []string) error {
	m.record("DeleteScramUser")

	m.mu.Lock()
	m.DeletedUsers = append(m.DeletedUsers, name)
	m.mu.Unlock()

	if m.DeleteScramUserFunc == nil {
		return nil
	}
	return m.DeleteScramUserFunc(ctx, name, mechanisms)
}

func (m *Mock) ListACLs(ctx context.Context, filter ACLFilter) ([]ACL, error) {
	m.record("ListACLs")
	if m.ListACLsFunc == nil {
		return nil, nil
	}
	return m.ListACLsFunc(ctx, filter)
}

func (m *Mock) CreateACL(ctx context.Context, acl ACL) error {
	m.record("CreateACL")

	m.mu.Lock()
	m.CreatedACLs = append(m.CreatedACLs, acl)
	m.mu.Unlock()

	if m.CreateACLFunc == nil {
		return nil
	}
	return m.CreateACLFunc(ctx, acl)
}

func (m *Mock) DeleteACL(ctx context.Context, acl ACL) (int, error) {
	m.record("DeleteACL")

	m.mu.Lock()
	m.DeletedACLs = append(m.DeletedACLs, acl)
	m.mu.Unlock()

	if m.DeleteACLFunc == nil {
		return 1, nil
	}
	return m.DeleteACLFunc(ctx, acl)
}

func (m *Mock) Close() error {
	m.record("Close")
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

// Calls returns the names of the methods invoked so far, in order.
func (m *Mock) Calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

// Called reports whether the named method was invoked.
func (m *Mock) Called(name string) bool {
	for _, call := range m.Calls() {
		if call == name {
			return true
		}
	}
	return false
}

func (m *Mock) record(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, name)
}

// MockTopicConfig is the configuration NewMock reports for any topic.
func MockTopicConfig(includeDefaults bool) []ConfigEntry {
	entries := []ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Source: "topic"},
		{Name: "retention.ms", Value: "604800000", Source: "topic"},
		{Name: "min.insync.replicas", Value: "2", Source: "topic"},
	}
	if includeDefaults {
		entries = append(entries,
			ConfigEntry{Name: "compression.type", Value: "producer", Default: true, Source: "default"},
			ConfigEntry{Name: "segment.bytes", Value: "1073741824", Default: true, Source: "default"},
			ConfigEntry{Name: "message.format.version", Value: "3.0-IV1",
				Default: true, ReadOnly: true, Source: "static"},
		)
	}
	return entries
}

// MockRecords builds a handful of records covering the formats the record view
// has to render: JSON, plain text, an empty value and a Confluent payload.
func MockRecords(topics []string) []Record {
	if len(topics) == 0 {
		topics = []string{"orders.events"}
	}
	topic := topics[0]
	base := time.Date(2026, 8, 4, 12, 6, 1, 884_000_000, time.UTC)

	return []Record{
		{
			Topic: topic, Partition: 3, Offset: 418204,
			Key:   []byte("ord-99f"),
			Value: []byte(`{"id":"ord-99f","status":"FAILED","amount":1499.00}`),
			Headers: []Header{
				{Key: "trace-id", Value: "4b1ec90"},
				{Key: "source", Value: "checkout-api"},
			},
			Timestamp: base, Size: 58,
		},
		{
			Topic: topic, Partition: 0, Offset: 391755,
			Key: []byte("ord-a10"), Value: []byte("plain text payload"),
			Timestamp: base.Add(20 * time.Millisecond), Size: 25,
		},
		{
			Topic: topic, Partition: 1, Offset: 402881,
			Key: []byte("ord-b44"), Value: nil,
			Timestamp: base.Add(50 * time.Millisecond), Size: 7,
		},
		{
			Topic: topic, Partition: 2, Offset: 88122,
			Key:       []byte("pay-3c1"),
			Value:     []byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0x06, 0x66, 0x6f, 0x6f},
			Timestamp: base.Add(90 * time.Millisecond), Size: 16,
		},
	}
}

// MockSession delivers the given records as one batch and closes, which is what
// a bounded read looks like from the UI's side.
func MockSession(ctx context.Context, records []Record) *ReadSession {
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan Batch, 1)

	go func() {
		defer close(out)
		if len(records) == 0 {
			return
		}
		select {
		case out <- Batch{Records: records, Read: int64(len(records)), Matched: int64(len(records))}:
		case <-ctx.Done():
		}
	}()

	return &ReadSession{Records: out, Err: func() error { return nil }, cancel: cancel}
}

// MockInstantiator returns an Instantiator handing out the given mock, for
// injecting into the root model.
func MockInstantiator(m *Mock) Instantiator {
	return func(*config.Cluster) (Facade, error) { return m, nil }
}
