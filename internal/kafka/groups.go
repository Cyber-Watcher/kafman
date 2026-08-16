package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/IBM/sarama"
)

// Consumer group states as Kafka reports them.
const (
	GroupStable              = "Stable"
	GroupPreparingRebalance  = "PreparingRebalance"
	GroupCompletingRebalance = "CompletingRebalance"
	GroupEmpty               = "Empty"
	GroupDead                = "Dead"
)

// ConsumerGroup is one group with its aggregate lag.
type ConsumerGroup struct {
	Name    string
	State   string
	Members int
	Topics  []string

	// TotalLag is the sum over every assigned partition. It is only filled by
	// the calls that ask for it, because it costs an offset request per
	// partition.
	TotalLag    int64
	LagKnown    bool
	Coordinator int32

	// CommittedSum is the sum of committed offsets over every partition the
	// group has actually committed on — never-committed partitions are skipped,
	// not counted as zero. Its growth between two readings is the group's
	// consumption rate. CommittedParts is how many partitions contributed;
	// NoCommit how many assigned partitions carry no commit at all (0 for a
	// group with no members: there is no assignment to compare against).
	// Filled only by ConsumerGroupsWithLag.
	CommittedSum   int64
	CommittedParts int
	CommittedKnown bool
	NoCommit       int

	// Protocol is the assignment strategy the group agreed on, empty for a group
	// with no members.
	Protocol string

	// MemberList is filled only by DescribeConsumerGroup: the list view needs
	// the count, not the members.
	MemberList []GroupMember
}

// GroupMember is one consumer in a group, with what it was assigned.
type GroupMember struct {
	MemberID string
	ClientID string
	Host     string

	// Assignment is what the group leader handed this member, sorted by topic
	// and partition.
	Assignment []PartitionRef
}

// PartitionRef names one partition.
type PartitionRef struct {
	Topic     string
	Partition int32
}

// Rebalancing reports whether the group is between assignments.
func (g ConsumerGroup) Rebalancing() bool {
	return g.State == GroupPreparingRebalance || g.State == GroupCompletingRebalance
}

// GroupOffset is one partition's position for a group.
type GroupOffset struct {
	Topic     string
	Partition int32

	// Current is the committed offset, or -1 when the group has never committed
	// for this partition. It is not zero: never-committed and committed-at-zero
	// mean very different things.
	Current int64

	OldestOffset int64
	NewestOffset int64

	// Lag is how far behind the end the group is, and Lead how far past the
	// start. Both are -1 when Current is unknown.
	Lag  int64
	Lead int64

	Host     string
	ClientID string
}

// ListConsumerGroups returns the groups without their lag, which is the cheap
// call suitable for a list.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) {
	return run(ctx, func() ([]ConsumerGroup, error) {
		maybeDelay("list consumer groups")

		names, err := c.admin.ListConsumerGroups()
		if err != nil {
			return nil, fmt.Errorf("list consumer groups: %w", err)
		}
		if len(names) == 0 {
			return nil, nil
		}

		list := make([]string, 0, len(names))
		for name := range names {
			list = append(list, name)
		}

		descriptions, err := c.admin.DescribeConsumerGroups(list)
		if err != nil {
			return nil, fmt.Errorf("describe consumer groups: %w", err)
		}

		groups := make([]ConsumerGroup, 0, len(descriptions))
		for _, d := range descriptions {
			groups = append(groups, ConsumerGroup{
				Name:    d.GroupId,
				State:   d.State,
				Members: len(d.Members),
				Topics:  assignedTopics(d),
			})
		}
		c.repairGroupStates(groups)

		sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
		return groups, nil
	})
}

// repairGroupStates fixes the state of groups the classic DescribeGroups API
// cannot see. A group on the KIP-848 consumer protocol (the default for new
// clients on Kafka 4.x) is described as "Dead" with no members, as if it did
// not exist. Its real state does come back in ListGroups v4+, so for every
// nominally dead group the state from the listing wins. Best effort: on any
// failure the described state stands.
func (c *Client) repairGroupStates(groups []ConsumerGroup) {
	needed := false
	for _, g := range groups {
		if g.State == GroupDead {
			needed = true
			break
		}
	}
	if !needed {
		return
	}

	states := c.listGroupStates()
	for i := range groups {
		if groups[i].State != GroupDead {
			continue
		}
		if state, ok := states[groups[i].Name]; ok && state != "" {
			groups[i].State = state
		}
	}
}

// listGroupStates asks every broker for its groups with their states, which
// sarama's ClusterAdmin drops on the floor. Before ListGroups v4 the response
// carries no state at all, so a protocol level below Kafka 2.6 returns nothing.
func (c *Client) listGroupStates() map[string]string {
	if c.client == nil {
		return nil
	}

	var version int16
	switch {
	case c.conf.Version.IsAtLeast(sarama.V3_8_0_0):
		version = 5
	case c.conf.Version.IsAtLeast(sarama.V2_6_0_0):
		version = 4
	default:
		return nil
	}

	states := make(map[string]string)
	for _, broker := range c.client.Brokers() {
		if err := broker.Open(c.conf); err != nil && !errors.Is(err, sarama.ErrAlreadyConnected) {
			continue
		}
		response, err := broker.ListGroups(&sarama.ListGroupsRequest{Version: version})
		if err != nil {
			slog.Debug("list group states", "broker", broker.ID(), "error", err)
			continue
		}
		for name, data := range response.GroupsData {
			if data.GroupState != "" {
				states[name] = data.GroupState
			}
		}
	}
	return states
}

// ConsumerGroupsWithLag returns the groups including their total lag.
//
// Both halves are batched: the committed offsets of every group come back in one
// request, and the end offsets of every partition they touch in one more. That
// matters because the Monitoring collector calls this on a timer — the earlier
// version asked for one offset per partition, which on a real cluster is
// hundreds of round trips every few seconds.
func (c *Client) ConsumerGroupsWithLag(ctx context.Context) ([]ConsumerGroup, error) {
	groups, err := c.ListConsumerGroups(ctx)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}

	return run(ctx, func() ([]ConsumerGroup, error) {
		maybeDelay("consumer group lag")

		// Ask for every group at once: nil topic filters mean "all assigned".
		request := make(map[string]map[string][]int32, len(groups))
		for _, g := range groups {
			request[g.Name] = nil
		}

		responses, err := c.admin.ListConsumerGroupOffsetsBatch(request)
		if errors.Is(err, sarama.ErrUnsupportedVersion) {
			// The batched OffsetFetch exists since Kafka 3.0, and sarama only
			// sends it when kafka-version says 3.0+. A cluster entry with the
			// version left blank runs at sarama's default protocol level (2.8),
			// so the batch is refused client-side before a single byte is sent —
			// no matter how new the broker is. One request per group works at
			// every protocol level.
			responses, err = c.groupOffsetsOneByOne(groups)
		}
		if err != nil {
			return nil, fmt.Errorf("fetch group offsets: %w", err)
		}

		// Every partition any group committed to, resolved in one request.
		targets := make(map[topicPartition]int64)
		for _, response := range responses {
			for topic, partitions := range response.Blocks {
				for id, block := range partitions {
					if block != nil && block.Offset >= 0 {
						targets[topicPartition{topic, id}] = 0
					}
				}
			}
		}

		ends := map[topicPartition]int64{}
		if len(targets) > 0 {
			ends, err = c.listOffsets(targets, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
		}

		for i := range groups {
			response, ok := responses[groups[i].Name]
			if !ok {
				continue
			}
			// A group whose partitions the cluster could not report on gets no
			// lag at all rather than an understated one: a number that is quietly
			// too small is worse than a dash.
			stats := lagStatsOf(response.Blocks, ends)
			groups[i].TotalLag, groups[i].LagKnown = stats.lag, stats.lagKnown
			groups[i].CommittedSum, groups[i].CommittedParts = stats.committed, stats.parts
			groups[i].CommittedKnown = true

			groups[i].NoCommit = c.noCommitCount(groups[i].Topics, stats.parts)
		}

		sort.Slice(groups, func(i, j int) bool { return groups[i].TotalLag > groups[j].TotalLag })
		return groups, nil
	})
}

// noCommitCount is how many of a group's assigned partitions carry no commit.
//
// The OffsetFetch with a nil filter only returns partitions the group has
// committed on, so "never committed" is derived from the assignment: the
// partition count of the assigned topics (cached metadata) minus the
// partitions with a commit. Blind for a group with no members — no assigned
// topics reads as 0, "no assignment to compare against", not as "everything
// committed".
func (c *Client) noCommitCount(topics []string, committedParts int) int {
	if c.client == nil || len(topics) == 0 {
		return 0
	}
	assigned := 0
	for _, topic := range topics {
		if ids, err := c.client.Partitions(topic); err == nil {
			assigned += len(ids)
		}
	}
	return max(assigned-committedParts, 0)
}

// groupLagStats is everything one pass over a group's offset blocks yields.
type groupLagStats struct {
	lag      int64
	lagKnown bool

	// committed is the sum of the committed offsets and parts how many
	// partitions carry one — the -1 "never committed" sentinels are skipped,
	// because a sentinel summed in would drag a real position down.
	committed int64
	parts     int
}

// lagStatsOf sums the lag and the committed offsets over every committed
// partition of one group, and reports whether the lag sum is complete.
//
// It is incomplete when the cluster did not report an end offset for a partition
// the group has committed to — a topic deleted moments ago, or a partition
// between leaders. Such a group's lag is shown as unknown rather than as a total
// that silently leaves partitions out.
func lagStatsOf(blocks map[string]map[int32]*sarama.OffsetFetchResponseBlock, ends map[topicPartition]int64) groupLagStats {
	stats := groupLagStats{lagKnown: true}

	for topic, partitions := range blocks {
		for id, block := range partitions {
			// -1 means the group has never committed here; counting it as lag
			// would invent a number.
			if block == nil || block.Offset < 0 {
				continue
			}
			stats.committed += block.Offset
			stats.parts++

			end, ok := ends[topicPartition{topic, id}]
			if !ok {
				stats.lagKnown = false
				continue
			}
			if lag := end - block.Offset; lag > 0 {
				stats.lag += lag
			}
		}
	}
	return stats
}

// DescribeConsumerGroup returns the per-partition detail for one group.
func (c *Client) DescribeConsumerGroup(ctx context.Context, name string) (ConsumerGroup, []GroupOffset, error) {
	type result struct {
		group   ConsumerGroup
		offsets []GroupOffset
	}

	r, err := run(ctx, func() (result, error) {
		maybeDelay("describe consumer group")

		descriptions, err := c.admin.DescribeConsumerGroups([]string{name})
		if err != nil {
			return result{}, fmt.Errorf("describe group %s: %w", name, err)
		}
		if len(descriptions) == 0 {
			return result{}, fmt.Errorf("consumer group %s does not exist", name)
		}
		d := descriptions[0]

		group := ConsumerGroup{
			Name:       d.GroupId,
			State:      d.State,
			Members:    len(d.Members),
			Topics:     assignedTopics(d),
			Protocol:   d.Protocol,
			MemberList: groupMembers(d),
		}
		// A KIP-848 group looks dead to the classic describe; the listing
		// knows better.
		repairable := []ConsumerGroup{group}
		c.repairGroupStates(repairable)
		group.State = repairable[0].State
		// The coordinator is one broker of the cluster and costs one lookup; it
		// is the first thing to know when a group misbehaves.
		if broker, err := c.admin.Coordinator(name); err == nil && broker != nil {
			group.Coordinator = broker.ID()
		}

		response, err := c.admin.ListConsumerGroupOffsets(name, nil)
		if err != nil {
			return result{}, fmt.Errorf("fetch offsets of %s: %w", name, err)
		}

		owners := partitionOwners(d)

		// Both boundaries of every partition the group holds, two requests in
		// total rather than two per partition.
		targets := make(map[topicPartition]int64)
		for topic, partitions := range response.Blocks {
			for id, block := range partitions {
				if block != nil {
					targets[topicPartition{topic, id}] = 0
				}
			}
		}
		newests, oldests := map[topicPartition]int64{}, map[topicPartition]int64{}
		if len(targets) > 0 {
			if newests, err = c.listOffsets(targets, sarama.OffsetNewest); err != nil {
				return result{}, err
			}
			if oldests, err = c.listOffsets(targets, sarama.OffsetOldest); err != nil {
				return result{}, err
			}
		}

		var offsets []GroupOffset
		for topic, partitions := range response.Blocks {
			for id, block := range partitions {
				if block == nil {
					continue
				}
				offset := GroupOffset{
					Topic: topic, Partition: id,
					Current: block.Offset, Lag: -1, Lead: -1,
				}
				if owner, ok := owners[topicPartition{topic, id}]; ok {
					offset.Host, offset.ClientID = owner.host, owner.clientID
				}
				if newest, ok := newests[topicPartition{topic, id}]; ok {
					offset.NewestOffset = newest
					if block.Offset >= 0 {
						offset.Lag = max(newest-block.Offset, 0)
					}
				}
				if oldest, ok := oldests[topicPartition{topic, id}]; ok {
					offset.OldestOffset = oldest
					if block.Offset >= 0 {
						offset.Lead = max(block.Offset-oldest, 0)
					}
				}
				if offset.Lag > 0 {
					group.TotalLag += offset.Lag
				}
				offsets = append(offsets, offset)
			}
		}
		group.LagKnown = true

		sort.Slice(offsets, func(i, j int) bool {
			if offsets[i].Topic != offsets[j].Topic {
				return offsets[i].Topic < offsets[j].Topic
			}
			return offsets[i].Partition < offsets[j].Partition
		})
		return result{group, offsets}, nil
	})

	return r.group, r.offsets, err
}

// DeleteConsumerGroup removes a group. It fails on the broker side if the group
// still has members, which is the desired behaviour.
func (c *Client) DeleteConsumerGroup(ctx context.Context, name string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("delete consumer group")
		if err := c.admin.DeleteConsumerGroup(name); err != nil {
			return struct{}{}, fmt.Errorf("delete consumer group %s: %w", name, err)
		}
		return struct{}{}, nil
	})
	return err
}

// groupOffsetsOneByOne fetches every group's committed offsets with a separate
// request each, shaped like the batch response so the caller cannot tell the
// difference. It is the fallback for protocol levels below Kafka 3.0, where the
// multi-group OffsetFetch does not exist.
func (c *Client) groupOffsetsOneByOne(groups []ConsumerGroup) (map[string]*sarama.OffsetFetchResponseGroup, error) {
	out := make(map[string]*sarama.OffsetFetchResponseGroup, len(groups))
	for _, g := range groups {
		response, err := c.admin.ListConsumerGroupOffsets(g.Name, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch offsets of %s: %w", g.Name, err)
		}
		out[g.Name] = &sarama.OffsetFetchResponseGroup{GroupId: g.Name, Blocks: response.Blocks}
	}
	return out, nil
}

// ------------------------------------------------------------------ helpers ---

type topicPartition struct {
	topic     string
	partition int32
}

type owner struct {
	host     string
	clientID string
}

// partitionOwners maps each assigned partition to the member holding it.
func partitionOwners(d *sarama.GroupDescription) map[topicPartition]owner {
	out := make(map[topicPartition]owner)
	for _, member := range d.Members {
		assignment, err := member.GetMemberAssignment()
		if err != nil || assignment == nil {
			continue
		}
		for topic, partitions := range assignment.Topics {
			for _, id := range partitions {
				out[topicPartition{topic, id}] = owner{
					host:     member.ClientHost,
					clientID: member.ClientId,
				}
			}
		}
	}
	return out
}

// groupMembers lists the members of a group with their assignments.
//
// A member whose assignment cannot be decoded is still listed: knowing that a
// consumer is connected matters even when what it holds is unreadable.
func groupMembers(d *sarama.GroupDescription) []GroupMember {
	members := make([]GroupMember, 0, len(d.Members))
	for id, member := range d.Members {
		entry := GroupMember{
			MemberID: id,
			ClientID: member.ClientId,
			Host:     member.ClientHost,
		}

		if assignment, err := member.GetMemberAssignment(); err == nil && assignment != nil {
			for topic, partitions := range assignment.Topics {
				for _, partition := range partitions {
					entry.Assignment = append(entry.Assignment, PartitionRef{topic, partition})
				}
			}
			sort.Slice(entry.Assignment, func(i, j int) bool {
				if entry.Assignment[i].Topic != entry.Assignment[j].Topic {
					return entry.Assignment[i].Topic < entry.Assignment[j].Topic
				}
				return entry.Assignment[i].Partition < entry.Assignment[j].Partition
			})
		}
		members = append(members, entry)
	}

	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })
	return members
}

func assignedTopics(d *sarama.GroupDescription) []string {
	seen := make(map[string]struct{})
	for _, member := range d.Members {
		assignment, err := member.GetMemberAssignment()
		if err != nil || assignment == nil {
			continue
		}
		for topic := range assignment.Topics {
			seen[topic] = struct{}{}
		}
	}

	topics := make([]string, 0, len(seen))
	for topic := range seen {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}
