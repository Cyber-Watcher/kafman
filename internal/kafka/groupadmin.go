package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// ResetKind is how the new position of every partition is worked out.
type ResetKind int

// The reset modes, matching what kafka-consumer-groups offers.
const (
	ResetToEarliest ResetKind = iota
	ResetToLatest
	ResetToOffset
	ResetToTime
	ResetShiftBy
)

func (k ResetKind) String() string {
	switch k {
	case ResetToEarliest:
		return "to-earliest"
	case ResetToLatest:
		return "to-latest"
	case ResetToOffset:
		return "to-offset"
	case ResetToTime:
		return "to-datetime"
	case ResetShiftBy:
		return "shift-by"
	default:
		return "unknown"
	}
}

// ResetSpec describes a reset before it is turned into per-partition changes.
type ResetSpec struct {
	Group string

	// Topics limits the reset. Empty means every topic the group has committed
	// offsets for.
	Topics []string

	Kind ResetKind

	// Offset is used by ResetToOffset, At by ResetToTime and Shift by
	// ResetShiftBy. Shift may be negative.
	Offset int64
	At     time.Time
	Shift  int64
}

// OffsetChange is one partition's move: what the preview shows and what the
// apply step commits.
//
// Preview and apply are separate calls over the same values on purpose. What
// the user approved is exactly what is written, without a second look at the
// partition boundaries that could have moved in between and quietly turned an
// approved reset into a different one.
type OffsetChange struct {
	Topic     string
	Partition int32

	// Current is the committed offset, or -1 when the group has never committed
	// for this partition.
	Current int64

	// New is where the partition ends up.
	New int64

	Oldest int64
	Newest int64

	// Clamped reports that the requested position fell outside the partition and
	// was pulled back to its boundary.
	Clamped bool

	// Note explains a partition the reset could not place as asked.
	Note string
}

// Moves reports whether the partition actually changes position.
func (c OffsetChange) Moves() bool { return c.Current != c.New }

// Delta is how far the partition moves. It is meaningless, and reported as 0,
// when the group has never committed here.
func (c OffsetChange) Delta() int64 {
	if c.Current < 0 {
		return 0
	}
	return c.New - c.Current
}

// LagAfter is how far behind the end the group will be once applied.
func (c OffsetChange) LagAfter() int64 { return max(c.Newest-c.New, 0) }

// PreviewOffsetReset works out where every partition would land, without
// changing anything.
func (c *Client) PreviewOffsetReset(ctx context.Context, spec ResetSpec) ([]OffsetChange, error) {
	return run(ctx, func() ([]OffsetChange, error) {
		maybeDelay("preview offset reset")

		targets, err := c.resetTargets(spec.Group, spec.Topics)
		if err != nil {
			return nil, err
		}
		if len(targets) == 0 {
			return nil, fmt.Errorf("group %s has no committed offsets%s",
				spec.Group, topicSuffix(spec.Topics))
		}

		oldest, err := c.listOffsets(targets, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
		newest, err := c.listOffsets(targets, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}

		// The timestamp lookup is a third round of the same batched call, asked
		// for only when the reset actually needs it.
		var atTime map[topicPartition]int64
		if spec.Kind == ResetToTime {
			atTime, err = c.listOffsets(targets, spec.At.UnixMilli())
			if err != nil {
				return nil, err
			}
		}

		// A reset is a deliberate, one-shot action: moving a partition whose
		// bounds could not be read would place it wherever the arithmetic of
		// zeroes happens to land.
		if missing := missingBounds(targets, oldest, newest); len(missing) > 0 {
			return nil, fmt.Errorf(
				"cannot read the bounds of %s — the topic may have been deleted, or a partition may have no leader",
				strings.Join(missing, ", "))
		}

		changes := make([]OffsetChange, 0, len(targets))
		for tp, current := range targets {
			change := OffsetChange{
				Topic:     tp.topic,
				Partition: tp.partition,
				Current:   current,
				Oldest:    oldest[tp],
				Newest:    newest[tp],
			}
			placeChange(&change, spec, atTime[tp], hasKey(atTime, tp))
			changes = append(changes, change)
		}

		sort.Slice(changes, func(i, j int) bool {
			if changes[i].Topic != changes[j].Topic {
				return changes[i].Topic < changes[j].Topic
			}
			return changes[i].Partition < changes[j].Partition
		})
		return changes, nil
	})
}

// placeChange computes the new position of one partition and clamps it into the
// partition, which is what Kafka would do anyway — but silently, and only once
// the group starts reading.
func placeChange(change *OffsetChange, spec ResetSpec, at int64, atKnown bool) {
	switch spec.Kind {
	case ResetToEarliest:
		change.New = change.Oldest

	case ResetToLatest:
		change.New = change.Newest

	case ResetToOffset:
		change.New = spec.Offset

	case ResetShiftBy:
		base := change.Current
		if base < 0 {
			// Nothing was ever committed here, so there is no position to shift
			// from; the start of the partition is the only honest base.
			base = change.Oldest
			change.Note = "never committed, shifted from the start"
		}
		change.New = base + spec.Shift

	case ResetToTime:
		// Kafka answers -1 when no record is at or after the timestamp, which
		// means the whole partition is older than it.
		if !atKnown || at < 0 {
			change.New = change.Newest
			change.Note = "no record at or after that time"
			return
		}
		change.New = at
	}

	switch {
	case change.New < change.Oldest:
		change.New, change.Clamped = change.Oldest, true
	case change.New > change.Newest:
		change.New, change.Clamped = change.Newest, true
	}
}

// ApplyOffsetReset commits the previewed positions.
//
// Kafka refuses a commit made from outside the group while the group has
// members, and that refusal is correct: moving the position under a running
// consumer would be worse than the error. The message says so instead of
// repeating the protocol's wording.
func (c *Client) ApplyOffsetReset(ctx context.Context, group string, changes []OffsetChange) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("apply offset reset")

		if len(changes) == 0 {
			return struct{}{}, errors.New("no offsets to reset")
		}

		offsets := make(map[string]map[int32]sarama.OffsetAndMetadata)
		for _, change := range changes {
			if offsets[change.Topic] == nil {
				offsets[change.Topic] = make(map[int32]sarama.OffsetAndMetadata)
			}
			offsets[change.Topic][change.Partition] = sarama.OffsetAndMetadata{
				Offset:   change.New,
				Metadata: "reset by kafman",
				// -1 omits the leader epoch: the offsets come from a preview, not
				// from records this client consumed, so there is no epoch to fence
				// against.
				LeaderEpoch: -1,
			}
		}

		response, err := c.admin.AlterConsumerGroupOffsets(group, offsets, nil)
		if err != nil {
			return struct{}{}, groupError(group, "reset offsets", err)
		}
		return struct{}{}, commitErrors(group, response)
	})
	return err
}

// DeleteGroupOffsets removes a group's committed offsets for one topic, which
// makes the group start from its auto.offset.reset position again.
func (c *Client) DeleteGroupOffsets(ctx context.Context, group, topic string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("delete group offsets")

		targets, err := c.resetTargets(group, []string{topic})
		if err != nil {
			return struct{}{}, err
		}

		partitions := make([]int32, 0, len(targets))
		for tp := range targets {
			if tp.topic == topic {
				partitions = append(partitions, tp.partition)
			}
		}
		if len(partitions) == 0 {
			return struct{}{}, fmt.Errorf("group %s has no committed offsets for %s", group, topic)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

		for _, partition := range partitions {
			if err := c.admin.DeleteConsumerGroupOffset(group, topic, partition); err != nil {
				return struct{}{}, groupError(group,
					fmt.Sprintf("delete offsets of %s p%d", topic, partition), err)
			}
		}
		return struct{}{}, nil
	})
	return err
}

// ------------------------------------------------------------------ helpers ---

// resetTargets resolves which partitions a reset covers and what the group has
// committed for each.
//
// A topic named in the spec that the group has never read is still included,
// with -1 as the current offset: seeding a group before it starts is a
// legitimate reason to reset.
func (c *Client) resetTargets(group string, topics []string) (map[topicPartition]int64, error) {
	wanted := make(map[string]bool, len(topics))
	for _, topic := range topics {
		wanted[topic] = true
	}

	response, err := c.admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return nil, groupError(group, "fetch offsets", err)
	}

	targets := make(map[topicPartition]int64)
	seen := make(map[string]bool)
	for topic, partitions := range response.Blocks {
		if len(wanted) > 0 && !wanted[topic] {
			continue
		}
		for id, block := range partitions {
			if block == nil {
				continue
			}
			targets[topicPartition{topic, id}] = block.Offset
			seen[topic] = true
		}
	}

	for topic := range wanted {
		if seen[topic] {
			continue
		}
		ids, err := c.client.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("partitions of %s: %w", topic, err)
		}
		for _, id := range ids {
			targets[topicPartition{topic, id}] = -1
		}
	}
	return targets, nil
}

// listOffsets asks for one position of every target partition. ListOffsets
// batches by partition leader, so this costs one request per broker rather than
// one per partition.
//
// Partitions that cannot be resolved are simply absent from the result: a topic
// deleted a moment ago, or a partition whose leader is being elected, is a
// normal thing to meet on a live cluster. Only a transport failure — nothing
// came back at all — is an error, because then there is nothing to work with.
// Callers decide what a partial answer means for them; for consumer lag it means
// "unknown", for an offset reset it means "refuse".
func (c *Client) listOffsets(targets map[topicPartition]int64, at int64) (map[topicPartition]int64, error) {
	request := make(map[string]map[int32]int64)
	for tp := range targets {
		if request[tp.topic] == nil {
			request[tp.topic] = make(map[int32]int64)
		}
		request[tp.topic][tp.partition] = at
	}

	response, err := c.admin.ListOffsets(request, nil)
	if err != nil && len(response) == 0 {
		return nil, fmt.Errorf("list offsets: %w", err)
	}

	offsets := make(map[topicPartition]int64, len(targets))
	for topic, partitions := range response {
		for id, result := range partitions {
			if result == nil || !isNoError(result.Err) {
				continue
			}
			offsets[topicPartition{topic, id}] = result.Offset
		}
	}
	if missing := len(targets) - len(offsets); missing > 0 {
		slog.Debug("some partitions did not report an offset",
			"missing", missing, "of", len(targets), "error", err)
	}
	return offsets, nil
}

// missingBounds names the target partitions the cluster did not report a
// position for, so a caller that needs all of them can say which are missing
// rather than "something went wrong".
func missingBounds(targets map[topicPartition]int64, resolved ...map[topicPartition]int64) []string {
	var missing []string
	for tp := range targets {
		for _, set := range resolved {
			if _, ok := set[tp]; !ok {
				missing = append(missing, fmt.Sprintf("%s p%d", tp.topic, tp.partition))
				break
			}
		}
	}
	sort.Strings(missing)
	return missing
}

// commitErrors turns the per-partition part of a commit response into one error
// naming the partitions that failed.
func commitErrors(group string, response *sarama.OffsetCommitResponse) error {
	if response == nil {
		return nil
	}

	var failed []string
	var first error
	for topic, partitions := range response.Errors {
		for id, kerr := range partitions {
			if isNoError(kerr) {
				continue
			}
			failed = append(failed, fmt.Sprintf("%s p%d", topic, id))
			if first == nil {
				first = kerr
			}
		}
	}
	if first == nil {
		return nil
	}
	sort.Strings(failed)

	return groupError(group, "reset offsets of "+strings.Join(failed, ", "), first)
}

// groupError explains the group errors whose protocol wording says nothing
// about what to do.
func groupError(group, action string, err error) error {
	switch {
	case errors.Is(err, sarama.ErrUnknownMemberId), errors.Is(err, sarama.ErrRebalanceInProgress):
		return fmt.Errorf("%s: group %s has active members; stop its consumers first", action, group)
	case errors.Is(err, sarama.ErrNonEmptyGroup):
		return fmt.Errorf("%s: group %s still has members", action, group)
	case errors.Is(err, sarama.ErrGroupSubscribedToTopic):
		return fmt.Errorf("%s: group %s is subscribed to that topic; stop its consumers first", action, group)
	case errors.Is(err, sarama.ErrGroupIDNotFound):
		return fmt.Errorf("%s: group %s does not exist", action, group)
	case errors.Is(err, sarama.ErrOffsetsLoadInProgress):
		return fmt.Errorf("%s: the coordinator is still loading offsets, try again", action)
	}
	return fmt.Errorf("%s of %s: %w", action, group, err)
}

// isNoError reports whether a Kafka error field means success. Kafka reports
// success as a code rather than as a nil error, so a plain nil check would treat
// every partition as failed.
func isNoError(err error) bool {
	return err == nil || errors.Is(err, sarama.ErrNoError)
}

func hasKey(m map[topicPartition]int64, key topicPartition) bool {
	_, ok := m[key]
	return ok
}

func topicSuffix(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	return " for " + strings.Join(topics, ", ")
}
