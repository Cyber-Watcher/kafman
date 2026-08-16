package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

// fakeGroupAdmin is a ClusterAdmin stub for the consumer-group lag path. The
// embedded interface is nil, so any call outside the stubbed set panics — which
// is exactly what a test wants from an unexpected round trip.
type fakeGroupAdmin struct {
	sarama.ClusterAdmin

	batchErr    error
	batchCalled bool
	oneByOne    []string

	// offsets holds each group's committed offsets, ends each partition's end
	// offset — the two halves the lag arithmetic runs on.
	offsets map[string]map[string]map[int32]*sarama.OffsetFetchResponseBlock
	ends    map[string]map[int32]*sarama.OffsetResult
}

func (f *fakeGroupAdmin) ListConsumerGroups() (map[string]string, error) {
	out := make(map[string]string, len(f.offsets))
	for name := range f.offsets {
		out[name] = "consumer"
	}
	return out, nil
}

func (f *fakeGroupAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	out := make([]*sarama.GroupDescription, 0, len(groups))
	for _, name := range groups {
		out = append(out, &sarama.GroupDescription{GroupId: name, State: "Empty"})
	}
	return out, nil
}

func (f *fakeGroupAdmin) ListConsumerGroupOffsetsBatch(map[string]map[string][]int32) (map[string]*sarama.OffsetFetchResponseGroup, error) {
	f.batchCalled = true
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	out := make(map[string]*sarama.OffsetFetchResponseGroup, len(f.offsets))
	for name, blocks := range f.offsets {
		out[name] = &sarama.OffsetFetchResponseGroup{GroupId: name, Blocks: blocks}
	}
	return out, nil
}

func (f *fakeGroupAdmin) ListConsumerGroupOffsets(group string, _ map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	f.oneByOne = append(f.oneByOne, group)
	return &sarama.OffsetFetchResponse{Blocks: f.offsets[group]}, nil
}

func (f *fakeGroupAdmin) ListOffsets(map[string]map[int32]int64, *sarama.ListOffsetsOptions) (map[string]map[int32]*sarama.OffsetResult, error) {
	return f.ends, nil
}

func lagFixture() *fakeGroupAdmin {
	return &fakeGroupAdmin{
		offsets: map[string]map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"alpha": {"orders": {0: {Offset: 5}}},
			"beta":  {"orders": {1: {Offset: 10}}},
		},
		ends: map[string]map[int32]*sarama.OffsetResult{
			"orders": {0: {Offset: 15}, 1: {Offset: 12}},
		},
	}
}

// A protocol level below Kafka 3.0 has no batched OffsetFetch; the lag call
// must fall back to one request per group and produce the same numbers.
func TestConsumerGroupsWithLagFallsBackPerGroup(t *testing.T) {
	admin := lagFixture()
	admin.batchErr = sarama.ErrUnsupportedVersion
	c := &Client{admin: admin}

	groups, err := c.ConsumerGroupsWithLag(context.Background())
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag: %v", err)
	}

	if !admin.batchCalled {
		t.Error("the batch should be tried first")
	}
	if len(admin.oneByOne) != 2 {
		t.Errorf("expected one request per group, got %v", admin.oneByOne)
	}
	assertLags(t, groups)
}

// When the batch works, the fallback must stay out of the way.
func TestConsumerGroupsWithLagUsesBatch(t *testing.T) {
	admin := lagFixture()
	c := &Client{admin: admin}

	groups, err := c.ConsumerGroupsWithLag(context.Background())
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag: %v", err)
	}
	if len(admin.oneByOne) != 0 {
		t.Errorf("per-group requests should not run when the batch works, got %v", admin.oneByOne)
	}
	assertLags(t, groups)
}

// Any batch failure other than the version mismatch is a real error and must
// surface, not silently degrade into N round trips.
func TestConsumerGroupsWithLagKeepsOtherErrors(t *testing.T) {
	admin := lagFixture()
	admin.batchErr = sarama.ErrOutOfBrokers
	c := &Client{admin: admin}

	if _, err := c.ConsumerGroupsWithLag(context.Background()); !errors.Is(err, sarama.ErrOutOfBrokers) {
		t.Fatalf("expected ErrOutOfBrokers, got %v", err)
	}
	if len(admin.oneByOne) != 0 {
		t.Errorf("no fallback on unrelated errors, got %v", admin.oneByOne)
	}
}

func assertLags(t *testing.T, groups []ConsumerGroup) {
	t.Helper()

	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	// Sorted by lag, worst first: alpha is 10 behind, beta 2.
	if groups[0].Name != "alpha" || groups[1].Name != "beta" {
		t.Fatalf("expected [alpha beta], got [%s %s]", groups[0].Name, groups[1].Name)
	}
	for i, want := range []int64{10, 2} {
		if !groups[i].LagKnown || groups[i].TotalLag != want {
			t.Errorf("%s: want lag %d (known), got %d known=%v",
				groups[i].Name, want, groups[i].TotalLag, groups[i].LagKnown)
		}
	}
}

// fakePartitionsClient serves partition lists from a map; everything else on
// the embedded nil interface panics, which is what a test wants.
type fakePartitionsClient struct {
	sarama.Client
	partitions map[string][]int32
}

func (f *fakePartitionsClient) Partitions(topic string) ([]int32, error) {
	ids, ok := f.partitions[topic]
	if !ok {
		return nil, sarama.ErrUnknownTopicOrPartition
	}
	return ids, nil
}

func TestNoCommitCount(t *testing.T) {
	c := &Client{client: &fakePartitionsClient{partitions: map[string][]int32{
		"orders": {0, 1, 2, 3},
		"audit":  {0, 1},
	}}}

	// Six assigned partitions, four with commits: two never committed.
	if got := c.noCommitCount([]string{"orders", "audit"}, 4); got != 2 {
		t.Errorf("noCommitCount = %d, want 2", got)
	}

	// A group with no assigned topics is blind, not fully committed.
	if got := c.noCommitCount(nil, 0); got != 0 {
		t.Errorf("no assignment should read as 0, got %d", got)
	}

	// An unknown topic contributes nothing rather than failing the count.
	if got := c.noCommitCount([]string{"gone"}, 0); got != 0 {
		t.Errorf("an unknown topic should contribute nothing, got %d", got)
	}

	// More commits than known partitions must not go negative — the topic may
	// have shrunk from the metadata this count runs on.
	if got := c.noCommitCount([]string{"audit"}, 5); got != 0 {
		t.Errorf("the count must not go negative, got %d", got)
	}
}
