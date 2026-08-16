package metrics

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

func TestSeriesRingWraps(t *testing.T) {
	s := NewSeries(3)
	base := time.Unix(1_700_000_000, 0)

	for i := range 5 {
		s.Add(base.Add(time.Duration(i)*time.Second), float64(i))
	}

	if s.Len() != 3 {
		t.Fatalf("len = %d, want the capacity 3", s.Len())
	}

	values := Values(s.All())
	want := []float64{2, 3, 4}
	for i, value := range want {
		if values[i] != value {
			t.Errorf("all = %v, want the newest three %v", values, want)
			break
		}
	}
	if last := s.LastValue(); last != 4 {
		t.Errorf("last = %v, want 4", last)
	}
}

// The window is measured from the newest sample, not from now: a collector that
// stalled should show its last window rather than an empty chart, which would
// read as "nothing is happening".
func TestWindowIsRelativeToTheLastSample(t *testing.T) {
	s := NewSeries(10)
	base := time.Now().Add(-time.Hour)

	for i := range 10 {
		s.Add(base.Add(time.Duration(i)*time.Minute), float64(i))
	}

	got := s.Window(3 * time.Minute)
	if len(got) != 4 {
		t.Fatalf("window holds %d points, want the last four", len(got))
	}
	if got[len(got)-1].Value != 9 {
		t.Errorf("the window should end at the newest sample, got %v", got)
	}
}

func TestRate(t *testing.T) {
	tests := []struct {
		name              string
		previous, current int64
		elapsed           time.Duration
		want              float64
	}{
		{name: "records written", previous: 100, current: 200, elapsed: time.Second, want: 100},
		{name: "half a second", previous: 100, current: 150, elapsed: 500 * time.Millisecond, want: 100},
		{name: "nothing written", previous: 100, current: 100, elapsed: time.Second, want: 0},
		// A counter that goes backwards means the topic was truncated or
		// recreated, not that records were unwritten.
		{name: "counter reset", previous: 500, current: 10, elapsed: time.Second, want: 0},
		{name: "no time passed", previous: 100, current: 200, elapsed: 0, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Rate(test.previous, test.current, test.elapsed); got != test.want {
				t.Errorf("Rate = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTrendOf(t *testing.T) {
	series := func(values ...float64) []Point {
		base := time.Unix(1_700_000_000, 0)
		points := make([]Point, len(values))
		for i, value := range values {
			points[i] = Point{At: base.Add(time.Duration(i) * time.Second), Value: value}
		}
		return points
	}

	tests := []struct {
		name   string
		points []Point
		want   Trend
	}{
		{name: "flat zero is idle", points: series(0, 0, 0, 0, 0, 0), want: TrendIdle},
		{name: "rising is growing", points: series(10, 20, 30, 40, 50, 60), want: TrendGrowing},
		{name: "falling is draining", points: series(60, 50, 40, 30, 20, 10), want: TrendDraining},
		{name: "flat but non-zero is steady", points: series(100, 100, 100, 100), want: TrendSteady},
		// Lag is never perfectly flat; small noise must not read as a trend.
		{name: "noise is steady", points: series(100, 104, 98, 101, 99, 103), want: TrendSteady},
		{name: "draining to zero", points: series(400, 300, 200, 0, 0, 0), want: TrendDraining},
		{name: "from zero is growing", points: series(0, 0, 0, 10, 20, 30), want: TrendGrowing},
		{name: "too short to tell", points: series(1, 2), want: TrendSteady},
		{name: "nothing at all", points: nil, want: TrendIdle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := TrendOf(test.points); got != test.want {
				t.Errorf("TrendOf = %v, want %v", got, test.want)
			}
		})
	}
}

// ---------------------------------------------------------------- collector ---

// growingMock is a cluster that keeps receiving records.
func growingMock() *kafka.Mock {
	mock := kafka.NewMock(nil)

	var polls atomic.Int64
	mock.TopicEndOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		n := polls.Add(1)
		return map[string]int64{"orders.events": 1000 * n, "payments.tx": 10 * n}, nil
	}
	return mock
}

// poll runs the collector n times and returns its view.
func poll(t *testing.T, collector *Collector, times int) View {
	t.Helper()

	collector.Start(context.Background())
	t.Cleanup(collector.Stop)

	for range times {
		select {
		case <-collector.Updates():
		case <-time.After(5 * time.Second):
			t.Fatal("the collector stopped producing updates")
		}
	}
	return collector.View(MaxWindow)
}

// The first poll has nothing to compare against; inventing a rate from a single
// reading is exactly the kind of made-up number this project refuses.
func TestFirstPollProducesNoRates(t *testing.T) {
	view := poll(t, NewCollector(growingMock(), 5*time.Millisecond), 1)

	if view.Ready() {
		t.Errorf("one reading cannot be a rate, got %v", view.Cluster)
	}
	if view.Polls != 1 {
		t.Errorf("polls = %d, want 1", view.Polls)
	}
}

func TestRatesAreMeasuredBetweenPolls(t *testing.T) {
	view := poll(t, NewCollector(growingMock(), 5*time.Millisecond), 3)

	if !view.Ready() {
		t.Fatal("after three polls there should be rates")
	}
	if len(view.Topics) != 2 {
		t.Fatalf("both topics should have series, got %d", len(view.Topics))
	}

	// 1000 records per poll against 10 makes the order unambiguous whatever the
	// wall-clock interval turned out to be.
	if view.Topics[0].Name != "orders.events" {
		t.Errorf("the busiest topic should sort first, got %q", view.Topics[0].Name)
	}
	if view.Topics[0].Last <= view.Topics[1].Last {
		t.Errorf("rates are not ordered: %v", view.Topics)
	}
	if view.Cluster[len(view.Cluster)-1].Value <= 0 {
		t.Error("the cluster rate should be the sum of the topics")
	}
}

// One failing source must not blank the others: a cluster whose log dirs cannot
// be described still has measurable message rates.
func TestOneFailingSourceDoesNotStopTheRest(t *testing.T) {
	mock := growingMock()
	mock.BrokerSizesFunc = func(context.Context) (map[int32]int64, error) {
		return nil, errors.New("kafka: insufficient permissions to describe log dirs")
	}

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 3)

	if !view.Ready() {
		t.Error("message rates should survive a log-dir failure")
	}
	if len(view.Written) != 0 {
		t.Error("a failed source should produce no samples at all, not zeroes")
	}
	if view.Err == nil {
		t.Error("the failure should still be reported")
	}
}

func TestGroupsThatDisappearAreForgotten(t *testing.T) {
	mock := growingMock()

	var polls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		if polls.Add(1) < 3 {
			return []kafka.ConsumerGroup{
				{Name: "temporary", State: kafka.GroupStable, TotalLag: 5, LagKnown: true},
			}, nil
		}
		return nil, nil
	}

	collector := NewCollector(mock, 5*time.Millisecond)
	view := poll(t, collector, 5)

	for _, group := range view.Groups {
		if group.Name == "temporary" {
			t.Error("a group that no longer exists should not keep a series")
		}
	}
}

func TestTopicSeriesAreCapped(t *testing.T) {
	mock := kafka.NewMock(nil)

	var polls atomic.Int64
	mock.TopicEndOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		n := polls.Add(1)
		ends := make(map[string]int64, TopicLimit+50)
		for i := range TopicLimit + 50 {
			ends[topicName(i)] = int64(i) * n
		}
		return ends, nil
	}

	collector := NewCollector(mock, 5*time.Millisecond)
	view := poll(t, collector, 3)

	if len(view.Topics) > TopicLimit {
		t.Errorf("%d topic series kept, the cap is %d", len(view.Topics), TopicLimit)
	}
	if !view.Ready() {
		t.Error("the cluster total should still be measured for every topic")
	}
}

func topicName(i int) string {
	return "topic-" + string(rune('a'+i%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

// Stop has to return once the goroutine is gone: the client it polls with is
// closed right after, and a poll in flight would fail on a closed connection.
func TestStopWaitsForTheGoroutine(t *testing.T) {
	collector := NewCollector(growingMock(), 5*time.Millisecond)
	collector.Start(context.Background())

	select {
	case <-collector.Updates():
	case <-time.After(5 * time.Second):
		t.Fatal("no first poll")
	}

	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}

// A topic that shows up between two polls has no previous reading to compare
// against. Treating the missing one as zero would turn its whole retained
// history into one poll's worth of traffic — a spike that never happened.
func TestATopicAppearingMidFlightIsNotASpike(t *testing.T) {
	mock := kafka.NewMock(nil)

	var polls atomic.Int64
	mock.TopicEndOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		n := polls.Add(1)
		ends := map[string]int64{"steady": 100 * n}
		if n >= 3 {
			// An old topic with a long history, seen for the first time.
			ends["appeared"] = 5_000_000 + n
		}
		return ends, nil
	}

	collector := NewCollector(mock, 5*time.Millisecond)
	view := poll(t, collector, 5)

	for _, point := range view.Cluster {
		// The steady topic alone can only account for a modest rate; anything
		// near five million means the newcomer's history was counted as traffic.
		if point.Value > 1_000_000 {
			t.Fatalf("a newly seen topic produced a false spike: %v", Values(view.Cluster))
		}
	}

	var appeared *NamedSeries
	for i, series := range view.Topics {
		if series.Name == "appeared" {
			appeared = &view.Topics[i]
		}
	}
	if appeared == nil {
		t.Fatal("the new topic should still get a series once it can be measured")
	}
	if appeared.Last > 1000 {
		t.Errorf("its first measured rate is %v, want the delta between two polls", appeared.Last)
	}
}

// Two polls in quick succession measure a fraction of a second, where one record
// either side of the boundary swings the rate by hundreds per second. Such a
// reading updates the baseline but must not become a sample.
func TestVeryShortIntervalsAreNotSampled(t *testing.T) {
	collector := NewCollector(growingMock(), time.Second)
	collector.Start(context.Background())
	t.Cleanup(collector.Stop)

	select {
	case <-collector.Updates():
	case <-time.After(5 * time.Second):
		t.Fatal("no first poll")
	}

	// Three manual polls, all far sooner than the interval.
	for range 3 {
		collector.Refresh()
		select {
		case <-collector.Updates():
		case <-time.After(5 * time.Second):
			t.Fatal("a manual poll produced no update")
		}
	}

	view := collector.View(MaxWindow)
	if len(view.Cluster) != 0 {
		t.Errorf("polls milliseconds apart should add no samples, got %v", Values(view.Cluster))
	}
	if view.Polls != 4 {
		t.Errorf("polls = %d, want all four to have happened", view.Polls)
	}
	// The absolute values are still refreshed: a manual poll is not a no-op.
	if len(view.Brokers) == 0 {
		t.Error("broker state should be refreshed by a manual poll")
	}
}

// A big lag that climbs slowly is still climbing. The relative threshold alone
// called it steady, because ten percent of a hundred thousand is a lot of
// records — but a series that never turns back is going somewhere.
func TestSlowButRelentlessGrowthIsATrend(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	points := make([]Point, 12)
	for i := range points {
		// 100_000 growing by 15 a step: 0.18% over the whole window.
		points[i] = Point{At: base.Add(time.Duration(i) * time.Second), Value: float64(100_000 + i*15)}
	}

	if got := TrendOf(points); got != TrendGrowing {
		t.Errorf("a lag that only rises is growing, got %v", got)
	}

	for i := range points {
		points[i].Value = float64(100_000 - i*15)
	}
	if got := TrendOf(points); got != TrendDraining {
		t.Errorf("a lag that only falls is draining, got %v", got)
	}

	// Noise around a level is still steady: one dip is enough to say the series
	// is not going one way.
	points[6].Value = points[5].Value + 1
	if got := TrendOf(points); got != TrendSteady {
		t.Errorf("a series that turns back is steady, got %v", got)
	}
}

// consumingMock is a cluster whose group commits offsets at a steady pace.
func consumingMock() *kafka.Mock {
	mock := growingMock()

	var polls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		n := polls.Add(1)
		return []kafka.ConsumerGroup{
			{Name: "billing-worker", State: kafka.GroupStable, Members: 3,
				TotalLag: 100 * n, LagKnown: true,
				CommittedSum: 5000 * n, CommittedParts: 4, CommittedKnown: true},
			{Name: "idle-consumer", State: kafka.GroupEmpty, TotalLag: 0, LagKnown: true,
				CommittedKnown: true, NoCommit: 2},
		}, nil
	}

	var startPolls atomic.Int64
	mock.TopicStartOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		n := startPolls.Add(1)
		return map[string]int64{
			"orders.events": 100 * n, // retention deletes on every poll
			"payments.tx":   0,       // nothing ever deleted
		}, nil
	}
	return mock
}

func groupByName(view View, name string) (GroupSeries, bool) {
	for _, group := range view.Groups {
		if group.Name == name {
			return group, true
		}
	}
	return GroupSeries{}, false
}

// A read rate is a delta: one poll has nothing to compare against, and the
// screen shows a dash rather than a zero.
func TestReadRateNeedsTwoPolls(t *testing.T) {
	view := poll(t, NewCollector(consumingMock(), 5*time.Millisecond), 1)

	group, ok := groupByName(view, "billing-worker")
	if !ok {
		t.Fatal("the group should be in the view")
	}
	if group.ReadKnown || len(group.Read) != 0 {
		t.Errorf("one poll cannot be a read rate: %+v", group)
	}
	// The member count is a gauge and exists from the first poll.
	if group.MembersNow != 3 || len(group.Members) == 0 {
		t.Errorf("members = %d over %d points, want 3 from the first poll",
			group.MembersNow, len(group.Members))
	}
}

func TestReadRateIsMeasuredBetweenPolls(t *testing.T) {
	view := poll(t, NewCollector(consumingMock(), 5*time.Millisecond), 3)

	group, ok := groupByName(view, "billing-worker")
	if !ok {
		t.Fatal("the group should be in the view")
	}
	if !group.ReadKnown || group.ReadLast <= 0 {
		t.Errorf("after three polls the read rate should be positive: %+v", group)
	}
	if len(view.Consumed) == 0 || view.Consumed[len(view.Consumed)-1].Value <= 0 {
		t.Errorf("the cluster-wide consume rate should follow: %v", view.Consumed)
	}

	// The never-committed count came through, and a fully-committed group has
	// none.
	idle, _ := groupByName(view, "idle-consumer")
	if idle.NoCommit != 2 {
		t.Errorf("idle-consumer NoCommit = %d, want 2", idle.NoCommit)
	}
	if group.NoCommit != 0 {
		t.Errorf("billing-worker NoCommit = %d, want 0", group.NoCommit)
	}
}

// A changed committed-partition set means the sum jumped for a structural
// reason; the baseline moves but no sample lands.
func TestReadRateSkipsPartitionSetChanges(t *testing.T) {
	mock := growingMock()
	var polls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		n := polls.Add(1)
		parts := 4
		if n >= 2 {
			parts = 6 // the group starts committing on a new topic
		}
		return []kafka.ConsumerGroup{
			{Name: "shifty", State: kafka.GroupStable, LagKnown: true,
				CommittedSum: 1_000_000 * n, CommittedParts: parts, CommittedKnown: true},
		}, nil
	}

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 2)
	group, _ := groupByName(view, "shifty")
	if group.ReadKnown {
		t.Errorf("a partition-set change must not produce a rate sample: %+v", group.Read)
	}

	// Once the set is stable again, the rate lands.
	view = poll(t, NewCollector(consumingMock(), 5*time.Millisecond), 3)
	if group, _ := groupByName(view, "billing-worker"); !group.ReadKnown {
		t.Error("a stable set should produce samples")
	}
}

// An offset reset makes the committed sum go backwards; the honest reading is
// zero — records were not un-read.
func TestReadRateClampsOffsetResets(t *testing.T) {
	mock := growingMock()
	var polls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		n := polls.Add(1)
		sum := int64(1_000_000)
		if n >= 2 {
			sum = 500 // reset to earliest between polls
		}
		return []kafka.ConsumerGroup{
			{Name: "reset", State: kafka.GroupStable, LagKnown: true,
				CommittedSum: sum, CommittedParts: 4, CommittedKnown: true},
		}, nil
	}

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 3)
	group, _ := groupByName(view, "reset")
	if !group.ReadKnown {
		t.Fatal("the rate should be measured")
	}
	for _, p := range group.Read {
		if p.Value < 0 {
			t.Errorf("a reset must read as zero, not negative: %v", p.Value)
		}
	}
}

// A group that disappeared takes its whole history with it.
func TestDisappearedGroupPurgesEverything(t *testing.T) {
	mock := growingMock()
	var polls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		if polls.Add(1) >= 3 {
			return nil, nil
		}
		return []kafka.ConsumerGroup{
			{Name: "doomed", State: kafka.GroupStable, LagKnown: true, Members: 2,
				CommittedSum: 100, CommittedParts: 1, CommittedKnown: true},
		}, nil
	}

	collector := NewCollector(mock, 5*time.Millisecond)
	view := poll(t, collector, 4)
	if len(view.Groups) != 0 {
		t.Errorf("the group should be gone from the view: %+v", view.Groups)
	}
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	if len(collector.groupRead) != 0 || len(collector.members) != 0 ||
		len(collector.prevCommit) != 0 || len(collector.noCommit) != 0 {
		t.Error("a vanished group must be purged from every map")
	}
}

// Retention: the retained count is a gauge, the deletion rate needs two polls.
func TestRetentionIsMeasured(t *testing.T) {
	view := poll(t, NewCollector(consumingMock(), 5*time.Millisecond), 3)

	var orders, payments *NamedSeries
	for i := range view.Topics {
		switch view.Topics[i].Name {
		case "orders.events":
			orders = &view.Topics[i]
		case "payments.tx":
			payments = &view.Topics[i]
		}
	}
	if orders == nil || payments == nil {
		t.Fatalf("both topics should be in the view: %+v", view.Topics)
	}

	if !orders.RetainedKnown || orders.Retained <= 0 {
		t.Errorf("retained should be end minus oldest: %+v", orders)
	}
	if len(orders.Head) == 0 || orders.HeadLast <= 0 {
		t.Errorf("an advancing retention head is a deletion rate: %+v", orders.Head)
	}
	if payments.HeadLast != 0 {
		t.Errorf("a topic that deletes nothing has a zero head rate: %v", payments.HeadLast)
	}
}

// Replication health is a gauge pair recorded from the first poll.
func TestReplicationHealthSeries(t *testing.T) {
	mock := consumingMock()
	mock.BrokerLoadsFunc = func(context.Context) ([]kafka.BrokerLoad, error) {
		return []kafka.BrokerLoad{
			{ID: 1, UnderReplicated: 2, NotPreferred: 1},
			{ID: 2, NotPreferred: 3},
		}, nil
	}

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 1)
	if len(view.URP) == 0 || view.URP[len(view.URP)-1].Value != 2 {
		t.Errorf("URP should sum over the brokers: %v", view.URP)
	}
	if len(view.NotPreferred) == 0 || view.NotPreferred[len(view.NotPreferred)-1].Value != 4 {
		t.Errorf("not-preferred should sum over the brokers: %v", view.NotPreferred)
	}
}
