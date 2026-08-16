package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// The placement of a partition is the whole arithmetic of a reset, and the one
// thing that must not be wrong: it decides which records a group re-reads.
func TestPlaceChange(t *testing.T) {
	// A partition holding offsets 100..900, with the group at 500.
	base := OffsetChange{Current: 500, Oldest: 100, Newest: 900}

	tests := []struct {
		name    string
		spec    ResetSpec
		change  OffsetChange
		at      int64
		atKnown bool

		want    int64
		clamped bool
		note    bool
	}{
		{
			name: "to-earliest lands on the first record",
			spec: ResetSpec{Kind: ResetToEarliest}, change: base, want: 100,
		},
		{
			name: "to-latest lands past the last record",
			spec: ResetSpec{Kind: ResetToLatest}, change: base, want: 900,
		},
		{
			name: "to-offset inside the partition is taken as given",
			spec: ResetSpec{Kind: ResetToOffset, Offset: 700}, change: base, want: 700,
		},
		{
			name: "an offset before the partition is clamped to its start",
			spec: ResetSpec{Kind: ResetToOffset, Offset: 5}, change: base,
			want: 100, clamped: true,
		},
		{
			name: "an offset past the partition is clamped to its end",
			spec: ResetSpec{Kind: ResetToOffset, Offset: 5000}, change: base,
			want: 900, clamped: true,
		},
		{
			name: "shift-by moves back from the committed position",
			spec: ResetSpec{Kind: ResetShiftBy, Shift: -200}, change: base, want: 300,
		},
		{
			name: "shift-by forward moves ahead",
			spec: ResetSpec{Kind: ResetShiftBy, Shift: 250}, change: base, want: 750,
		},
		{
			name: "shifting past the start stops at the start",
			spec: ResetSpec{Kind: ResetShiftBy, Shift: -100_000}, change: base,
			want: 100, clamped: true,
		},
		{
			// With nothing committed there is no position to shift from, so the
			// start of the partition is the only honest base — and it is said so.
			name:   "shifting a never-committed partition starts from the beginning",
			spec:   ResetSpec{Kind: ResetShiftBy, Shift: 50},
			change: OffsetChange{Current: -1, Oldest: 100, Newest: 900},
			want:   150, note: true,
		},
		{
			name:   "to-datetime uses the offset Kafka reports for the timestamp",
			spec:   ResetSpec{Kind: ResetToTime, At: time.Unix(0, 0)},
			change: base, at: 640, atKnown: true, want: 640,
		},
		{
			// Kafka answers -1 when every record is older than the timestamp.
			name:   "a timestamp after every record lands at the end",
			spec:   ResetSpec{Kind: ResetToTime},
			change: base, at: -1, atKnown: true,
			want: 900, note: true,
		},
		{
			name:   "a partition with no answer at all lands at the end",
			spec:   ResetSpec{Kind: ResetToTime},
			change: base, atKnown: false,
			want: 900, note: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			change := test.change
			placeChange(&change, test.spec, test.at, test.atKnown)

			if change.New != test.want {
				t.Errorf("new offset is %d, want %d", change.New, test.want)
			}
			if change.Clamped != test.clamped {
				t.Errorf("clamped is %v, want %v", change.Clamped, test.clamped)
			}
			if (change.Note != "") != test.note {
				t.Errorf("note is %q, want one: %v", change.Note, test.note)
			}
		})
	}
}

func TestOffsetChangeArithmetic(t *testing.T) {
	change := OffsetChange{Current: 500, New: 300, Oldest: 100, Newest: 900}

	if got := change.Delta(); got != -200 {
		t.Errorf("delta is %d, want -200", got)
	}
	if got := change.LagAfter(); got != 600 {
		t.Errorf("lag after is %d, want 600", got)
	}
	if !change.Moves() {
		t.Error("a change to a different offset moves")
	}

	still := OffsetChange{Current: 500, New: 500}
	if still.Moves() {
		t.Error("a change to the same offset does not move")
	}

	// Never committed: a delta against -1 would be a made-up number.
	unread := OffsetChange{Current: -1, New: 100, Newest: 900}
	if got := unread.Delta(); got != 0 {
		t.Errorf("the delta of a never-committed partition is %d, want 0", got)
	}
	if !unread.Moves() {
		t.Error("committing a position for a never-committed partition is a move")
	}
}

// Kafka reports success as an error code rather than as nil, so a plain nil
// check would treat every partition as failed.
func TestIsNoError(t *testing.T) {
	if !isNoError(nil) {
		t.Error("nil is success")
	}
	if !isNoError(sarama.ErrNoError) {
		t.Error("ErrNoError is success")
	}
	if isNoError(sarama.ErrUnknownMemberId) {
		t.Error("a real error is not success")
	}
}

func TestCommitErrorsNamesThePartitions(t *testing.T) {
	response := &sarama.OffsetCommitResponse{
		Errors: map[string]map[int32]sarama.KError{
			"orders.events": {0: sarama.ErrNoError, 3: sarama.ErrUnknownMemberId},
		},
	}

	err := commitErrors("billing-worker", response)
	if err == nil {
		t.Fatal("a failed partition should be reported")
	}
	for _, want := range []string{"orders.events p3", "active members"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should mention %q, got %q", want, err)
		}
	}

	clean := &sarama.OffsetCommitResponse{
		Errors: map[string]map[int32]sarama.KError{
			"orders.events": {0: sarama.ErrNoError},
		},
	}
	if err := commitErrors("billing-worker", clean); err != nil {
		t.Errorf("a clean response is not an error, got %v", err)
	}
}

// The protocol wording of these says nothing about what to do about them.
func TestGroupErrorExplains(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{sarama.ErrUnknownMemberId, "stop its consumers first"},
		{sarama.ErrRebalanceInProgress, "stop its consumers first"},
		{sarama.ErrGroupSubscribedToTopic, "stop its consumers first"},
		{sarama.ErrGroupIDNotFound, "does not exist"},
		{sarama.ErrOffsetsLoadInProgress, "try again"},
	}

	for _, test := range tests {
		got := groupError("billing-worker", "reset offsets", test.err).Error()
		if !strings.Contains(got, test.want) {
			t.Errorf("%v should be explained with %q, got %q", test.err, test.want, got)
		}
	}

	// Anything else keeps the original error, wrapped rather than swallowed.
	other := errors.New("dial tcp: connection refused")
	wrapped := groupError("g", "reset offsets", other)
	if !errors.Is(wrapped, other) {
		t.Errorf("an unrecognised error should stay wrapped, got %v", wrapped)
	}
}

func TestResetKindNames(t *testing.T) {
	// The names are what the preview and the notification show, so they are
	// worth pinning: they match kafka-consumer-groups.
	want := map[ResetKind]string{
		ResetToEarliest: "to-earliest",
		ResetToLatest:   "to-latest",
		ResetToOffset:   "to-offset",
		ResetToTime:     "to-datetime",
		ResetShiftBy:    "shift-by",
	}
	for kind, name := range want {
		if got := kind.String(); got != name {
			t.Errorf("%d is %q, want %q", kind, got, name)
		}
	}
}

// A partition the cluster cannot report on must not silently shrink a group's
// lag: an understated number is worse than a dash, because it looks fine.
func TestTotalLagIsIncompleteWhenAPartitionIsMissing(t *testing.T) {
	blocks := map[string]map[int32]*sarama.OffsetFetchResponseBlock{
		"orders.events": {
			0: {Offset: 100},
			1: {Offset: 200},
			2: {Offset: -1}, // never committed here
		},
	}

	full := map[topicPartition]int64{
		{"orders.events", 0}: 150,
		{"orders.events", 1}: 260,
	}
	stats := lagStatsOf(blocks, full)
	if stats.lag != 110 || !stats.lagKnown {
		t.Errorf("lag = %d known = %v, want 110 and known", stats.lag, stats.lagKnown)
	}

	// The committed sum skips the -1 sentinel: never-committed is not zero.
	if stats.committed != 300 || stats.parts != 2 {
		t.Errorf("committed = %d over %d parts, want 300 over 2", stats.committed, stats.parts)
	}

	// The end offset of one partition is missing — the topic was deleted, or its
	// leader is being elected.
	partial := map[topicPartition]int64{{"orders.events", 0}: 150}
	stats = lagStatsOf(blocks, partial)
	if stats.lagKnown {
		t.Error("a missing partition should make the total incomplete")
	}
	if stats.lag != 50 {
		t.Errorf("lag over what is known = %d, want 50", stats.lag)
	}
	// The committed sum is unaffected: it needs no end offsets.
	if stats.committed != 300 || stats.parts != 2 {
		t.Errorf("committed = %d over %d parts, want 300 over 2", stats.committed, stats.parts)
	}
}

// missingBounds names what could not be read, which is what an offset reset
// refuses on.
func TestMissingBounds(t *testing.T) {
	targets := map[topicPartition]int64{
		{"orders.events", 0}: 0,
		{"orders.events", 1}: 0,
		{"payments.tx", 0}:   0,
	}
	resolved := map[topicPartition]int64{
		{"orders.events", 0}: 10,
		{"payments.tx", 0}:   20,
	}

	missing := missingBounds(targets, resolved)
	if len(missing) != 1 || missing[0] != "orders.events p1" {
		t.Errorf("missing = %v, want the one partition that did not answer", missing)
	}

	if got := missingBounds(targets, targets); len(got) != 0 {
		t.Errorf("nothing should be missing when everything resolved, got %v", got)
	}
}
