package messages

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/query"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

func newTab(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	mock := kafka.NewMock(nil)
	m := New(5000, nil)
	m.SetClient(mock)

	topics, _ := mock.ListTopics(context.Background(), true)
	m.Update(msg.TopicsLoaded{Topics: topics})
	return m, mock
}

// withRecords returns a tab that has already received a batch.
func withRecords(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()
	m, mock := newTab(t)
	m.records.Append(kafka.MockRecords([]string{"orders.events"}))
	m.setFocus(focusRecords)
	return m, mock
}

func render(t *testing.T, m *Model) string {
	t.Helper()
	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k := ktx.New(cfg, theme.New(theme.Dark, false), 130, 30)
	return ansi.Strip(m.View(k))
}

func key(s string) tea.KeyMsg {
	switch s {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "space":
		return tea.KeyMsg{Type: tea.KeySpace}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "ctrl+o":
		return tea.KeyMsg{Type: tea.KeyCtrlO}
	case "ctrl+u":
		return tea.KeyMsg{Type: tea.KeyCtrlU}
	case "f4":
		return tea.KeyMsg{Type: tea.KeyF4}
	case "/":
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")}
	case "ctrl+e":
		return tea.KeyMsg{Type: tea.KeyCtrlE}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

// ------------------------------------------------------------ topic panel ---

func TestTopicListHidesInternalByDefault(t *testing.T) {
	m, _ := newTab(t)

	if view := render(t, m); strings.Contains(view, "__consumer_offsets") {
		t.Errorf("internal topics should be hidden by default:\n%s", view)
	}
	m.Update(key("f4"))
	if view := render(t, m); !strings.Contains(view, "__consumer_offsets") {
		t.Errorf("F4 should reveal internal topics:\n%s", view)
	}
}

func TestTypingFiltersTopics(t *testing.T) {
	m, _ := newTab(t)

	// No mode to enter: typing filters straight away.
	m.Update(key("pay"))

	view := render(t, m)
	if !strings.Contains(view, "payments.tx") {
		t.Errorf("the matching topic should remain:\n%s", view)
	}
	if strings.Contains(view, "audit.log") {
		t.Errorf("non-matching topics should be filtered out:\n%s", view)
	}
	if !m.AcceptsText() {
		t.Error("while filtering, keys belong to the panel rather than the global bindings")
	}

	m.Update(key("esc"))
	if view := render(t, m); !strings.Contains(view, "audit.log") {
		t.Errorf("Esc should clear the filter:\n%s", view)
	}
}

func TestSelectionSurvivesFilterChanges(t *testing.T) {
	m, _ := newTab(t)

	m.Update(key("space")) // tick the first topic
	selected := m.topics.Selected()
	if len(selected) != 1 {
		t.Fatalf("selected %v, want exactly one topic", selected)
	}

	m.Update(key("x")) // filter to nothing
	m.Update(key("esc"))

	if got := m.topics.Selected(); len(got) != 1 || got[0] != selected[0] {
		t.Errorf("selection = %v, want it preserved across filtering", got)
	}
}

func TestSelectedTopicsFloatToTheTop(t *testing.T) {
	m, _ := newTab(t)

	// Tick the last topic, then force a rebuild the way a refresh would.
	m.topics.cursor = len(m.topics.visibleTopics) - 1
	last := m.topics.currentName()
	m.Update(key("space"))
	m.topics.rebuild()

	if got := m.topics.visibleTopics[0].Name; got != last {
		t.Errorf("first topic = %q, want the selected %q floated to the top", got, last)
	}
}

func TestClearSelection(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.Update(key("ctrl+u"))

	if got := m.topics.Selected(); len(got) != 0 {
		t.Errorf("Ctrl+U should clear the selection, got %v", got)
	}
}

func TestHidingTheTopicPanel(t *testing.T) {
	m, _ := newTab(t)

	if view := render(t, m); !strings.Contains(view, "Topics") {
		t.Fatal("the topic panel should be visible initially")
	}
	m.Update(key("ctrl+o"))
	if view := render(t, m); strings.Contains(view, "─ Topics ") {
		t.Errorf("Ctrl+O should hide the topic panel:\n%s", view)
	}
}

// ---------------------------------------------------------- record table ---

func TestRecordsAreRendered(t *testing.T) {
	m, _ := withRecords(t)
	view := render(t, m)

	for _, want := range []string{"ord-99f", "FAILED", "orders.events["} {
		if !strings.Contains(view, want) {
			t.Errorf("the record table should show %q:\n%s", want, view)
		}
	}
}

func TestRecordFormatsAreDistinguished(t *testing.T) {
	m, _ := withRecords(t)
	view := render(t, m)

	// The mock includes a Confluent payload; it must be labelled, not dumped as
	// though it were text.
	if !strings.Contains(view, "Confluent wire format") {
		t.Errorf("an Avro payload should be named rather than shown as bytes:\n%s", view)
	}
}

func TestFollowStopsOnManualMovement(t *testing.T) {
	m, _ := withRecords(t)

	if !m.records.Following() {
		t.Fatal("follow should be on by default")
	}
	m.Update(key("up"))
	if m.records.Following() {
		t.Error("moving by hand should switch follow off — fighting the cursor is worse than a keypress")
	}

	m.Update(key("f"))
	if !m.records.Following() {
		t.Error("f should turn follow back on")
	}
}

func TestFollowKeepsTheNewestSelected(t *testing.T) {
	m, _ := withRecords(t)

	before := m.records.Selected()
	if before == nil {
		t.Fatal("a record should be selected")
	}
	// A new batch arrives while following.
	m.records.Append([]kafka.Record{{
		Topic: "orders.events", Partition: 9, Offset: 999_999,
		Key: []byte("newest"), Value: []byte("{}"),
	}})

	if got := m.records.Selected(); got == nil || string(got.record.Key) != "newest" {
		t.Error("while following, the newest record should stay selected")
	}
}

func TestReverseOrderKeepsTheSelection(t *testing.T) {
	m, _ := withRecords(t)
	m.Update(key("up")) // stop following, move off the end
	selected := m.records.Selected().record.Offset

	m.Update(key("r"))

	if got := m.records.Selected().record.Offset; got != selected {
		t.Errorf("reversing the order moved the selection from offset %d to %d",
			selected, got)
	}
}

func TestTimestampFormatCycles(t *testing.T) {
	m, _ := withRecords(t)

	first := render(t, m)
	m.Update(key("t"))
	second := render(t, m)

	if first == second {
		t.Error("t should change the timestamp format")
	}
	// The full format carries the date, the compact one does not.
	if !strings.Contains(second, "2026-08-04") {
		t.Errorf("the second format should be the full timestamp:\n%s", second)
	}
}

func TestColumnResize(t *testing.T) {
	m, _ := withRecords(t)

	before := m.records.columnPadding
	m.Update(tea.KeyMsg{Type: tea.KeyRight})
	if m.records.columnPadding <= before {
		t.Error("the right arrow should widen the topic and key columns")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	if m.records.columnPadding != before {
		t.Error("the left arrow should narrow them again")
	}
}

func TestRingBufferDropsOldest(t *testing.T) {
	m, _ := newTab(t)
	m.records = newRecordList(10)

	for i := range 25 {
		m.records.Append([]kafka.Record{{
			Topic: "t", Offset: int64(i), Value: []byte("x"),
		}})
	}

	if got := m.records.Len(); got != 10 {
		t.Errorf("buffer holds %d records, want the capacity of 10", got)
	}
	// The oldest are the ones dropped.
	if first := m.records.rows[0].record.Offset; first != 15 {
		t.Errorf("oldest retained offset = %d, want 15", first)
	}
}

// --------------------------------------------------------------- reading ---

func TestReadingRequiresATopic(t *testing.T) {
	m, _ := newTab(t)

	cmd := m.Update(key("enter"))
	if cmd == nil {
		t.Fatal("pressing Enter with nothing selected should explain what to do")
	}
	notification, ok := cmd().(msg.Notify)
	if !ok || !strings.Contains(notification.Text, "Space") {
		t.Errorf("got %#v, want a hint naming the key to press", cmd())
	}
}

func TestReadingStartsForSelectedTopics(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	if !mock.Called("ReadRecords") {
		t.Errorf("Enter should start a read; calls: %v", mock.Calls())
	}
	if !m.Reading() {
		t.Error("the tab should report that a read is running")
	}
	// Focus moves to the records: that is where the user is now looking.
	if m.focus != focusRecords {
		t.Error("starting a read should move focus to the record panel")
	}
}

func TestEscapeStopsAReadRatherThanQuitting(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}

	cmd := m.Update(key("esc"))
	if m.Reading() {
		t.Error("Esc should stop the read")
	}
	if cmd == nil {
		t.Error("stopping should be acknowledged")
	}
}

func TestFocusTopicFromAnotherTabSelectsAndReads(t *testing.T) {
	m, mock := newTab(t)

	if cmd := m.Update(msg.FocusTopic{Topic: "payments.tx"}); cmd != nil {
		cmd()
	}

	if got := m.topics.Selected(); len(got) != 1 || got[0] != "payments.tx" {
		t.Errorf("selection = %v, want only the requested topic", got)
	}
	if !mock.Called("ReadRecords") {
		t.Error("arriving from another tab should start reading straight away")
	}
}

func TestDisconnectStopsReadingAndClears(t *testing.T) {
	m, _ := withRecords(t)
	m.Update(key("space"))
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}

	m.SetClient(nil)

	if m.Reading() {
		t.Error("losing the connection must stop the read")
	}
	if m.RecordCount() != 0 {
		t.Error("records from a closed connection should not stay on screen")
	}
	if view := render(t, m); !strings.Contains(view, "not connected") {
		t.Errorf("the tab should say it is not connected:\n%s", view)
	}
}

func TestStartPositionCycles(t *testing.T) {
	m, _ := newTab(t)

	seen := make(map[string]bool)
	for range 4 {
		seen[startText(m.start)] = true
		m.Update(key("s"))
	}
	for _, want := range []string{"end - 100", "end - 1000", "beginning", "end"} {
		if !seen[want] {
			t.Errorf("cycling the start position should reach %q, saw %v", want, seen)
		}
	}
}

// --------------------------------------------------------------- details ---

func TestOpeningRecordDetails(t *testing.T) {
	m, _ := withRecords(t)

	m.Update(key("enter"))
	if m.details == nil {
		t.Fatal("Enter on a record should open the details")
	}

	view := render(t, m)
	for _, want := range []string{"Topic", "Partition", "Offset", "Timestamp", "Key"} {
		if !strings.Contains(view, want) {
			t.Errorf("the details should show %q:\n%s", want, view)
		}
	}
	if got := m.Breadcrumb(); len(got) != 1 || got[0] != "Record" {
		t.Errorf("breadcrumb = %v, want [Record]", got)
	}
}

func TestArrowsStepThroughRecordsWithDetailsOpen(t *testing.T) {
	m, _ := withRecords(t)
	m.Update(key("up")) // leave follow mode part-way up the list
	m.Update(key("enter"))

	before := m.details.row.record.Offset
	m.Update(key("up"))
	after := m.details.row.record.Offset

	// Scanning records without closing and reopening the overlay is the whole
	// point of this behaviour.
	if before == after {
		t.Error("the arrows should step through records while the details stay open")
	}
	if m.details == nil {
		t.Error("stepping must not close the overlay")
	}
}

func TestEscapeClosesDetailsFirst(t *testing.T) {
	m, _ := withRecords(t)
	m.Update(key("space"))
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	m.setFocus(focusRecords)
	m.Update(key("enter")) // open details

	if m.details == nil {
		t.Fatal("setup: the details should be open")
	}
	m.Update(key("esc"))

	if m.details != nil {
		t.Error("Esc should close the details before it stops anything else")
	}
}

func TestDetailsShowConfluentHonestly(t *testing.T) {
	m, _ := withRecords(t)

	// Walk to the Confluent record, which is last in the mock set.
	m.records.follow = false
	m.records.cursor = m.records.Len() - 1
	m.Update(key("enter"))

	view := render(t, m)
	if !strings.Contains(view, "schema id 42") {
		t.Errorf("the schema id should be named:\n%s", view)
	}
	if !strings.Contains(view, "Schema Registry is not configured") {
		t.Errorf("the details should say why it cannot be decoded:\n%s", view)
	}
}

func TestDetailsTextForClipboard(t *testing.T) {
	m, _ := withRecords(t)
	m.records.follow = false
	m.records.cursor = 0
	m.Update(key("enter"))

	text := m.details.Text()
	for _, want := range []string{"topic:", "partition:", "offset:", "key:"} {
		if !strings.Contains(text, want) {
			t.Errorf("the clipboard text should contain %q:\n%s", want, text)
		}
	}
}

// ---------------------------------------------------------------- query line ---

func TestSlashOpensTheQueryLine(t *testing.T) {
	m, _ := newTab(t)

	m.Update(key("/"))
	if m.focus != focusQuery {
		t.Fatal("/ should focus the query line")
	}
	if !m.AcceptsText() {
		t.Error("with the query focused, keys belong to it rather than the global bindings")
	}

	// Typing goes into the query, not the topic filter.
	for _, r := range `key == "a"` {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	if got := m.query.Text(); got != `key == "a"` {
		t.Errorf("query text = %q, want the typed text", got)
	}
}

func TestQueryErrorIsShownInPlace(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("/"))
	m.query.SetText(`key == "a" and nonsense`)

	// Validation is debounced, so drive it directly.
	m.Update(queryValidatedMsg{Text: m.query.Text(), Err: &query.Error{
		Message: "unknown field", Position: 15, Tail: "nonsense",
	}})

	if m.query.Err() == nil {
		t.Fatal("the parse error should be recorded")
	}
	view := render(t, m)
	if !strings.Contains(view, "unknown field") {
		t.Errorf("the error message should be visible:\n%s", view)
	}
}

func TestStaleValidationIsIgnored(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("/"))
	m.query.SetText("current text")

	// A result for text that has since changed must not mark the query bad.
	m.Update(queryValidatedMsg{Text: "older text", Err: &query.Error{Message: "stale"}})

	if m.query.Err() != nil {
		t.Error("a validation result for older text should be discarded")
	}
}

func TestQueryHistoryNavigation(t *testing.T) {
	m, _ := newTab(t)
	m.query.Remember(`key == "first"`)
	m.query.Remember(`key == "second"`)

	m.Update(key("/"))
	m.Update(key("up"))
	if got := m.query.Text(); got != `key == "second"` {
		t.Errorf("up should recall the most recent query, got %q", got)
	}

	m.Update(key("up"))
	if got := m.query.Text(); got != `key == "first"` {
		t.Errorf("up again should go further back, got %q", got)
	}

	m.Update(key("down"))
	m.Update(key("down"))
	if got := m.query.Text(); got != "" {
		t.Errorf("coming back past the newest entry should restore the draft, got %q", got)
	}
}

func TestHistoryHasNoDuplicates(t *testing.T) {
	m, _ := newTab(t)
	m.query.Remember("a")
	m.query.Remember("b")
	m.query.Remember("a") // moves to the end rather than duplicating

	if got := m.query.History(); len(got) != 2 || got[1] != "a" {
		t.Errorf("history = %v, want [b a]", got)
	}
}

func TestSuggestionCompletesFromHistory(t *testing.T) {
	m, _ := newTab(t)
	m.query.Remember(`key contains "order-99"`)

	m.Update(key("/"))
	for _, r := range "key cont" {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	if got := m.query.suggestion(); got != `key contains "order-99"` {
		t.Errorf("suggestion = %q, want the matching history entry", got)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyRight})
	if got := m.query.Text(); got != `key contains "order-99"` {
		t.Errorf("the right arrow should accept the suggestion, got %q", got)
	}
}

// ----------------------------------------------------------------- filtering ---

func TestFilterKeepsOnlyMatchingRecords(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.query.SetText(`value contains "FAILED"`)

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}

	// Feed a batch by hand: one record matches, three do not.
	m.Update(recordsMsg{batch: kafka.Batch{
		Records: kafka.MockRecords([]string{"orders.events"}),
		Read:    4, Matched: 4,
	}})

	if got := m.RecordCount(); got != 1 {
		t.Errorf("kept %d records, want only the matching one", got)
	}
	if m.read != 4 {
		t.Errorf("read = %d, want all four counted as read", m.read)
	}
	if m.matched != 1 {
		t.Errorf("matched = %d, want 1", m.matched)
	}
}

func TestCounterShowsMatchedAndRead(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.query.SetText(`value contains "FAILED"`)
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	m.Update(recordsMsg{batch: kafka.Batch{
		Records: kafka.MockRecords([]string{"orders.events"}), Read: 4, Matched: 4,
	}})

	if view := render(t, m); !strings.Contains(view, "1 / 4") {
		t.Errorf("the border counter should show matched over read:\n%s", view)
	}
}

func TestQueryDrivesTheStartPosition(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	m.query.SetText("from beginning")

	var got kafka.ReadOptions
	mock.ReadRecordsFunc = func(ctx context.Context, opts kafka.ReadOptions) (*kafka.ReadSession, error) {
		got = opts
		return kafka.MockSession(ctx, nil), nil
	}

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	if got.Start.Kind != kafka.StartBeginning {
		t.Errorf("start = %v, want the query's from clause to win", got.Start.Kind)
	}
}

func TestFilteredReadIsNotBoundedByTheLimit(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	// With a filter, the reader must keep going until enough records match.
	m.query.SetText(`value contains "FAILED" limit 5`)

	var got kafka.ReadOptions
	mock.ReadRecordsFunc = func(ctx context.Context, opts kafka.ReadOptions) (*kafka.ReadSession, error) {
		got = opts
		return kafka.MockSession(ctx, nil), nil
	}

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	if got.Limit != 0 {
		t.Errorf("read limit = %d, want the reader unbounded so the filter can find matches", got.Limit)
	}
}

func TestUnfilteredLimitIsPassedToTheReader(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	m.query.SetText("limit 25")

	var got kafka.ReadOptions
	mock.ReadRecordsFunc = func(ctx context.Context, opts kafka.ReadOptions) (*kafka.ReadSession, error) {
		got = opts
		return kafka.MockSession(ctx, nil), nil
	}

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	if got.Limit != 25 {
		t.Errorf("read limit = %d, want 25 passed straight through", got.Limit)
	}
}

func TestInvalidQueryRefusesToRead(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	m.query.SetText("key ==")

	cmd := m.Update(key("enter"))
	if mock.Called("ReadRecords") {
		t.Error("an unparsable query should not start a read")
	}
	if cmd == nil {
		t.Fatal("the refusal should be explained")
	}
	if _, ok := cmd().(msg.Notify); !ok {
		t.Errorf("got %T, want an explanation", cmd())
	}
}

func TestRunningAQueryRemembersIt(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.query.SetText(`key == "abc"`)

	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	history := m.QueryHistory()
	if len(history) == 0 || history[len(history)-1] != `key == "abc"` {
		t.Errorf("history = %v, want the query that was just run", history)
	}
}

// ------------------------------------------------------ produce and export ---

func TestProduceFormOpensForTheSelectedTopic(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	selected := m.topics.Selected()[0]

	m.Update(key("p"))
	if m.produce == nil {
		t.Fatal("P should open the produce form")
	}
	if m.produce.topic != selected {
		t.Errorf("form topic = %q, want the selected %q", m.produce.topic, selected)
	}
	if !m.AcceptsText() {
		t.Error("with the form open, keys belong to it rather than the global bindings")
	}
	if got := m.Breadcrumb(); len(got) != 1 || got[0] != "Produce" {
		t.Errorf("breadcrumb = %v, want [Produce]", got)
	}
}

func TestProduceFormNeedsATopic(t *testing.T) {
	m, _ := newTab(t)

	cmd := m.Update(key("p"))
	if m.produce != nil {
		t.Error("with nothing selected there is no topic to publish to")
	}
	if cmd == nil {
		t.Fatal("the refusal should be explained")
	}
}

func TestProduceFormRejectsBrokenJSON(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.Update(key("p"))

	m.produce.value.SetValue(`{"unclosed": `)
	if m.produce.value.Err() == nil {
		t.Error("a payload that looks like JSON but does not parse should be flagged")
	}

	// Plain text is fine: not every topic carries JSON.
	m.produce.value.SetValue("just some text")
	if err := m.produce.value.Err(); err != nil {
		t.Errorf("plain text should be accepted: %v", err)
	}
}

func TestProduceSendsTheRecord(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	topic := m.topics.Selected()[0]
	m.Update(key("p"))

	m.produce.key.SetValue("ord-test")
	m.produce.value.SetValue(`{"status":"NEW"}`)
	m.produce.headers.SetValue("source=kafman, trace-id=abc")

	pressFormButton(t, m, buttonSend)

	if len(mock.Produced) != 1 {
		t.Fatalf("published %d records, want 1", len(mock.Produced))
	}
	req := mock.Produced[0]
	if req.Topic != topic {
		t.Errorf("topic = %q, want %q", req.Topic, topic)
	}
	if string(req.Key) != "ord-test" {
		t.Errorf("key = %q", req.Key)
	}
	if len(req.Headers) != 2 {
		t.Errorf("headers = %v, want both parsed", req.Headers)
	}
	if req.Manual {
		t.Error("the default is partitioning by key, not a manual partition")
	}
}

func TestProduceManualPartition(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	m.Update(key("p"))

	m.produce.partitionMode.SelectValue(partitionManual)
	m.produce.partition.SetValue("5")
	m.produce.value.SetValue("x")

	pressFormButton(t, m, buttonSend)

	if len(mock.Produced) != 1 {
		t.Fatalf("published %d records, want 1", len(mock.Produced))
	}
	if req := mock.Produced[0]; !req.Manual || req.Partition != 5 {
		t.Errorf("request = %+v, want partition 5 chosen manually", req)
	}
}

func TestProduceEscapeCancels(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("space"))
	m.Update(key("p"))
	m.Update(key("esc"))

	if m.produce != nil {
		t.Error("Esc should close the form")
	}
	if len(mock.Produced) != 0 {
		t.Error("cancelling must not publish anything")
	}
}

// pressFormButton walks focus to a button and presses it.
func pressFormButton(t *testing.T, m *Model, label string) {
	t.Helper()
	for range 40 {
		m.Update(key("tab"))
		if m.produce == nil {
			return
		}
		if m.produce.form.FocusedButton() == label {
			if cmd := m.Update(key("enter")); cmd != nil {
				if produced, ok := cmd().(producedMsg); ok {
					m.Update(produced)
				}
			}
			return
		}
	}
	t.Fatalf("never reached the %q button", label)
}

func TestExportWritesAFile(t *testing.T) {
	m, _ := newTab(t)
	dir := t.TempDir()
	m.SetContext("test-cluster", dir)
	m.Update(key("space"))
	m.query.SetText(`key contains "ord"`)
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}
	// Starting a read clears the buffer, so the records go in afterwards.
	m.records.Append(kafka.MockRecords([]string{"orders.events"}))

	cmd := m.Update(key("ctrl+e"))
	if cmd == nil {
		t.Fatal("Ctrl+E should export")
	}
	notification, ok := cmd().(msg.Notify)
	if !ok || notification.Level != msg.NotifySuccess {
		t.Fatalf("got %#v, want a success notification", cmd())
	}

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected one export file in %s, got %v (%v)", dir, entries, err)
	}

	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Errorf("the export is not valid JSON:\n%s", data)
	}
	// The query travels with the export so the set can be reproduced.
	if !strings.Contains(string(data), `key contains`) {
		t.Errorf("the export should record the query:\n%s", data)
	}
	if !strings.Contains(string(data), "test-cluster") {
		t.Errorf("the export should record the cluster:\n%s", data)
	}
}

func TestExportSelectedWritesOneRecord(t *testing.T) {
	m, _ := withRecords(t)
	dir := t.TempDir()
	m.SetContext("c", dir)

	if cmd := m.Update(key("e")); cmd != nil {
		cmd()
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected one file, got %d", len(entries))
	}
	data, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))

	var export kafka.Export
	if err := json.Unmarshal(data, &export); err != nil {
		t.Fatal(err)
	}
	if export.Count != 1 {
		t.Errorf("exported %d records, want just the selected one", export.Count)
	}
}

func TestExportWithNothingToExport(t *testing.T) {
	m, _ := newTab(t)
	m.SetContext("c", t.TempDir())

	cmd := m.Update(key("ctrl+e"))
	if cmd == nil {
		t.Fatal("the refusal should be explained")
	}
	if notification, ok := cmd().(msg.Notify); !ok || notification.Level != msg.NotifyWarning {
		t.Errorf("got %#v, want a warning", cmd())
	}
}

// ----------------------------------------------------------------- sorting ---

func TestOrderBySortsTheBuffer(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("space"))
	m.query.SetText("order by offset desc")
	if cmd := m.Update(key("enter")); cmd != nil {
		cmd()
	}

	m.records.Append(kafka.MockRecords([]string{"orders.events"}))

	records := m.records.Records()
	for i := 1; i < len(records); i++ {
		if records[i-1].Offset < records[i].Offset {
			t.Errorf("records are not in descending offset order: %d before %d",
				records[i-1].Offset, records[i].Offset)
		}
	}
}

func TestSortingSwitchesFollowOff(t *testing.T) {
	m, _ := withRecords(t)

	m.records.Sort("offset", true)
	if m.records.Following() {
		t.Error("a sorted view has no newest row to chase, so follow must switch off")
	}
}

func TestNewRecordsKeepTheSortOrder(t *testing.T) {
	m, _ := newTab(t)
	m.records.Sort("offset", false)
	m.records.Append([]kafka.Record{{Topic: "t", Offset: 500}})
	m.records.Append([]kafka.Record{{Topic: "t", Offset: 100}})

	records := m.records.Records()
	if len(records) != 2 || records[0].Offset != 100 {
		t.Errorf("a record arriving out of order should be sorted into place, got %v",
			[]int64{records[0].Offset, records[1].Offset})
	}
}
