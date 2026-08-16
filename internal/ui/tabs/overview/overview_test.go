package overview

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// loaded builds a tab that has finished loading from the mock.
func loaded(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	mock := kafka.NewMock(nil)
	m := New()
	m.SetClient(mock)

	info, _ := mock.DescribeCluster(context.Background())
	m.Update(msg.Connected{Client: mock, Info: info})

	health, _ := mock.ClusterHealth(context.Background())
	m.Update(msg.HealthLoaded{Health: health})

	topics, _ := mock.ListTopics(context.Background(), false)
	m.Update(msg.TopicsLoaded{Topics: topics})

	topicSizes, _ := mock.TopicSizes(context.Background())
	brokerSizes, _ := mock.BrokerSizes(context.Background())
	m.Update(msg.SizesLoaded{Topics: topicSizes, Brokers: brokerSizes})

	groups, _ := mock.ConsumerGroupsWithLag(context.Background())
	m.Update(msg.GroupsLoaded{Groups: groups})

	quorum, err := mock.DescribeQuorum(context.Background())
	m.Update(quorumLoadedMsg{Quorum: quorum, Err: err})

	return m, mock
}

func render(t *testing.T, m *Model) string {
	t.Helper()
	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k := ktx.New(cfg, theme.New(theme.Dark, false), 120, 32)
	return ansi.Strip(m.View(k))
}

func TestPanelsShowLoadedData(t *testing.T) {
	m, _ := loaded(t)
	view := render(t, m)

	want := []string{
		"kafka-2:9092",   // brokers panel
		"controller",     // the controller is marked
		"orders.events",  // topic table
		"billing-worker", // group table
		"In sync",        // health panel
		"53 / 54",        // in-sync partitions from the mock
	}
	for _, s := range want {
		if !strings.Contains(view, s) {
			t.Errorf("the Overview should show %q:\n%s", s, view)
		}
	}
}

func TestHealthShowsTheWorstPartition(t *testing.T) {
	m, _ := loaded(t)
	view := render(t, m)

	// A count alone leaves the user nowhere to start, so the worst offender is
	// named.
	if !strings.Contains(view, "worst:") {
		t.Errorf("a degraded cluster should name its worst partition:\n%s", view)
	}
	if !strings.Contains(view, "audit.log p7") {
		t.Errorf("the worst partition should be identified:\n%s", view)
	}
	if !strings.Contains(view, "ISR 2/3") {
		t.Errorf("the worst partition's ISR should be shown:\n%s", view)
	}
}

func TestHealthyClusterSaysSo(t *testing.T) {
	mock := kafka.NewMock(nil)
	mock.ClusterHealthFunc = func(context.Context) (kafka.Health, error) {
		return kafka.Health{Topics: 3, Partitions: 30, InSync: 30, TotalReplicas: 90}, nil
	}

	m := New()
	m.SetClient(mock)
	health, _ := mock.ClusterHealth(context.Background())
	m.Update(msg.HealthLoaded{Health: health})

	view := render(t, m)
	if !strings.Contains(view, "every partition is fully replicated") {
		t.Errorf("a healthy cluster should say so plainly:\n%s", view)
	}
	if strings.Contains(view, "worst:") {
		t.Errorf("a healthy cluster has no worst partition:\n%s", view)
	}
}

func TestTopicsSortedBySize(t *testing.T) {
	m, _ := loaded(t)

	rows := m.topicTable.Rows()
	if len(rows) == 0 {
		t.Fatal("no topic rows")
	}
	// audit.log is the largest in the mock, so it leads.
	if rows[0].Cells[0] != "audit.log" {
		t.Errorf("first row = %q, want the largest topic audit.log", rows[0].Cells[0])
	}
}

func TestGroupsSortedByLag(t *testing.T) {
	m, _ := loaded(t)

	rows := m.groupTable.Rows()
	if len(rows) == 0 {
		t.Fatal("no group rows")
	}
	if rows[0].Cells[0] != "billing-worker" {
		t.Errorf("first row = %q, want the group with the most lag", rows[0].Cells[0])
	}
	// 38104 rendered with the underscore separator kafman uses everywhere.
	if got := rows[0].Cells[3]; got != "38_104" {
		t.Errorf("lag = %q, want 38_104", got)
	}
}

func TestPanelFailureIsVisible(t *testing.T) {
	m := New()
	m.SetClient(kafka.NewMock(nil))
	m.Update(msg.GroupsLoaded{Err: errors.New("permission denied")})

	view := render(t, m)
	if !strings.Contains(view, "failed") {
		t.Errorf("a failed panel should say so rather than look empty:\n%s", view)
	}
}

func TestOnePanelFailingLeavesTheOthers(t *testing.T) {
	m, mock := loaded(t)

	// Sizes are the most likely to be refused: DescribeLogDirs needs cluster
	// permissions the other calls do not.
	m.Update(msg.SizesLoaded{Err: errors.New("cluster authorization failed")})

	view := render(t, m)
	if !strings.Contains(view, "orders.events") {
		t.Errorf("the topic list should survive a size failure:\n%s", view)
	}
	if !strings.Contains(view, "billing-worker") {
		t.Errorf("the group list should survive a size failure:\n%s", view)
	}
	_ = mock
}

func TestDisconnectClearsTheData(t *testing.T) {
	m, _ := loaded(t)
	if !strings.Contains(render(t, m), "orders.events") {
		t.Fatal("setup: the topic should be visible before disconnecting")
	}

	// Stale numbers presented as current are worse than no numbers.
	m.SetClient(nil)

	view := render(t, m)
	if strings.Contains(view, "orders.events") {
		t.Errorf("data from a closed connection must not stay on screen:\n%s", view)
	}
	if !strings.Contains(view, "not connected") {
		t.Errorf("the tab should say it is not connected:\n%s", view)
	}
}

func TestEnterOnATopicAsksToOpenIt(t *testing.T) {
	m, _ := loaded(t)

	cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("Enter on a topic row should lead somewhere")
	}
	focus, ok := cmd().(msg.FocusTopic)
	if !ok {
		t.Fatalf("got %T, want msg.FocusTopic", cmd())
	}
	if focus.Topic != "audit.log" {
		t.Errorf("focus on %q, want the selected topic", focus.Topic)
	}
}

func TestEnterOnAGroupAsksToOpenIt(t *testing.T) {
	m, _ := loaded(t)

	m.Update(tea.KeyMsg{Type: tea.KeyTab}) // move focus to the groups panel

	cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("Enter on a group row should lead somewhere")
	}
	focus, ok := cmd().(msg.FocusGroup)
	if !ok {
		t.Fatalf("got %T, want msg.FocusGroup", cmd())
	}
	if focus.Group != "billing-worker" {
		t.Errorf("focus on %q, want the selected group", focus.Group)
	}
}

func TestFocusCyclesBetweenPanels(t *testing.T) {
	m, _ := loaded(t)

	if !m.topicTable.Focused() {
		t.Error("the topic table should start focused")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if !m.groupTable.Focused() || m.topicTable.Focused() {
		t.Error("Tab should move focus to the group table")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if !m.topicTable.Focused() {
		t.Error("Tab should cycle back to the topic table")
	}
}

func TestInitLoadsEveryPanel(t *testing.T) {
	mock := kafka.NewMock(nil)
	m := New()
	m.SetClient(mock)

	cmd := m.Init()
	if cmd == nil {
		t.Fatal("Init should start loading")
	}
	// Run the batch so the mock records what was asked for.
	if batch, ok := cmd().(tea.BatchMsg); ok {
		for _, c := range batch {
			c()
		}
	}

	for _, want := range []string{"ClusterHealth", "ListTopics", "TopicSizes", "ConsumerGroupsWithLag"} {
		if !mock.Called(want) {
			t.Errorf("Init should have called %s; calls: %v", want, mock.Calls())
		}
	}
}

func TestInitWithoutAClientIsSafe(t *testing.T) {
	m := New()
	if cmd := m.Init(); cmd != nil {
		t.Error("without a client there is nothing to load")
	}
	render(t, m) // must not panic
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:              "0 B",
		512:            "512 B",
		1024:           "1.0 KB",
		1_048_576:      "1.0 MB",
		40_912_000_000: "38.1 GB",
	}
	for input, want := range cases {
		if got := humanBytes(input); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestHumanCount(t *testing.T) {
	cases := map[int64]string{
		0:       "0",
		999:     "999",
		1000:    "1000", // four digits stay unbroken: 1000 reads fine
		10_000:  "10_000",
		41_882:  "41_882",
		1000000: "1_000_000",
		-5000:   "-5000",
	}
	for input, want := range cases {
		if got := humanCount(input); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", input, got, want)
		}
	}
}

// A panel that fails has to say why. An empty grid under a border reading
// "failed" is what someone screenshots when asking what went wrong.
func TestFailedPanelsShowTheReason(t *testing.T) {
	m, _ := loaded(t)

	m.Update(msg.GroupsLoaded{Err: errors.New("kafka server: The group coordinator is not available")})
	m.Update(msg.TopicsLoaded{Err: errors.New("kafka server: insufficient permissions")})

	view := render(t, m)
	if !strings.Contains(view, "coordinator") {
		t.Errorf("the groups panel should carry its reason:\n%s", view)
	}
	if !strings.Contains(view, "permissions") {
		t.Errorf("the topics panel should carry its reason:\n%s", view)
	}
	// And the border still marks the panel as failed.
	if !strings.Contains(view, "failed") {
		t.Error("a failed panel should be marked in its border too")
	}
}

// A cluster with no consumer groups is not a failure, and must not read like one.
func TestEmptyGroupsSayThatTheyAreEmpty(t *testing.T) {
	m, _ := loaded(t)

	m.Update(msg.GroupsLoaded{Groups: nil})

	view := render(t, m)
	if !strings.Contains(view, "no consumer groups") {
		t.Errorf("an empty cluster should say so:\n%s", view)
	}
	if strings.Contains(view, "failed") {
		t.Errorf("an empty list is not a failure:\n%s", view)
	}
}

// The ROLE column shows what each node actually is — its process.roles on
// KRaft — never the metadata "controller id", which there is a random broker.
func TestBrokerRolesAreShown(t *testing.T) {
	m, _ := loaded(t)
	view := render(t, m)

	if !strings.Contains(view, "broker+controller") {
		t.Errorf("the combined node should show both roles:\n%s", view)
	}
	for _, line := range strings.Split(view, "\n") {
		if !strings.Contains(line, "kafka-1:9092") {
			continue
		}
		if !strings.Contains(line, "broker") || strings.Contains(line, "controller") {
			t.Errorf("a plain broker should show just its role:\n%s", line)
		}
		return
	}
	t.Fatalf("broker row not rendered:\n%s", view)
}

// The Cluster panel names the real KRaft quorum leader — the one thing the
// metadata "controller id" lies about — and the Brokers table places every
// node in the quorum.
func TestQuorumIsShown(t *testing.T) {
	m, _ := loaded(t)
	view := render(t, m)

	if !strings.Contains(view, "leader node 2 · epoch 5 · HW 28_400") {
		t.Errorf("the Cluster panel should carry the quorum line:\n%s", view)
	}
	for _, want := range []string{"★ leader", "observer (lag 14)"} {
		if !strings.Contains(view, want) {
			t.Errorf("the Brokers table should show %q:\n%s", want, view)
		}
	}
}

// Without a quorum (a ZooKeeper cluster) the panel keeps the classic
// controller line and the QUORUM column shows dashes, not claims.
func TestNoQuorumFallsBackToController(t *testing.T) {
	m, _ := loaded(t)
	m.Update(quorumLoadedMsg{Err: kafka.ErrNoQuorum})
	view := render(t, m)

	if strings.Contains(view, "leader node") {
		t.Errorf("no quorum may be claimed without one:\n%s", view)
	}
	if !strings.Contains(view, "Controller") {
		t.Errorf("the controller line should be back:\n%s", view)
	}
}

// Every quorum label pads to the same display width: the ★ is a multi-byte
// rune, and padding by len() used to shift the leader's ROLE column left.
func TestQuorumLabelsAlign(t *testing.T) {
	m, _ := loaded(t)
	th := theme.New(theme.Dark, false)

	want := ansi.StringWidth(ansi.Strip(m.quorumLabel(th, 1)))
	for _, id := range []int32{2, 3, 9} { // leader, lagging observer, unknown
		if got := ansi.StringWidth(ansi.Strip(m.quorumLabel(th, id))); got != want {
			t.Errorf("node %d: label width %d, want %d", id, got, want)
		}
	}
}
