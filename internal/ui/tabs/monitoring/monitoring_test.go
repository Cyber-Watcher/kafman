package monitoring

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// growingMock is a cluster whose topics keep receiving records and whose group
// falls further behind on every poll, which is what a dashboard has to show.
func growingMock(step int64) *kafka.Mock {
	mock := kafka.NewMock(nil)

	var polls atomic.Int64
	mock.TopicEndOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		n := polls.Add(1)
		return map[string]int64{
			"orders.events": 1000 + step*n,
			"payments.tx":   500 + n, // one record per poll, a much quieter topic
		}, nil
	}

	var lagPolls atomic.Int64
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		n := lagPolls.Add(1)
		return []kafka.ConsumerGroup{
			{Name: "billing-worker", State: kafka.GroupStable, Members: 3,
				TotalLag: 1000 * n, LagKnown: true,
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

// newTab wires a tab to a mock and polls it n times, the way the event loop
// would: every command is executed and its message fed back.
func newTab(t *testing.T, mock *kafka.Mock, polls int) *Model {
	t.Helper()

	m := New(5 * time.Millisecond)
	m.SetClient(mock)
	t.Cleanup(m.Close)

	cmd := m.Init()
	for range polls {
		if cmd == nil {
			t.Fatal("the tab stopped waiting for updates")
		}
		message := cmd()
		if message == nil {
			t.Fatal("the collector closed its channel")
		}
		cmd = m.Update(message)
	}
	return m
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

// The first poll only takes the baseline every rate is measured against, so the
// tab has to say it is waiting rather than draw a chart of zeroes.
func TestFirstPollOnlyEstablishesTheBaseline(t *testing.T) {
	m := newTab(t, growingMock(600), 1)

	view := render(t, m)
	if !strings.Contains(view, "measuring") && !strings.Contains(view, "waiting") {
		t.Errorf("with one poll the tab should say it is still measuring:\n%s", view)
	}
	if len(m.view.Cluster) != 0 {
		t.Errorf("a rate cannot exist after one reading, got %v", m.view.Cluster)
	}
}

func TestRatesAppearAfterTwoPolls(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	if len(m.view.Cluster) == 0 {
		t.Fatal("two readings are enough for a rate")
	}
	if m.view.Cluster[len(m.view.Cluster)-1].Value <= 0 {
		t.Errorf("the cluster rate should be positive, got %v", m.view.Cluster)
	}

	view := render(t, m)
	for _, want := range []string{"Throughput", "orders.events", "payments.tx", "Consumer lag"} {
		if !strings.Contains(view, want) {
			t.Errorf("the dashboard should show %q:\n%s", want, view)
		}
	}
}

// The busiest topic belongs at the top: the panel is there to answer "what is
// this cluster doing", not "what topics exist".
func TestTopicsAreOrderedByRate(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	if len(m.view.Topics) < 2 {
		t.Fatalf("both topics should have series, got %d", len(m.view.Topics))
	}
	if m.view.Topics[0].Name != "orders.events" {
		t.Errorf("the busiest topic should come first, got %q", m.view.Topics[0].Name)
	}
	if m.view.Topics[0].Last <= m.view.Topics[1].Last {
		t.Errorf("rates are not ordered: %v", m.view.Topics)
	}
}

func TestGrowingLagIsMarked(t *testing.T) {
	m := newTab(t, growingMock(600), 5)

	var billing *struct {
		trend string
		last  float64
	}
	for _, group := range m.view.Groups {
		if group.Name == "billing-worker" {
			billing = &struct {
				trend string
				last  float64
			}{group.Trend.String(), group.Last}
		}
	}
	if billing == nil {
		t.Fatal("the group with lag should be in the view")
	}
	if billing.trend != "growing" {
		t.Errorf("lag that rises on every poll is growing, got %q", billing.trend)
	}

	view := render(t, m)
	if !strings.Contains(view, "growing") {
		t.Errorf("the trend should be named on screen:\n%s", view)
	}
	if !strings.Contains(view, "idle") {
		t.Errorf("a group with no lag at all should read as idle:\n%s", view)
	}
}

// Disk growth is a sum over every replica and includes compaction: it is not
// what producers sent, and must never be shown as though it were.
func TestDiskRateIsMarkedAsAnEstimate(t *testing.T) {
	mock := growingMock(600)
	var polls atomic.Int64
	mock.BrokerSizesFunc = func(context.Context) (map[int32]int64, error) {
		n := polls.Add(1)
		return map[int32]int64{1: 1_000_000 * n, 2: 900_000 * n, 3: 800_000 * n}, nil
	}

	m := newTab(t, mock, 3)

	view := render(t, m)
	if !strings.Contains(view, "≈") {
		t.Errorf("an estimated byte rate must carry its ≈:\n%s", view)
	}
}

// Without a JMX exporter the heap and handler gauges cannot be filled in, and
// the panel says so instead of leaving an unexplained gap.
func TestMissingJMXIsStated(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	view := render(t, m)
	if !strings.Contains(view, "JMX") {
		t.Errorf("the brokers panel should say what needs JMX:\n%s", view)
	}
}

func TestBrokersPanelShowsTheController(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	view := render(t, m)
	if !strings.Contains(view, "LEADERS") || !strings.Contains(view, "ON DISK") {
		t.Errorf("the brokers panel should show the load columns:\n%s", view)
	}
	if !strings.Contains(view, "◆") {
		t.Errorf("the controller should be marked:\n%s", view)
	}
}

func TestWindowKeysCycleTheSpan(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	if got := windows[m.window]; got != 5*time.Minute {
		t.Fatalf("the tab should start on 5m, got %s", got)
	}

	m.Update(key("+"))
	if got := windows[m.window]; got != 15*time.Minute {
		t.Errorf("+ should widen the window, got %s", got)
	}
	m.Update(key("-"))
	m.Update(key("-"))
	if got := windows[m.window]; got != time.Minute {
		t.Errorf("- should narrow it, got %s", got)
	}

	// The ends are ends, not wrap-arounds: the window is a range, and jumping
	// from 1m to an hour with one keypress would be a surprise.
	m.Update(key("-"))
	if got := windows[m.window]; got != time.Minute {
		t.Errorf("the narrowest window should stay, got %s", got)
	}
	if !strings.Contains(render(t, m), "1m") {
		t.Error("the current window should be visible")
	}
}

func TestTabMovesBetweenPanels(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	if m.focus != focusTopics || m.selectorFocused {
		t.Fatal("the topics panel starts focused")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.focus != focusGroups {
		t.Error("Tab should move to the groups panel")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if !m.selectorFocused {
		t.Error("Tab should move on to the section list")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.selectorFocused || m.focus != focusTopics {
		t.Error("Tab should cycle back to the topics panel")
	}

	// And the same ring backwards.
	m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
	if !m.selectorFocused {
		t.Error("Shift+Tab should go back to the section list")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
	if m.selectorFocused || m.focus != focusGroups {
		t.Error("Shift+Tab should go back to the groups panel")
	}
}

// openSection walks the selector with keys, the way a user would.
func openSection(m *Model, target section) {
	for !m.selectorFocused {
		m.Update(tea.KeyMsg{Type: tea.KeyTab})
	}
	for m.section != target {
		m.Update(key("j"))
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
}

func TestSelectorNavigation(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if !m.selectorFocused {
		t.Fatal("two Tabs should reach the section list")
	}

	m.Update(key("j"))
	if m.section != sectionGroups {
		t.Errorf("j should move down to Groups, got %v", m.section)
	}
	m.Update(key("j"))
	if m.section != sectionJMX {
		t.Errorf("j again should reach JMX, got %v", m.section)
	}
	m.Update(key("j"))
	if m.section != sectionOS {
		t.Errorf("one more j should reach OS, got %v", m.section)
	}
	m.Update(key("j"))
	if m.section != sectionOverview {
		t.Errorf("the list should wrap, got %v", m.section)
	}
	m.Update(key("k"))
	if m.section != sectionOS {
		t.Errorf("k should wrap back to OS, got %v", m.section)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if m.selectorFocused {
		t.Error("Enter should drop focus into the section")
	}

	// The selector marks the current section.
	view := render(t, m)
	if !strings.Contains(view, "▸ OS") {
		t.Errorf("the selector should mark the open section:\n%s", view)
	}
}

func TestShortcutsFollowTheSection(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	labels := func() string {
		var b strings.Builder
		for _, s := range m.Shortcuts() {
			b.WriteString(s.Key + " " + s.Label + " ")
		}
		return b.String()
	}

	if got := labels(); !strings.Contains(got, "Enter") {
		t.Errorf("Overview advertises the Enter jump, got %q", got)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if got := labels(); !strings.Contains(got, "Section") {
		t.Errorf("the selector advertises its own keys, got %q", got)
	}

	m.Update(key("j")) // Groups
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if got := labels(); !strings.Contains(got, "Open in Manage") {
		t.Errorf("the Groups section advertises the Enter jump, got %q", got)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyTab}) // back to the selector
	m.Update(key("j"))                     // JMX
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if got := labels(); strings.Contains(got, "Enter") || !strings.Contains(got, "t ") {
		t.Errorf("the JMX section has no Enter jump but keeps t, got %q", got)
	}
}

// Losing the connection stops the poller: it holds a client that is about to be
// closed, and its numbers describe a cluster no longer being watched.
func TestDisconnectStopsTheCollector(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	m.SetClient(nil)
	if m.collector != nil {
		t.Error("the collector should be gone with the connection")
	}
	if len(m.view.Cluster) != 0 {
		t.Error("stale rates should not stay on screen")
	}
	if !strings.Contains(render(t, m), "not connected") {
		t.Error("a disconnected tab should say so")
	}
}

func TestFormatRate(t *testing.T) {
	tests := []struct {
		rate float64
		want string
	}{
		{0, "0"},
		{2.5, "2.5"},
		{42, "42"},
		{2910, "2.9k"},
		{1_500_000, "1.5M"},
	}
	for _, test := range tests {
		if got := formatRate(test.rate); got != test.want {
			t.Errorf("formatRate(%v) = %q, want %q", test.rate, got, test.want)
		}
	}
}

func key(s string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

// The brokers panel is drawn by hand rather than by the table component, so the
// invariant it could break — a row wider than its panel — is checked directly,
// at the sizes a terminal actually offers.
func TestBrokersPanelFitsItsWidth(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, width := range []int{30, 40, 55, 63, 80, 120} {
		k := ktx.New(cfg, theme.New(theme.Dark, false), 130, 30)
		for _, line := range strings.Split(m.brokersBody(k, width), "\n") {
			if got := ansi.StringWidth(line); got > width-1 {
				t.Errorf("at width %d a line is %d wide: %q", width, got, ansi.Strip(line))
			}
		}
	}
}

// The whole frame has the same invariant, including the smallest terminal the
// app supports.
func TestFrameFitsSmallTerminals(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, size := range [][2]int{{61, 16}, {80, 24}, {130, 30}, {200, 60}} {
		k := ktx.New(cfg, theme.New(theme.Dark, false), size[0], size[1])
		for i, line := range strings.Split(m.View(k), "\n") {
			// Display width, not byte length: every block character here is
			// multi-byte and a byte count would fail on a frame that fits.
			if got := ansi.StringWidth(line); got > size[0] {
				t.Errorf("at %dx%d line %d is %d wide: %q",
					size[0], size[1], i, got, ansi.Strip(line))
			}
		}
	}
}

// The JMX panel is drawn by hand like the brokers panel, and has the same
// invariant to break.
func TestJMXPanelFitsItsWidth(t *testing.T) {
	m := newTab(t, growingMock(600), 2)
	m.view.JMXConfigured = true
	m.view.JMX = []metrics.JMXStat{
		{BrokerJMX: jmxReading(1, 0.62, 0.92, 1_600_000)},
		{BrokerJMX: jmxReading(2, 0.55, 0.10, 1_500_000)},
	}

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, width := range []int{30, 40, 55, 63, 80, 120} {
		k := ktx.New(cfg, theme.New(theme.Dark, false), 130, 30)
		for _, line := range strings.Split(m.jmxBody(k, width), "\n") {
			if got := ansi.StringWidth(line); got > width-1 {
				t.Errorf("at width %d a line is %d wide: %q", width, got, ansi.Strip(line))
			}
		}
	}
}

// jmxReading builds one broker's exporter reading for the tests.
func jmxReading(id int32, heap, idle, bytesIn float64) metrics.BrokerJMX {
	var samples []metrics.Sample
	add := func(name string, value float64) {
		samples = append(samples, metrics.Sample{Name: name, Value: value})
	}
	add("jvm_memory_heap_max_bytes", 8<<30)
	add("jvm_memory_heap_used_bytes", heap*float64(int64(8)<<30))
	add("kafka_server_requesthandlerpool_requesthandleravgidlepercent_oneminuterate", idle)
	add("kafka_network_socketserver_networkprocessoravgidlepercent", idle)
	add("kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate", bytesIn)
	add("kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate", bytesIn*2)
	add("jvm_gc_collection_seconds_count", 200)
	add("jvm_gc_collection_seconds_sum", 3.6)

	reading := metrics.BrokerJMX{BrokerID: id, URL: "http://broker/metrics", At: time.Now()}
	metrics.Select(&reading, samples)
	return reading
}

// ---------------------------------------------------------------------- JMX ---

// exporterBody is what a jmx_exporter serves, in the names docker/jmx-exporter.yml
// produces.
func exporterBody(heapUsed, idle, bytesIn float64) string {
	return strings.Join([]string{
		"# TYPE jvm_memory_heap_used_bytes gauge",
		fmt.Sprintf("jvm_memory_heap_used_bytes %g", heapUsed),
		"jvm_memory_heap_max_bytes 8.589934592E9",
		fmt.Sprintf("kafka_server_requesthandlerpool_requesthandleravgidlepercent_oneminuterate %g", idle),
		fmt.Sprintf("kafka_network_socketserver_networkprocessoravgidlepercent %g", idle),
		fmt.Sprintf("kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate %g", bytesIn),
		fmt.Sprintf("kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate %g", bytesIn*2),
		"kafka_server_replicamanager_underreplicatedpartitions_value 0.0",
		`jvm_gc_collection_seconds_count{gc="G1 Young Generation"} 200`,
		`jvm_gc_collection_seconds_sum{gc="G1 Young Generation"} 3.6`,
		"",
	}, "\n")
}

// jmxTab wires a tab to a mock cluster whose brokers expose the given exporter
// URLs.
func jmxTab(t *testing.T, polls int, urls ...string) *Model {
	t.Helper()

	cluster := &config.Cluster{Name: "with-jmx", Brokers: []string{"localhost:9092"}}
	cluster.JMX.Enabled = true
	for i, url := range urls {
		cluster.JMX.Targets = append(cluster.JMX.Targets,
			config.JMXTarget{BrokerID: int32(i + 1), URL: url})
	}

	mock := growingMock(600)
	mock.ClusterFunc = func() *config.Cluster { return cluster }
	return newTab(t, mock, polls)
}

func TestJMXPanelShowsWhatTheExporterReported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exporterBody(5.15e9, 0.92, 1_600_000)))
	}))
	defer server.Close()

	m := jmxTab(t, 2, server.URL)

	openSection(m, sectionJMX)
	view := render(t, m)
	for _, want := range []string{"JMX", "HEAP", "IDLE", "NET IDLE", "OUT", "60%", "92%", "scraped"} {
		if !strings.Contains(view, want) {
			t.Errorf("the JMX section should show %q:\n%s", want, view)
		}
	}
}

func TestSelectorOpensJMXSection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exporterBody(5.15e9, 0.92, 1_600_000)))
	}))
	defer server.Close()

	m := jmxTab(t, 2, server.URL)

	// The tab opens on Overview with the Brokers panel — JMX no longer shares
	// its slot.
	if view := render(t, m); !strings.Contains(view, "LEADERS") {
		t.Errorf("Overview should show the brokers panel:\n%s", view)
	}

	openSection(m, sectionJMX)
	if view := render(t, m); !strings.Contains(view, "HEAP") {
		t.Errorf("the JMX section should show the exporter table:\n%s", view)
	}

	// The chart toggles between heap and bytes-in with t.
	if view := render(t, m); !strings.Contains(view, "Heap  %") {
		t.Errorf("the JMX chart starts on heap:\n%s", view)
	}
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "Bytes in") {
		t.Errorf("t should switch the chart to bytes in:\n%s", view)
	}

	openSection(m, sectionOverview)
	if view := render(t, m); !strings.Contains(view, "LEADERS") {
		t.Errorf("the way back to Overview should work:\n%s", view)
	}
}

// An unreachable exporter keeps its row: the broker is still there, and "its
// exporter is down" is worth seeing.
func TestUnreachableExporterKeepsItsRow(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exporterBody(5.15e9, 0.92, 1_600_000)))
	}))
	defer good.Close()

	m := jmxTab(t, 2, good.URL, "http://127.0.0.1:1/metrics")

	openSection(m, sectionJMX)
	view := render(t, m)
	if !strings.Contains(view, "unreachable") {
		t.Errorf("a failed scrape should say so:\n%s", view)
	}
	// And the working broker is unaffected.
	if !strings.Contains(view, "92%") {
		t.Errorf("the healthy exporter's numbers should still be there:\n%s", view)
	}
}

// Without an exporter nothing is invented: the Brokers panel names what is
// missing, and the JMX section points at the config instead of showing zeroes.
func TestWithoutJMXNothingIsInvented(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	view := render(t, m)
	if !strings.Contains(view, "need a JMX exporter") {
		t.Errorf("the brokers panel should say what needs an exporter:\n%s", view)
	}

	openSection(m, sectionJMX)
	view = render(t, m)
	if !strings.Contains(view, "no JMX exporter configured") {
		t.Errorf("the JMX section should point at the config:\n%s", view)
	}
	if strings.Contains(view, "0%") {
		t.Errorf("an unconfigured exporter must not produce numbers:\n%s", view)
	}
}

// Without node_exporter the OS section points at the config the same way.
func TestWithoutNodeExporterNothingIsInvented(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	openSection(m, sectionOS)
	view := render(t, m)
	if !strings.Contains(view, "node_exporter not configured") {
		t.Errorf("the OS section should point at the config:\n%s", view)
	}
}

// A saturating idle meter must not read as 200% idle.
func TestIdleAboveOneIsSaturated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The value the test stack's brokers actually report.
		_, _ = w.Write([]byte(exporterBody(5.15e9, 2.0007, 0)))
	}))
	defer server.Close()

	m := jmxTab(t, 2, server.URL)

	openSection(m, sectionJMX)
	view := render(t, m)
	if strings.Contains(view, "200%") {
		t.Errorf("an idle ratio above 1 should saturate:\n%s", view)
	}
	if !strings.Contains(view, "100%") {
		t.Errorf("it should read as fully idle:\n%s", view)
	}
}

// The three panels want 24 rows and a terminal does not always have them; the
// frame must fit whatever it is given rather than pushing the status bar off
// the screen.
func TestPanelHeightsAlwaysFit(t *testing.T) {
	for available := 4; available <= 60; available++ {
		chart, rates, groups := panelHeights(available)

		if chart < 0 || rates < 0 || groups < 0 {
			t.Fatalf("at %d rows: negative heights %d/%d/%d", available, chart, rates, groups)
		}
		if total := chart + rates + groups; total != available {
			t.Errorf("at %d rows the panels use %d", available, total)
		}
		// A panel too short to hold anything is worse than no panel: it renders a
		// border around nothing.
		for name, height := range map[string]int{"rates": rates, "groups": groups} {
			if height > 0 && height < 4 {
				t.Errorf("at %d rows the %s panel is only %d tall", available, name, height)
			}
		}
	}
}

// And the same, measured on the rendered frame.
func TestFrameFitsEveryHeight(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for height := 6; height <= 40; height++ {
		k := ktx.New(cfg, theme.New(theme.Dark, false), 100, height+4)
		k.AvailableHeight = height

		lines := strings.Split(m.View(k), "\n")
		if len(lines) != height {
			t.Errorf("at %d available rows the tab drew %d lines", height, len(lines))
		}
	}
}

// `t` cycles the throughput chart: cluster total → one line per topic →
// retention deletes → back, each with a legend naming the lines.
func TestThroughputChartTogglesToTopics(t *testing.T) {
	m := newTab(t, growingMock(600), 3)
	key := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("t")}

	m.Update(key)
	view := render(t, m)
	if !strings.Contains(view, "Throughput by topic") {
		t.Errorf("t should switch the chart to topics:\n%s", view)
	}
	// The legend names the plotted topics under the chart.
	if !strings.Contains(view, "orders.events") || !strings.Contains(view, "payments.tx") {
		t.Errorf("the legend should name the plotted topics:\n%s", view)
	}

	m.Update(key)
	if view := render(t, m); !strings.Contains(view, "Retention deletes by topic") {
		t.Errorf("t again should switch to the retention deletes:\n%s", view)
	}

	m.Update(key)
	if view := render(t, m); !strings.Contains(view, "Throughput  msg/s") {
		t.Errorf("the cycle should wrap back to the cluster total:\n%s", view)
	}
}

// The retained record count sits next to the rate, and reads as a dash until
// the oldest offsets have been fetched.
func TestRetainedColumnAndDash(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	view := render(t, m)
	if !strings.Contains(view, "RETAINED") {
		t.Errorf("the topics table should have a RETAINED column:\n%s", view)
	}
	// growingMock: orders.events end = 1000+600n, oldest = 100n.
	if !strings.Contains(view, "2500") {
		t.Errorf("retained = end - oldest should be shown:\n%s", view)
	}

	// Without start offsets the column holds a dash, not a zero.
	mock := growingMock(600)
	mock.TopicStartOffsetsFunc = func(context.Context, bool) (map[string]int64, error) {
		return nil, errors.New("not authorised")
	}
	m = newTab(t, mock, 2)
	if view := render(t, m); !strings.Contains(view, "—") {
		t.Errorf("an unknown retained count must read as a dash:\n%s", view)
	}
}

// The replication trend line sits in the Brokers panel and warns only when a
// count is non-zero.
func TestReplicationTrendLineInBrokersPanel(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	view := render(t, m)
	if !strings.Contains(view, "replication: URP") {
		t.Errorf("the brokers panel should carry the URP trend:\n%s", view)
	}
	if !strings.Contains(view, "not-preferred") {
		t.Errorf("the brokers panel should carry the not-preferred trend:\n%s", view)
	}
}

// The charts draw with braille dots unless plain fonts are asked for.
func TestChartsAreBrailleUnlessPlain(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	braille := func(s string) bool {
		for _, r := range s {
			if r >= 0x2800 && r <= 0x28FF {
				return true
			}
		}
		return false
	}

	if view := render(t, m); !braille(view) {
		t.Error("the dashboard should draw with braille dots")
	}

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k := ktx.New(cfg, theme.New(theme.Dark, true), 130, 30)
	if view := ansi.Strip(m.View(k)); braille(view) {
		t.Error("plain fonts must not use braille")
	}
}

// Every row's sparkline gets its own series colour, so neighbouring topics can
// be told apart — the btop look.
func TestSparklinesGetSeriesColours(t *testing.T) {
	m := newTab(t, growingMock(600), 3)
	render(t, m) // the theme reaches the tab through View

	rows := m.topicTable.Rows()
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 topic rows, got %d", len(rows))
	}
	for i, row := range rows {
		if len(row.CellStyles) == 0 || row.CellStyles[3] == nil {
			t.Fatalf("row %d: the sparkline cell should carry a series style", i)
		}
	}
	first := rows[0].CellStyles[3].GetForeground()
	second := rows[1].CellStyles[3].GetForeground()
	if first == second {
		t.Error("neighbouring rows should not share a colour")
	}
}

// Enter is a launchpad: a topic opens in Messages, a group in Manage.
func TestEnterJumpsToTopicAndGroup(t *testing.T) {
	m := newTab(t, growingMock(600), 3)
	enter := tea.KeyMsg{Type: tea.KeyEnter}

	cmd := m.Update(enter)
	if cmd == nil {
		t.Fatal("enter on a topic should produce a jump")
	}
	if focus, ok := cmd().(msg.FocusTopic); !ok || focus.Topic == "" {
		t.Fatalf("expected a FocusTopic message, got %T", cmd())
	}

	m.Update(tea.KeyMsg{Type: tea.KeyTab}) // over to the lag panel
	cmd = m.Update(enter)
	if cmd == nil {
		t.Fatal("enter on a group should produce a jump")
	}
	if focus, ok := cmd().(msg.FocusGroup); !ok || focus.Group == "" {
		t.Fatalf("expected a FocusGroup message, got %T", cmd())
	}
}
