//go:build integration

package ui

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

func devConfig(t *testing.T) *config.Config {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "..", "docker", "kafman.dev.yaml")
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load dev config: %v", err)
	}
	return cfg
}

// nextPoll waits for the collector's next reading and applies it, which is what
// the event loop does with the command the tab keeps in flight.
func nextPoll(t *testing.T, m *Model, limit time.Duration) {
	t.Helper()

	done := make(chan tea.Msg, 1)
	go func() { done <- m.monitoringTab.AwaitUpdate()() }()

	select {
	case message := <-done:
		if message == nil {
			t.Fatal("the collector closed its channel")
		}
		m.monitoringTab.Update(message)
	case <-time.After(limit):
		t.Fatalf("no poll within %s", limit)
	}
}

// openMonitoringSection walks the Monitoring selector with keys, the way a
// user would: Tab to the section list, down the given number of entries,
// Enter into the section.
func openMonitoringSection(m *Model, down int) {
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	for range down {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
}

// drain runs commands until none are left, feeding results back in, so an
// async chain completes the way it would in the real event loop.
func drain(t *testing.T, m *Model, cmd tea.Cmd, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for cmd != nil && time.Now().Before(deadline) {
		message := cmd()
		if message == nil {
			return
		}
		if batch, ok := message.(tea.BatchMsg); ok {
			for _, c := range batch {
				drain(t, m, c, time.Until(deadline))
			}
			return
		}
		_, cmd = m.Update(message)
	}
}

// TestLiveClusterSwitch connects to the real stack and switches between two
// clusters with different security profiles.
func TestLiveClusterSwitch(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 34})

	drain(t, m, m.connectCurrent(), 30*time.Second)

	if m.client == nil {
		t.Fatal("no client after connecting to local")
	}
	if len(m.info.Brokers) != 3 {
		t.Errorf("got %d brokers, want 3", len(m.info.Brokers))
	}
	t.Logf("connected to local: cluster id %s, %d brokers", m.info.ClusterID, len(m.info.Brokers))

	// Switch to the TLS cluster.
	drain(t, m, m.switchCluster("local-ssl"), 30*time.Second)

	if m.opts.Cluster.Name != "local-ssl" {
		t.Fatalf("current cluster = %q, want local-ssl", m.opts.Cluster.Name)
	}
	if m.client == nil {
		t.Fatal("no client after switching to local-ssl")
	}
	if len(m.info.Brokers) != 3 {
		t.Errorf("after the switch, got %d brokers, want 3", len(m.info.Brokers))
	}
	t.Logf("switched to local-ssl (%s): %d brokers",
		m.opts.Cluster.SecurityProfile(), len(m.info.Brokers))

	// The choice must be persisted for the next run.
	reloaded, err := config.Load(cfg.Path())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.CurrentCluster != "local-ssl" {
		t.Errorf("current-cluster on disk = %q, want local-ssl", reloaded.CurrentCluster)
	}

	// Put it back, so the dev config is left as it was found.
	t.Cleanup(func() { _ = reloaded.SwitchCluster("local") })

	view := ansi.Strip(m.View())
	if !strings.Contains(view, "local-ssl") {
		t.Errorf("the frame should show the new cluster:\n%s", view)
	}
	t.Logf("\n%s", m.View())
}

// TestLiveFailedSwitch switches to a cluster that cannot authenticate.
func TestLiveFailedSwitch(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	drain(t, m, m.switchCluster("local-bad-password"), 30*time.Second)
	t.Cleanup(func() {
		if reloaded, err := config.Load(cfg.Path()); err == nil {
			_ = reloaded.SwitchCluster("local")
		}
	})

	if m.client != nil {
		t.Error("a failed connection must not leave a client behind")
	}
	view := ansi.Strip(m.View())
	if !strings.Contains(view, "disconnected") {
		t.Errorf("the frame should show the failure:\n%s", view)
	}
	if !strings.Contains(view, "authentication was rejected") {
		t.Errorf("the diagnosis should be on screen:\n%s", view)
	}
	t.Logf("\n%s", m.View())
}

// TestLiveTestOtherCluster tests a cluster without disturbing the current one.
func TestLiveTestOtherCluster(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	before := m.client
	drain(t, m, m.testCluster(msg.TestCluster{Name: "local-sasl-scram"}), 30*time.Second)

	if m.client != before {
		t.Error("testing another cluster must not replace the live client")
	}
	if m.opts.Cluster.Name != "local" {
		t.Error("testing another cluster must not switch to it")
	}
	t.Log("the live connection survived a test of another cluster")
}

// TestLiveClustersTab renders the Clusters tab against the real dev config,
// after testing one reachable cluster and one that fails certificate checks.
func TestLiveClustersTab(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 26})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	m.Update(tea.KeyMsg{Type: tea.KeyCtrlK}) // jump to Clusters

	drain(t, m, m.testCluster(msg.TestCluster{Name: "local-sasl-scram"}), 30*time.Second)
	drain(t, m, m.testCluster(msg.TestCluster{Name: "local-untrusted-ca"}), 30*time.Second)

	view := ansi.Strip(m.View())
	if !strings.Contains(view, "● reachable") {
		t.Errorf("the reachable cluster should be marked:\n%s", view)
	}
	if !strings.Contains(view, "✖ failed") {
		t.Errorf("the unreachable cluster should be marked:\n%s", view)
	}

	// Move onto the broken cluster: its full error belongs in the details.
	for range 5 {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	}
	m.notification = chrome.Notification{}

	view = ansi.Strip(m.View())
	flat := strings.Join(strings.Fields(view), " ")
	if !strings.Contains(flat, "certificate signed by unknown authority") {
		t.Errorf("the details panel should carry the whole error:\n%s", view)
	}
	t.Logf("\n%s", m.View())
}

// TestLiveOverview renders the Overview tab against the real stack.
func TestLiveOverview(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 32})

	drain(t, m, m.connectCurrent(), 30*time.Second)
	drain(t, m, m.overviewTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())

	// The panels that prove the metadata layer is wired through. On the KRaft
	// stack the controller line is the quorum line, with the real leader.
	for _, want := range []string{
		"Cluster ID", "kafman-test-cluster-01",
		"Quorum", "leader node", "In sync", "Brokers",
		"orders.events", "billing-worker",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("the Overview should show %q:\n%s", want, view)
		}
	}

	// A healthy stack must not claim problems.
	if strings.Contains(view, "worst:") {
		t.Errorf("a healthy cluster should report no worst partition:\n%s", view)
	}

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveMessages reads real records from the stack and renders them.
func TestLiveMessages(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	// Switch to Messages and load the topic list.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})
	drain(t, m, m.messagesTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())
	if !strings.Contains(view, "orders.events") {
		t.Fatalf("the topic list should be loaded:\n%s", view)
	}

	// A bounded read: following would never return, and the drain helper blocks
	// inside the command that waits on the channel.
	m.messagesTab.SetReadOptions(kafka.Start{Kind: kafka.StartLastN, N: 5}, false, 0)
	drain(t, m, m.messagesTab.Update(msg.FocusTopic{Topic: "orders.events"}), 30*time.Second)

	if m.messagesTab.RecordCount() == 0 {
		t.Fatal("no records were read")
	}
	t.Logf("read %d records", m.messagesTab.RecordCount())

	view = ansi.Strip(m.View())
	if !strings.Contains(view, "ord-") {
		t.Errorf("records should be on screen:\n%s", view)
	}
	if !strings.Contains(view, `"status"`) {
		t.Errorf("the JSON payload should be visible:\n%s", view)
	}

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveQueryFilter runs a real query against the stack and checks that the
// filter actually narrows the result.
func TestLiveQueryFilter(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	// Record batches are routed to the active tab, so it has to be Messages.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})
	drain(t, m, m.messagesTab.Init(), 30*time.Second)

	// dlq.orders holds 12 records, all with status FAILED; orders.events holds
	// 500 with status NEW. A query over both must keep only the twelve.
	m.messagesTab.SelectTopics("orders.events", "dlq.orders")
	m.messagesTab.SetQuery(`value.status == "FAILED" from beginning`)
	drain(t, m, m.messagesTab.StartReading(), 60*time.Second)

	if got := m.messagesTab.RecordCount(); got != 12 {
		t.Errorf("kept %d records, want the 12 FAILED ones from dlq.orders", got)
	}
	t.Logf("read %d records, kept %d", m.messagesTab.ReadCount(), m.messagesTab.RecordCount())

	view := ansi.Strip(m.View())
	if !strings.Contains(view, "ord-fail-") {
		t.Errorf("the matching records should be on screen:\n%s", view)
	}
	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveQueryJSONPath checks a numeric comparison through a JSON path.
func TestLiveQueryJSONPath(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})
	drain(t, m, m.messagesTab.Init(), 30*time.Second)

	// audit.log records carry {"seq": N}; init.sh writes 800 of them.
	m.messagesTab.SelectTopics("audit.log")
	m.messagesTab.SetQuery(`value.seq > 700 from beginning`)
	drain(t, m, m.messagesTab.StartReading(), 60*time.Second)

	kept := m.messagesTab.RecordCount()
	if kept == 0 {
		t.Fatal("a numeric JSON path comparison matched nothing")
	}
	if kept >= 800 {
		t.Errorf("kept %d of 800 records; the filter did not narrow anything", kept)
	}
	t.Logf("value.seq > 700 kept %d of %d records", kept, m.messagesTab.ReadCount())
}

// TestLiveManageTopics drives the Manage tab against the real cluster: create a
// topic through the form, change a config, then delete it.
func TestLiveManageTopics(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")}) // Manage
	drain(t, m, m.manageTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())
	if !strings.Contains(view, "orders.events") {
		t.Fatalf("the topic list should be loaded:\n%s", view)
	}
	// The section list is what makes the tab navigable.
	for _, want := range []string{"Topics", "Consumer groups", "Users", "ACLs"} {
		if !strings.Contains(view, want) {
			t.Errorf("the section list should offer %q:\n%s", want, view)
		}
	}

	name := "kafman-ui-test-" + time.Now().Format("150405")
	t.Cleanup(func() {
		if m.client != nil {
			_ = m.client.DeleteTopic(context.Background(), name)
		}
	})

	// Create it through the tab's own API, the way the form would.
	drain(t, m, m.manageTab.CreateTopic(kafka.TopicSpec{
		Name: name, Partitions: 2, ReplicationFactor: 1,
		Configs: map[string]string{"cleanup.policy": "delete", "retention.ms": "3600000"},
	}), 60*time.Second)

	view = ansi.Strip(m.View())
	if !strings.Contains(view, name) {
		t.Errorf("the created topic should appear in the list:\n%s", view)
	}
	t.Logf("created %s through the Manage tab", name)

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveManageGroups drives the Consumer groups section against the real
// stack: the list, the per-partition offsets of a selected group, and a full
// reset — form, preview, confirmation — ending with the group re-reading from
// the start of the topic.
func TestLiveManageGroups(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	// A group of its own, so the stack's fixtures keep their offsets.
	group := "kafman-ui-group-" + time.Now().Format("150405")
	const topic = "orders.events"

	client := m.client
	if client == nil {
		t.Fatal("the cluster should be connected")
	}
	t.Cleanup(func() { _ = client.DeleteConsumerGroup(context.Background(), group) })

	seed, err := client.PreviewOffsetReset(context.Background(), kafka.ResetSpec{
		Group: group, Topics: []string{topic}, Kind: kafka.ResetToLatest,
	})
	if err != nil {
		t.Fatalf("seed the group: %v", err)
	}
	if err := client.ApplyOffsetReset(context.Background(), group, seed); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	// Land on the group the way Overview's Enter does.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")}) // Manage
	drain(t, m, m.manageTab.FocusGroup(msg.FocusGroup{Group: group}), 60*time.Second)

	view := ansi.Strip(m.View())
	for _, want := range []string{group, topic, "total lag"} {
		if !strings.Contains(view, want) {
			t.Fatalf("the groups section should show %q:\n%s", want, view)
		}
	}
	t.Logf("\n%s", m.View())

	// Reset to the beginning: preview → confirm → apply.
	drain(t, m, m.manageTab.PreviewOffsetReset(kafka.ResetSpec{
		Group: group, Topics: []string{topic}, Kind: kafka.ResetToEarliest,
	}), 60*time.Second)

	view = ansi.Strip(m.View())
	if !strings.Contains(view, "nothing has been changed yet") {
		t.Fatalf("the preview should be on screen:\n%s", view)
	}
	t.Logf("\n%s", m.View())

	drain(t, m, m.manageTab.Update(tea.KeyMsg{Type: tea.KeyEnter}), 10*time.Second) // ask
	drain(t, m, m.manageTab.Update(tea.KeyMsg{Type: tea.KeyEnter}), 60*time.Second) // apply

	described, _, err := client.DescribeConsumerGroup(context.Background(), group)
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}
	if described.TotalLag <= 0 {
		t.Errorf("after a reset to the beginning the lag is %d, want the whole topic",
			described.TotalLag)
	}
	t.Logf("%s is %d records behind after the reset", group, described.TotalLag)

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveManageUsers drives the Users section against the real stack: create a
// SCRAM user through the tab, see it in the list, then delete it.
func TestLiveManageUsers(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	name := "kafman-ui-user-" + time.Now().Format("150405")
	t.Cleanup(func() {
		if m.client != nil {
			_ = m.client.DeleteScramUser(context.Background(), name, nil)
		}
	})

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")}) // Manage
	m.manageTab.OpenSection("Users (SCRAM)")
	drain(t, m, m.manageTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())
	for _, want := range []string{"SCRAM users", "admin", "SCRAM-SHA-512"} {
		if !strings.Contains(view, want) {
			t.Fatalf("the users section should show %q:\n%s", want, view)
		}
	}
	t.Logf("\n%s", m.View())

	drain(t, m, m.manageTab.UpsertScramUser(kafka.ScramUserSpec{
		Name: name, Mechanism: kafka.ScramSHA512, Password: "live-secret",
	}), 60*time.Second)

	if view = ansi.Strip(m.View()); !strings.Contains(view, name) {
		t.Errorf("the created user should appear in the list:\n%s", view)
	}
	t.Logf("created %s through the Manage tab", name)

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveManageACLs drives the ACLs section against the real stack, including
// the stack's own rules created by docker/init.sh.
func TestLiveManageACLs(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 30})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	principal := "User:kafman-ui-acl-" + time.Now().Format("150405")
	acl := kafka.ACL{
		Principal: principal, Host: "*", Operation: "Read", Permission: "Deny",
		ResourceType: "Topic", ResourceName: "orders.events", PatternType: "Literal",
	}
	t.Cleanup(func() {
		if m.client != nil {
			_, _ = m.client.DeleteACL(context.Background(), acl)
		}
	})

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")}) // Manage
	m.manageTab.OpenSection("ACLs")
	drain(t, m, m.manageTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())
	// docker/init.sh grants billing-worker read on orders.events.
	for _, want := range []string{"ACLs", "User:billing-worker", "orders.events"} {
		if !strings.Contains(view, want) {
			t.Fatalf("the ACL list should show %q:\n%s", want, view)
		}
	}

	drain(t, m, m.manageTab.CreateACL(acl), 60*time.Second)

	if view = ansi.Strip(m.View()); !strings.Contains(view, principal) {
		t.Errorf("the created rule should appear in the list:\n%s", view)
	}
	if !strings.Contains(view, "Deny") {
		t.Errorf("the denial should be visible as such:\n%s", view)
	}
	t.Logf("created a Deny rule for %s through the Manage tab", principal)

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveMonitoringUnderLoad is the acceptance test of the stage: with records
// being written to the stack, the dashboard's numbers move and agree with what
// the cluster itself reports.
func TestLiveMonitoringUnderLoad(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	client := m.client
	if client == nil {
		t.Fatal("the cluster should be connected")
	}

	topic := "kafman-load-test-" + time.Now().Format("150405")
	t.Cleanup(func() { _ = client.DeleteTopic(context.Background(), topic) })
	if err := client.CreateTopic(context.Background(), kafka.TopicSpec{
		Name: topic, Partitions: 3, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// A producer running for as long as the dashboard is watching.
	load, stopLoad := context.WithCancel(context.Background())
	defer stopLoad()

	var produced atomic.Int64
	go func() {
		for load.Err() == nil {
			if _, err := client.Produce(load, kafka.ProduceRequest{
				Topic: topic,
				Value: []byte(`{"seq":` + strconv.FormatInt(produced.Load(), 10) + `}`),
			}); err != nil {
				return
			}
			produced.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// A collector of its own, polling faster than the configured interval so the
	// test does not take a minute.
	collector := metrics.NewCollector(client, 700*time.Millisecond)
	collector.Start(context.Background())
	t.Cleanup(collector.Stop)

	const polls = 6
	for range polls {
		select {
		case <-collector.Updates():
		case <-time.After(30 * time.Second):
			t.Fatal("the collector stopped producing updates")
		}
	}
	stopLoad()

	view := collector.View(metrics.MaxWindow)
	if !view.Ready() {
		t.Fatal("after six polls the dashboard should have rates")
	}

	// The topic under load has to be in the view, with a rate in the same order
	// of magnitude as what was actually produced.
	var measured *metrics.NamedSeries
	for i, series := range view.Topics {
		if series.Name == topic {
			measured = &view.Topics[i]
		}
	}
	if measured == nil {
		t.Fatalf("the topic under load is missing from %d series", len(view.Topics))
	}

	elapsed := view.At.Sub(view.Topics[0].Points[0].At).Seconds()
	expected := float64(produced.Load()) / max(elapsed, 1)
	peak := metrics.Max(measured.Points)

	t.Logf("produced %d records, measured peak %.1f msg/s, rough expectation %.1f msg/s",
		produced.Load(), peak, expected)

	if peak <= 0 {
		t.Errorf("a topic being written to should show a positive rate: %v",
			metrics.Values(measured.Points))
	}
	// Sampling boundaries make an exact match impossible; an order of magnitude
	// apart would mean the arithmetic is wrong, not that the sampling drifted.
	if peak > expected*10 || peak*10 < expected {
		t.Errorf("measured %.1f msg/s against roughly %.1f expected", peak, expected)
	}

	// And the same numbers reach the screen, measured by the tab's own collector
	// while the load is still running.
	load2, stopLoad2 := context.WithCancel(context.Background())
	defer stopLoad2()
	go func() {
		for load2.Err() == nil {
			if _, err := client.Produce(load2, kafka.ProduceRequest{
				Topic: topic, Value: []byte(`{"seq":-1}`),
			}); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	// Four readings at the tab's own cadence: a rate over a fraction of a second
	// is noise, and the collector says so by not sampling it at all.
	for range 4 {
		nextPoll(t, m, 30*time.Second)
	}
	stopLoad2()

	frame := ansi.Strip(m.View())
	// The topics column truncates long names from the front, so the tail is
	// what stays visible.
	tail := topic[len(topic)-10:]
	for _, want := range []string{"Throughput", "Busiest topics", "Brokers", "Consumer lag", tail} {
		if !strings.Contains(frame, want) {
			t.Errorf("the dashboard should show %q:\n%s", want, frame)
		}
	}

	// The chart has to have moved: a dashboard drawn while records are flowing
	// must not read as an idle cluster.
	onScreen := m.monitoringTab.Rate()
	if onScreen <= 0 {
		t.Errorf("the tab shows %.1f msg/s while the topic is being written to:\n%s",
			onScreen, frame)
	}
	t.Logf("the tab measured %.1f msg/s of its own", onScreen)

	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveLagAgreesWithKafka checks the consumer-lag column against what the
// cluster reports for the same group, which is the number an operator would
// cross-check with kafka-consumer-groups.
func TestLiveLagAgreesWithKafka(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	client := m.client
	if client == nil {
		t.Fatal("the cluster should be connected")
	}

	collector := metrics.NewCollector(client, 500*time.Millisecond)
	collector.Start(context.Background())
	t.Cleanup(collector.Stop)

	for range 3 {
		select {
		case <-collector.Updates():
		case <-time.After(30 * time.Second):
			t.Fatal("no updates")
		}
	}

	view := collector.View(metrics.MaxWindow)
	if len(view.Groups) == 0 {
		t.Skip("the stack has no consumer groups; run make stack-init")
	}

	groups, err := client.ConsumerGroupsWithLag(context.Background())
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag: %v", err)
	}
	reported := make(map[string]int64, len(groups))
	for _, group := range groups {
		reported[group.Name] = group.TotalLag
	}

	states := make(map[string]string, len(groups))
	for _, group := range groups {
		states[group.Name] = group.State
	}

	var checked int
	for _, series := range view.Groups {
		want, ok := reported[series.Name]
		if !ok {
			continue
		}
		t.Logf("%-20s lag %-8d trend %-9s state %s",
			series.Name, int64(series.Last), series.Trend, states[series.Name])

		// A group with live members commits while the test runs, so the two
		// readings are of two different moments and cannot be compared for
		// equality — only a group whose consumers have exited holds still.
		if states[series.Name] != kafka.GroupEmpty {
			if series.Last < 0 {
				t.Errorf("%s: negative lag %v", series.Name, series.Last)
			}
			continue
		}
		if int64(series.Last) != want {
			t.Errorf("%s: dashboard shows %d, the cluster reports %d",
				series.Name, int64(series.Last), want)
		}
		checked++
	}

	if checked == 0 {
		t.Error("no idle group was available to compare exactly")
	}
}

// TestLiveJMX is the acceptance test of the stage: with the stack's exporters up,
// heap and idle handlers are on screen; with JMX turned off in the config the
// tab works and says what is missing instead of inventing numbers.
func TestLiveJMX(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")
	if !cluster.JMX.Enabled || len(cluster.JMX.Targets) == 0 {
		t.Skip("the dev config has no JMX targets")
	}

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	nextPoll(t, m, 30*time.Second)

	readings := m.monitoringTab.JMX()
	if len(readings) != len(cluster.JMX.Targets) {
		t.Fatalf("got %d scrapes, want one per target", len(readings))
	}
	for _, broker := range readings {
		if !broker.OK() {
			t.Errorf("broker %d: %v", broker.BrokerID, broker.Err)
			continue
		}
		if broker.HeapUsed <= 0 || broker.HeapMax <= 0 {
			t.Errorf("broker %d reported no heap: %v / %v",
				broker.BrokerID, broker.HeapUsed, broker.HeapMax)
		}
		if !broker.Has("handler_idle") {
			t.Errorf("broker %d reported no request-handler idle ratio", broker.BrokerID)
		}
		t.Logf("broker %d: heap %.0f%%, handler idle %.0f%%, GC avg %v, in %.0f B/s",
			broker.BrokerID, broker.HeapFraction()*100,
			metrics.IdlePercent(broker.HandlerIdle), broker.GCPause(), broker.BytesIn)
	}

	// Bytes in is the headline number the Kafka API cannot give; under load it
	// has to move, otherwise the panel is showing a metric it never reads.
	client := m.client
	if client == nil {
		t.Fatal("the cluster should be connected")
	}
	// A topic of its own: the stack's fixtures carry exact record counts that
	// other tests assert on, and a load test that writes into them breaks those
	// tests in a way that looks unrelated.
	topic := "kafman-jmx-load-" + time.Now().Format("150405")
	t.Cleanup(func() { _ = client.DeleteTopic(context.Background(), topic) })
	if err := client.CreateTopic(context.Background(), kafka.TopicSpec{
		Name: topic, Partitions: 3, ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Kafka's meters are exponentially weighted and tick every five seconds, so
	// the load has to run for longer than a burst before the one-minute rate
	// means anything. Replication factor 3 puts traffic on every broker.
	deadline := time.Now().Add(12 * time.Second)
	records := 0
	for time.Now().Before(deadline) {
		if _, err := client.Produce(context.Background(), kafka.ProduceRequest{
			Topic: topic,
			Value: []byte(fmt.Sprintf(`{"jmx-probe":%d}`, records)),
		}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		records++
	}
	// Poll now and take the reading that poll produces.
	m.monitoringTab.Update(tea.KeyMsg{Type: tea.KeyF5})
	nextPoll(t, m, 30*time.Second)

	var peak float64
	for _, broker := range m.monitoringTab.JMX() {
		if !broker.OK() {
			continue
		}
		peak = math.Max(peak, broker.BytesIn)
		t.Logf("broker %d after %d records: in %.0f B/s, out %.0f B/s",
			broker.BrokerID, records, broker.BytesIn, broker.BytesOut)
	}
	// A hundred bytes a second cannot come from an idle cluster's noise.
	if peak < 100 {
		t.Errorf("the busiest broker reported %.1f B/s in after %d records", peak, records)
	}

	// The exporter's numbers live in their own section now.
	openMonitoringSection(m, 2)
	frame := ansi.Strip(m.View())
	for _, want := range []string{"JMX", "HEAP", "IDLE", "NET IDLE", "scraped"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the JMX section should show %q:\n%s", want, frame)
		}
	}
	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveWithoutJMX runs the same tab against the same cluster with JMX turned
// off, which is the other half of the criterion: no exporter, no errors, no
// invented numbers.
func TestLiveWithoutJMX(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	// The same cluster, minus the exporters.
	without := *cluster
	without.JMX = config.JMX{}

	m := New(Options{Config: cfg, Cluster: &without, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	nextPoll(t, m, 30*time.Second)

	if got := m.monitoringTab.JMX(); len(got) != 0 {
		t.Errorf("a cluster without an exporter should have no scrapes, got %d", len(got))
	}

	frame := ansi.Strip(m.View())
	if !strings.Contains(frame, "need a JMX exporter") {
		t.Errorf("the panel should say what is missing:\n%s", frame)
	}
	// The rest of the dashboard is unaffected: the Kafka API needs no exporter.
	for _, want := range []string{"Throughput", "Busiest topics", "LEADERS", "Consumer lag"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the dashboard should still show %q:\n%s", want, frame)
		}
	}
	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveNodeExporter is the acceptance test of the OS section: with the
// stack's node_exporters up, every node reports OS metrics, and at least one
// resolves its data directory through log.dirs — the whole DescribeConfigs →
// mountpoint chain, end to end.
func TestLiveNodeExporter(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")
	if !cluster.NodeExporter.Enabled || len(cluster.NodeExporter.Targets) == 0 {
		t.Skip("the dev config has no node-exporter targets")
	}

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	nextPoll(t, m, 30*time.Second)

	// CPU, disk and network are counters: a rate needs a second scrape, and a
	// second scrape closer than a quarter of the interval is discarded as
	// noise, so the second poll has to wait that quarter out.
	time.Sleep(2 * time.Second)
	m.monitoringTab.Update(tea.KeyMsg{Type: tea.KeyF5})
	nextPoll(t, m, 30*time.Second)

	nodes := m.monitoringTab.Nodes()
	if len(nodes) != len(cluster.NodeExporter.Targets) {
		t.Fatalf("got %d node scrapes, want one per target", len(nodes))
	}

	dataDirResolved := false
	for _, node := range nodes {
		if !node.OK() {
			t.Errorf("node %d: %v", node.BrokerID, node.Err)
			continue
		}
		if node.MemTotal <= 0 {
			t.Errorf("node %d reported no memory", node.BrokerID)
		}
		if !node.Has("load") || node.Load1 < 0 {
			t.Errorf("node %d reported no load average", node.BrokerID)
		}
		if len(node.CPUSeries) == 0 {
			t.Errorf("node %d has no CPU rate after two polls", node.BrokerID)
		}
		for _, p := range node.CPUSeries {
			if p.Value < 0 || p.Value > 100 {
				t.Errorf("node %d: cpu%% out of range: %v", node.BrokerID, p.Value)
			}
		}
		if node.DataDirKnown && node.DataDirPath == "/var/lib/kafka/data" {
			dataDirResolved = true
		}
		t.Logf("node %d: cpu series %d, mem %.1f GiB, load %.2f, data dir known=%v",
			node.BrokerID, len(node.CPUSeries), node.MemTotal/(1<<30), node.Load1, node.DataDirKnown)
	}
	if !dataDirResolved {
		t.Error("no node resolved its data dir to /var/lib/kafka/data — the log.dirs match is broken")
	}

	// And the section renders what was scraped.
	openMonitoringSection(m, 3)
	frame := ansi.Strip(m.View())
	for _, want := range []string{"CPU", "RAM", "LOAD 1/5/15", "data dir /var/lib/kafka/data"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the OS section should show %q:\n%s", want, frame)
		}
	}
	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// TestLiveGroupsConsumeRate is the acceptance test of the Groups section: with
// the live profile's consumers connected, the fast group shows a positive read
// rate with members, and the slow group reads well below the produce rate —
// the "producing faster than consuming" picture the section exists for.
func TestLiveGroupsConsumeRate(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 34})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	requireLiveGroups(t, m)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	nextPoll(t, m, 30*time.Second)

	// A read rate is a delta over at least a quarter of the interval, so the
	// second poll has to wait that out; the live producer writes ~20 rec/s,
	// so a couple of seconds is measurable.
	groupByName := func(name string) (metrics.GroupSeries, bool) {
		for _, group := range m.monitoringTab.Groups() {
			if group.Name == name {
				return group, true
			}
		}
		return metrics.GroupSeries{}, false
	}
	var fast metrics.GroupSeries
	for attempt := range 5 {
		time.Sleep(2 * time.Second)
		m.monitoringTab.Update(tea.KeyMsg{Type: tea.KeyF5})
		nextPoll(t, m, 30*time.Second)
		if got, ok := groupByName("live-orders-fast"); ok && got.ReadKnown && got.ReadLast > 0 {
			fast = got
			break
		}
		t.Logf("attempt %d: the fast group's read rate is not measured yet", attempt+1)
	}

	if !fast.ReadKnown || fast.ReadLast <= 0 {
		t.Fatalf("live-orders-fast should have a positive read rate: %+v", fast)
	}
	if fast.MembersNow < 1 {
		t.Errorf("live-orders-fast has connected members, got %d", fast.MembersNow)
	}
	t.Logf("live-orders-fast: read %.1f rec/s, %d members", fast.ReadLast, fast.MembersNow)

	// The slow group deliberately cannot keep up: it reads one record per
	// 200ms while the producer writes ~20/s.
	if slow, ok := groupByName("live-orders-slow"); ok && slow.ReadKnown {
		produce := m.monitoringTab.Rate()
		t.Logf("live-orders-slow: read %.1f rec/s against produce %.1f rec/s", slow.ReadLast, produce)
		if produce > 0 && slow.ReadLast >= produce {
			t.Errorf("the slow group cannot read (%.1f) as fast as the cluster writes (%.1f)",
				slow.ReadLast, produce)
		}
	}

	// And the section renders it.
	openMonitoringSection(m, 1)
	frame := ansi.Strip(m.View())
	for _, want := range []string{"READ/S", "MEMBERS", "Produce vs consume", "live-orders-fast"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the Groups section should show %q:\n%s", want, frame)
		}
	}
	m.notification = chrome.Notification{}
	t.Logf("\n%s", m.View())
}

// requireLiveGroups skips when the live compose profile is not running: a stack
// without it is a valid state, and the message says how to start it.
func requireLiveGroups(t *testing.T, m *Model) {
	t.Helper()

	if m.client == nil {
		t.Fatal("the cluster should be connected")
	}
	groups, err := m.client.ListConsumerGroups(context.Background())
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	for _, group := range groups {
		if group.Name == "live-orders-fast" && group.State == kafka.GroupStable {
			return
		}
	}
	t.Skip("live-orders-fast is not running; start it with: make stack-live")
}

// TestLiveGroupsOnScreen is the UI half of the live-group fixture: a group with
// real members has to render as one — Stable, with a member count, owners on its
// partitions and an assignment behind Enter.
func TestLiveGroupsOnScreen(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 32})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	requireLiveGroups(t, m)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")}) // Manage
	drain(t, m, m.manageTab.FocusGroup(msg.FocusGroup{Group: "live-orders-fast"}), 30*time.Second)

	view := ansi.Strip(m.View())
	for _, want := range []string{"live-orders-fast", "Stable", "live.orders", "172.18."} {
		if !strings.Contains(view, want) {
			t.Errorf("the groups section should show %q:\n%s", want, view)
		}
	}
	// A Stable group's partitions have owners; the dash is for partitions nobody
	// is reading.
	if strings.Contains(view, "live.orders") && strings.Count(view, "―") > 2 {
		t.Errorf("a Stable group should not show unowned partitions:\n%s", view)
	}

	// Enter opens the members, which is the only place an assignment is spelled
	// out.
	drain(t, m, m.manageTab.Update(tea.KeyMsg{Type: tea.KeyEnter}), 10*time.Second)

	members := ansi.Strip(m.View())
	for _, want := range []string{"members", "fast-1", "fast-2", "fast-3", "assignment", "live.orders p"} {
		if !strings.Contains(members, want) {
			t.Errorf("the members screen should show %q:\n%s", want, members)
		}
	}
	t.Logf("\n%s", m.View())
}

// TestLiveGroupsOnOverview checks the count the Overview summarises them with:
// with the live profile running, "0 stable" would be wrong.
func TestLiveGroupsOnOverview(t *testing.T) {
	cfg := devConfig(t)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 32})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	requireLiveGroups(t, m)

	drain(t, m, m.overviewTab.Init(), 30*time.Second)

	view := ansi.Strip(m.View())
	if strings.Contains(view, "(0 stable)") {
		t.Errorf("with live consumers connected the cluster has stable groups:\n%s", view)
	}
	if !strings.Contains(view, "live-orders-slow") {
		t.Errorf("the laggiest group should be on the Overview:\n%s", view)
	}
}

// TestLiveLagTrendIsGrowing checks the Monitoring column against a group that
// cannot keep up: over several polls its lag only goes one way.
func TestLiveLagTrendIsGrowing(t *testing.T) {
	cfg := devConfig(t)
	cfg.UI.RefreshInterval = config.Duration(time.Second)
	cluster, _ := cfg.Cluster("local")

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman-test.log"})
	m.Update(tea.WindowSizeMsg{Width: 130, Height: 32})
	drain(t, m, m.connectCurrent(), 30*time.Second)
	requireLiveGroups(t, m)

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")}) // Monitoring
	m.monitoringTab.Init()
	for range 8 {
		nextPoll(t, m, 30*time.Second)
	}

	var slow *metrics.GroupSeries
	for i, group := range m.monitoringTab.Groups() {
		if group.Name == "live-orders-slow" {
			slow = &m.monitoringTab.Groups()[i]
		}
	}
	if slow == nil {
		t.Fatal("the slow group is missing from the dashboard")
	}
	t.Logf("live-orders-slow: lag %.0f, trend %s over %d samples",
		slow.Last, slow.Trend, len(slow.Points))

	if slow.Trend != metrics.TrendGrowing {
		t.Errorf("a group that reads slower than the producer writes should be growing, got %s",
			slow.Trend)
	}
	if !strings.Contains(ansi.Strip(m.View()), "growing") {
		t.Error("the trend should be named on screen")
	}
}
