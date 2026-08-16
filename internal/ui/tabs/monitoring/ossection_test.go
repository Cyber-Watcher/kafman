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

	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// nodeExporterBody is what a node_exporter serves; the counters advance with n
// so two scrapes produce a measurable rate.
func nodeExporterBody(n float64) string {
	return fmt.Sprintf(`node_cpu_seconds_total{cpu="0",mode="idle"} %g
node_cpu_seconds_total{cpu="0",mode="user"} %g
node_memory_MemTotal_bytes 16000000000
node_memory_MemAvailable_bytes 4000000000
node_load1 0.42
node_load5 0.38
node_load15 0.35
node_disk_read_bytes_total{device="sda"} %g
node_disk_written_bytes_total{device="sda"} %g
node_network_receive_bytes_total{device="eth0"} %g
node_network_transmit_bytes_total{device="eth0"} %g
node_network_receive_bytes_total{device="lo"} 99e9
node_filesystem_size_bytes{device="/dev/sda1",fstype="xfs",mountpoint="/"} 50000000000
node_filesystem_avail_bytes{device="/dev/sda1",fstype="xfs",mountpoint="/"} 20000000000
node_filesystem_size_bytes{device="/dev/sdb1",fstype="xfs",mountpoint="/var/lib/kafka/data"} 100000000000
node_filesystem_avail_bytes{device="/dev/sdb1",fstype="xfs",mountpoint="/var/lib/kafka/data"} 30000000000
`, 1000+8*n, 100+2*n, 3000*n, 4000*n, 5000*n, 2000*n)
}

func nodeServer(t *testing.T) *httptest.Server {
	t.Helper()

	var scrapes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(nodeExporterBody(float64(scrapes.Add(1)))))
	}))
	t.Cleanup(server.Close)
	return server
}

// nodeMock is a mock cluster whose nodes expose the given exporter URLs.
func nodeMock(urls ...string) *kafka.Mock {
	cluster := &config.Cluster{Name: "with-nodes", Brokers: []string{"localhost:9092"}}
	cluster.NodeExporter.Enabled = true
	for i, url := range urls {
		cluster.NodeExporter.Targets = append(cluster.NodeExporter.Targets,
			config.NodeTarget{BrokerID: int32(i + 1), URL: url})
	}

	mock := growingMock(600)
	mock.ClusterFunc = func() *config.Cluster { return cluster }
	return mock
}

// nodeTab wires a tab to such a mock and polls it.
func nodeTab(t *testing.T, polls int, urls ...string) *Model {
	t.Helper()
	return newTab(t, nodeMock(urls...), polls)
}

func TestOSSectionRendersNodeData(t *testing.T) {
	m := nodeTab(t, 3, nodeServer(t).URL)

	openSection(m, sectionOS)
	view := render(t, m)

	for _, want := range []string{
		"CPU", "RAM", "LOAD 1/5/15", "DISK R/W", "NET RX/TX",
		"75%",                                    // RAM: 1 - 4/16
		"0.42/0.38/0.35",                         // loads
		"data dir /var/lib/kafka/data: 70% used", // matched against the mock's log.dirs
		"root /: 60% used",
		"node 1", // the chart legend
		"scraped",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("the OS section should show %q:\n%s", want, view)
		}
	}
}

// One poll: gauges are real, but a counter has nothing to compare against, and
// the section says so rather than showing a zero rate.
func TestOSSectionRatesNeedTwoPolls(t *testing.T) {
	m := nodeTab(t, 1, nodeServer(t).URL)

	openSection(m, sectionOS)
	view := render(t, m)

	if !strings.Contains(view, "75%") {
		t.Errorf("gauges are valid from the first scrape:\n%s", view)
	}
	if !strings.Contains(view, "—") {
		t.Errorf("rates without a second scrape should read as a dash:\n%s", view)
	}
}

// A node whose data dir cannot be matched — here because log.dirs is not
// readable at all — says "unknown" rather than picking a filesystem at random.
func TestOSSectionStatesTheGaps(t *testing.T) {
	mock := nodeMock(nodeServer(t).URL)
	mock.BrokerLogDirsFunc = func(context.Context) (map[int32][]string, error) {
		return nil, errors.New("cluster authorization failed")
	}
	m := newTab(t, mock, 2)

	openSection(m, sectionOS)
	view := render(t, m)
	if !strings.Contains(view, "data dir: unknown") {
		t.Errorf("an unmatched data dir must be named as unknown:\n%s", view)
	}
}

// An unreachable node keeps its row with the error.
func TestOSSectionKeepsUnreachableNodes(t *testing.T) {
	m := nodeTab(t, 2, nodeServer(t).URL, "http://127.0.0.1:1/metrics")

	openSection(m, sectionOS)
	view := render(t, m)
	if !strings.Contains(view, "unreachable") {
		t.Errorf("a failed scrape should say so:\n%s", view)
	}
	if !strings.Contains(view, "75%") {
		t.Errorf("the healthy node's numbers should still be there:\n%s", view)
	}
}

// The OS table is drawn by hand like the brokers panel, and has the same
// invariant to break.
func TestOSBodyFitsItsWidth(t *testing.T) {
	m := nodeTab(t, 3, nodeServer(t).URL)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, width := range []int{30, 40, 55, 63, 80, 120} {
		k := ktx.New(cfg, theme.New(theme.Dark, false), 130, 30)
		for _, line := range strings.Split(m.osBody(k, width), "\n") {
			if got := ansi.StringWidth(line); got > width-1 {
				t.Errorf("at width %d a line is %d wide: %q", width, got, ansi.Strip(line))
			}
		}
	}
}

// The section splitters must hand out exactly what they were given.
func TestSectionHeightsAlwaysFit(t *testing.T) {
	for available := 1; available <= 60; available++ {
		for brokers := 1; brokers <= 5; brokers++ {
			table, chart := jmxHeights(available, brokers)
			if table < 0 || chart < 0 {
				t.Fatalf("jmxHeights(%d, %d) went negative: %d/%d", available, brokers, table, chart)
			}
			if table+chart != max(available, 0) {
				t.Errorf("jmxHeights(%d, %d) uses %d rows", available, brokers, table+chart)
			}

			chart, table = osHeights(available, brokers)
			if table < 0 || chart < 0 {
				t.Fatalf("osHeights(%d, %d) went negative: %d/%d", available, brokers, chart, table)
			}
			if table+chart != max(available, 0) {
				t.Errorf("osHeights(%d, %d) uses %d rows", available, brokers, table+chart)
			}
		}
	}
}

// The whole frame fits in the JMX and OS sections too, including small
// terminals.
func TestSectionsFitTheFrame(t *testing.T) {
	m := nodeTab(t, 3, nodeServer(t).URL)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range []section{sectionJMX, sectionOS} {
		openSection(m, target)
		for _, size := range [][2]int{{61, 16}, {80, 24}, {130, 30}, {200, 60}} {
			k := ktx.New(cfg, theme.New(theme.Dark, false), size[0], size[1])
			for i, line := range strings.Split(m.View(k), "\n") {
				if got := ansi.StringWidth(line); got > size[0] {
					t.Errorf("section %v at %dx%d: line %d is %d wide: %q",
						target, size[0], size[1], i, got, ansi.Strip(line))
				}
			}
		}
	}
}

// The chart cycles through the metrics with t, and each names itself.
func TestOSChartCycles(t *testing.T) {
	m := nodeTab(t, 3, nodeServer(t).URL)

	openSection(m, sectionOS)
	if view := render(t, m); !strings.Contains(view, "CPU  %") {
		t.Errorf("the OS chart starts on CPU:\n%s", view)
	}
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "Network rx+tx") {
		t.Errorf("t should switch to network:\n%s", view)
	}
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "Disk r+w") {
		t.Errorf("t again should switch to disk:\n%s", view)
	}
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "CPU  %") {
		t.Errorf("the cycle should wrap back to CPU:\n%s", view)
	}
}

// CPU percentages on screen stay inside [0, 100].
func TestOSSectionCPUSane(t *testing.T) {
	m := nodeTab(t, 3, nodeServer(t).URL)

	for _, node := range m.Nodes() {
		if len(node.CPUSeries) == 0 {
			t.Fatal("after three polls the CPU series should exist")
		}
		for _, p := range node.CPUSeries {
			if p.Value < 0 || p.Value > 100 {
				t.Errorf("cpu%% out of range: %v", p.Value)
			}
		}
	}
}
