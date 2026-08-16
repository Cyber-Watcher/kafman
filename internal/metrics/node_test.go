package metrics

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

func nodeFixture(t *testing.T) NodeOS {
	t.Helper()

	data, err := os.ReadFile("testdata/node_sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	samples, err := ParseExposition(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	node := NodeOS{BrokerID: 1}
	SelectNode(&node, samples)
	return node
}

func TestSelectNodeSumsTheCounters(t *testing.T) {
	node := nodeFixture(t)

	if got, want := node.CPUTotalSeconds, 205101.5; math.Abs(got-want) > 0.01 {
		t.Errorf("cpu total = %v, want %v (sum over every cpu and mode)", got, want)
	}
	if got, want := node.CPUIdleSeconds, 199001.0; math.Abs(got-want) > 0.01 {
		t.Errorf("cpu idle = %v, want %v", got, want)
	}
	if node.CPUIdleSeconds >= node.CPUTotalSeconds {
		t.Error("idle time must be a share of the total")
	}
	if got, want := node.DiskReadBytes, 2.0e9; got != want {
		t.Errorf("disk read = %v, want %v (sum over devices)", got, want)
	}
	if got, want := node.DiskWrittenBytes, 4.0e9; got != want {
		t.Errorf("disk written = %v, want %v", got, want)
	}
}

func TestSelectNodeExcludesLoopback(t *testing.T) {
	node := nodeFixture(t)

	if got, want := node.NetRxBytes, 7.5e9; got != want {
		t.Errorf("net rx = %v, want %v — lo must not be counted", got, want)
	}
	if got, want := node.NetTxBytes, 3.1e9; got != want {
		t.Errorf("net tx = %v, want %v — lo must not be counted", got, want)
	}
}

func TestSelectNodeGauges(t *testing.T) {
	node := nodeFixture(t)

	if got := node.MemUsedFraction(); math.Abs(got-0.5) > 0.001 {
		t.Errorf("mem used fraction = %v, want 0.5", got)
	}
	if node.Load1 != 0.42 || node.Load5 != 0.38 || node.Load15 != 0.35 {
		t.Errorf("loads = %v/%v/%v", node.Load1, node.Load5, node.Load15)
	}
	for _, family := range []string{"cpu", "mem", "load", "disk", "net", "fs"} {
		if !node.Has(family) {
			t.Errorf("family %q should be present", family)
		}
	}
}

func TestSelectNodeJoinsFilesystems(t *testing.T) {
	node := nodeFixture(t)

	root, ok := node.FS("/")
	if !ok {
		t.Fatal("the root filesystem should be reported")
	}
	if got := root.UsedFraction(); math.Abs(got-0.6) > 0.001 {
		t.Errorf("root used fraction = %v, want 0.6", got)
	}

	data, ok := node.FS("/var/lib/kafka/data")
	if !ok {
		t.Fatal("the data filesystem should be reported")
	}
	if got := data.UsedFraction(); math.Abs(got-0.7) > 0.001 {
		t.Errorf("data used fraction = %v, want 0.7", got)
	}

	// A filesystem with size 0 has no usable usage fraction and must be
	// dropped rather than divide by zero later.
	if _, ok := node.FS("/sys"); ok {
		t.Error("a zero-size filesystem should be dropped")
	}
}

func TestMatchDataDir(t *testing.T) {
	mounts := []FSStat{
		{Mountpoint: "/", SizeBytes: 100},
		{Mountpoint: "/var/lib", SizeBytes: 100},
		{Mountpoint: "/var/lib/kafka/data", SizeBytes: 100},
	}

	fs, dir, ok := MatchDataDir(mounts, []string{"/var/lib/kafka/data"})
	if !ok || fs.Mountpoint != "/var/lib/kafka/data" || dir != "/var/lib/kafka/data" {
		t.Errorf("exact mountpoint should win: %v %q %v", fs, dir, ok)
	}

	fs, _, ok = MatchDataDir(mounts, []string{"/var/lib/kafka/logs"})
	if !ok || fs.Mountpoint != "/var/lib" {
		t.Errorf("the longest prefix should win: %v %v", fs, ok)
	}

	fs, _, ok = MatchDataDir([]FSStat{{Mountpoint: "/", SizeBytes: 100}}, []string{"/data"})
	if !ok || fs.Mountpoint != "/" {
		t.Errorf("root matches everything: %v %v", fs, ok)
	}

	// /var/library must not match the /var/lib mountpoint: prefixes are whole
	// path segments, not strings.
	fs, _, ok = MatchDataDir([]FSStat{{Mountpoint: "/var/lib", SizeBytes: 100}}, []string{"/var/library"})
	if ok {
		t.Errorf("partial segment matched: %v", fs)
	}

	if _, _, ok := MatchDataDir(mounts, nil); ok {
		t.Error("no log.dirs means no match — the UI must say unknown")
	}
}

func TestRateF(t *testing.T) {
	if got := RateF(100, 200, 10*time.Second); got != 10 {
		t.Errorf("RateF = %v, want 10", got)
	}
	if got := RateF(200, 100, 10*time.Second); got != 0 {
		t.Errorf("a counter reset must read as zero, got %v", got)
	}
	if got := RateF(100, 200, 0); got != 0 {
		t.Errorf("a zero span must read as zero, got %v", got)
	}
}

// nodeCluster is a mock whose cluster has node-exporter targets pointing at
// the given URLs.
func nodeCluster(urls ...string) *kafka.Mock {
	mock := kafka.NewMock(nil)
	base := mock.Cluster()

	targets := make([]config.NodeTarget, len(urls))
	for i, u := range urls {
		targets[i] = config.NodeTarget{BrokerID: int32(i + 1), URL: u}
	}
	base.NodeExporter = config.NodeExporter{Enabled: true, Targets: targets}
	mock.ClusterFunc = func() *config.Cluster { return base }
	return mock
}

// countingNodeServer serves a node_exporter exposition whose counters advance
// on every request, so two scrapes produce a measurable rate.
func countingNodeServer(t *testing.T) *httptest.Server {
	t.Helper()

	var scrapes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := float64(scrapes.Add(1))
		body := "node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"} " + formatFloat(1000+8*n) + "\n" +
			"node_cpu_seconds_total{cpu=\"0\",mode=\"user\"} " + formatFloat(100+2*n) + "\n" +
			"node_memory_MemTotal_bytes 1000\n" +
			"node_memory_MemAvailable_bytes 400\n" +
			"node_load1 0.5\nnode_load5 0.4\nnode_load15 0.3\n" +
			"node_network_receive_bytes_total{device=\"eth0\"} " + formatFloat(5000*n) + "\n" +
			"node_network_transmit_bytes_total{device=\"eth0\"} " + formatFloat(2000*n) + "\n" +
			"node_disk_read_bytes_total{device=\"sda\"} " + formatFloat(3000*n) + "\n" +
			"node_disk_written_bytes_total{device=\"sda\"} " + formatFloat(4000*n) + "\n" +
			"node_filesystem_size_bytes{mountpoint=\"/var/lib/kafka/data\"} 1000\n" +
			"node_filesystem_avail_bytes{mountpoint=\"/var/lib/kafka/data\"} 250\n"
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func TestCollectorScrapesNodes(t *testing.T) {
	server := countingNodeServer(t)
	mock := nodeCluster(server.URL)

	// One poll: gauges are present, but a counter has nothing to compare
	// against yet — no series, no invented rates.
	view := poll(t, NewCollector(mock, 5*time.Millisecond), 1)
	if !view.NodesConfigured {
		t.Fatal("the cluster has node-exporter targets")
	}
	if len(view.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(view.Nodes))
	}
	node := view.Nodes[0]
	if !node.OK() || node.MemTotal != 1000 {
		t.Errorf("gauges should be valid from the first scrape: %+v", node.NodeOS)
	}
	if len(node.CPUSeries) != 0 {
		t.Errorf("one scrape cannot be a rate, got %v", node.CPUSeries)
	}
	if len(node.RAMSeries) == 0 {
		t.Error("memory is a gauge — its history starts with the first scrape")
	}
	if !node.DataDirKnown || node.DataDirFS.Mountpoint != "/var/lib/kafka/data" {
		t.Errorf("the data dir should resolve against the mock's log.dirs: %+v", node)
	}

	// More polls: rates land and stay sane.
	view = poll(t, NewCollector(nodeCluster(server.URL), 5*time.Millisecond), 3)
	node = view.Nodes[0]
	if len(node.CPUSeries) == 0 || len(node.NetRxSeries) == 0 || len(node.DiskWriteSeries) == 0 {
		t.Fatalf("after three polls the counter series should exist: %+v", node)
	}
	for _, p := range node.CPUSeries {
		if p.Value < 0 || p.Value > 100 {
			t.Errorf("cpu%% out of range: %v", p.Value)
		}
	}
}

// The tab's Init runs on every switch onto it; a second Start must not add a
// second polling goroutine, whose immediate poll would reset every baseline.
func TestStartIsIdempotent(t *testing.T) {
	server := countingNodeServer(t)
	collector := NewCollector(nodeCluster(server.URL), time.Hour)

	collector.Start(context.Background())
	t.Cleanup(collector.Stop)

	select {
	case <-collector.Updates():
	case <-time.After(5 * time.Second):
		t.Fatal("no first poll")
	}

	// A second Start with an hour-long interval: any new poll within the wait
	// below could only come from a second goroutine's immediate poll.
	collector.Start(context.Background())
	select {
	case <-collector.Updates():
		t.Fatal("a second Start started a second poller")
	case <-time.After(50 * time.Millisecond):
	}
}

// A node whose exporter is down keeps its row with the error: "this node's
// exporter is unreachable" is itself worth showing.
func TestUnreachableNodeKeepsItsRow(t *testing.T) {
	good := countingNodeServer(t)
	mock := nodeCluster(good.URL, "http://127.0.0.1:1/metrics")

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 1)
	if len(view.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(view.Nodes))
	}
	if !view.Nodes[0].OK() {
		t.Errorf("the good node should be ok: %v", view.Nodes[0].Err)
	}
	if view.Nodes[1].OK() {
		t.Error("the unreachable node should carry its error")
	}
}
