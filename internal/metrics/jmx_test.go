package metrics

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// sampleExposition is a capture from the test stack's exporter.
func sampleExposition(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile("testdata/jmx_sample.txt")
	if err != nil {
		t.Fatalf("read the captured exposition: %v", err)
	}
	return string(data)
}

func TestParseExpositionOnRealOutput(t *testing.T) {
	samples, err := ParseExposition(strings.NewReader(sampleExposition(t)))
	if err != nil {
		t.Fatalf("ParseExposition: %v", err)
	}
	if len(samples) == 0 {
		t.Fatal("nothing was parsed")
	}

	byName := make(map[string][]Sample)
	for _, sample := range samples {
		byName[sample.Name] = append(byName[sample.Name], sample)
	}

	// Comments carry no values and must not become samples.
	for _, sample := range samples {
		if strings.HasPrefix(sample.Name, "#") {
			t.Errorf("a comment became a sample: %+v", sample)
		}
	}

	// Scientific notation is how the exporter writes large numbers.
	heap := byName["jvm_memory_heap_max_bytes"]
	if len(heap) != 1 {
		t.Fatalf("heap max appears %d times, want once", len(heap))
	}
	if heap[0].Value != 5.36870912e8 {
		t.Errorf("heap max = %v, want 536870912", heap[0].Value)
	}

	// Labels: one metric, one series per garbage collector.
	gc := byName["jvm_gc_collection_seconds_count"]
	if len(gc) < 2 {
		t.Fatalf("garbage collectors give %d series, want one each", len(gc))
	}
	names := make(map[string]bool)
	for _, sample := range gc {
		names[sample.Label("gc")] = true
	}
	if !names["G1 Young Generation"] {
		t.Errorf("a label with spaces should survive parsing, got %v", names)
	}
}

func TestParseSampleForms(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		want   Sample
		reject bool
	}{
		{
			name: "plain",
			line: "jvm_memory_heap_used_bytes 3.39539616E8",
			want: Sample{Name: "jvm_memory_heap_used_bytes", Value: 3.39539616e8},
		},
		{
			name: "integer",
			line: "jvm_gc_collection_seconds_count 124",
			want: Sample{Name: "jvm_gc_collection_seconds_count", Value: 124},
		},
		{
			name: "one label",
			line: `jvm_gc_collection_seconds_sum{gc="G1 Old Generation"} 0.0`,
			want: Sample{
				Name:   "jvm_gc_collection_seconds_sum",
				Labels: map[string]string{"gc": "G1 Old Generation"},
			},
		},
		{
			name: "several labels",
			line: `kafka_network_requestmetrics_localtimems_mean{request="Fetch",quantile="0.99"} 1.5`,
			want: Sample{
				Name:   "kafka_network_requestmetrics_localtimems_mean",
				Labels: map[string]string{"request": "Fetch", "quantile": "0.99"},
				Value:  1.5,
			},
		},
		{
			name: "escaped quote in a label",
			line: `metric{name="a \"quoted\" bean"} 1`,
			want: Sample{
				Name:   "metric",
				Labels: map[string]string{"name": `a "quoted" bean`},
				Value:  1,
			},
		},
		{
			// The format allows a trailing timestamp; kafman ignores it.
			name: "with a timestamp",
			line: "metric 42 1700000000000",
			want: Sample{Name: "metric", Value: 42},
		},
		{name: "negative", line: "jvm_memory_max_bytes -1.0", want: Sample{Name: "jvm_memory_max_bytes", Value: -1}},
		{name: "no value", line: "metric", reject: true},
		{name: "not a number", line: "metric abc", reject: true},
		{name: "unbalanced braces", line: `metric{a="1" 5`, reject: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := parseSample(test.line)
			if test.reject {
				if ok {
					t.Errorf("the line should have been rejected, got %+v", got)
				}
				return
			}
			if !ok {
				t.Fatalf("the line should have parsed: %q", test.line)
			}
			if got.Name != test.want.Name {
				t.Errorf("name = %q, want %q", got.Name, test.want.Name)
			}
			if test.want.Value != 0 && got.Value != test.want.Value {
				t.Errorf("value = %v, want %v", got.Value, test.want.Value)
			}
			for key, want := range test.want.Labels {
				if got.Label(key) != want {
					t.Errorf("label %s = %q, want %q", key, got.Label(key), want)
				}
			}
		})
	}
}

// NaN and the infinities are part of the format; they must not abort a scrape.
func TestParseExpositionSurvivesSpecialValues(t *testing.T) {
	text := strings.Join([]string{
		"# TYPE weird gauge",
		"weird_nan NaN",
		"weird_inf +Inf",
		"weird_neg_inf -Inf",
		"broken_line",
		"after_the_broken_line 7",
	}, "\n")

	samples, err := ParseExposition(strings.NewReader(text))
	if err != nil {
		t.Fatalf("ParseExposition: %v", err)
	}

	byName := make(map[string]float64, len(samples))
	for _, sample := range samples {
		byName[sample.Name] = sample.Value
	}

	if !math.IsNaN(byName["weird_nan"]) {
		t.Errorf("NaN did not survive: %v", byName["weird_nan"])
	}
	if !math.IsInf(byName["weird_inf"], 1) {
		t.Errorf("+Inf did not survive: %v", byName["weird_inf"])
	}
	// One unreadable line must not cost the lines after it.
	if byName["after_the_broken_line"] != 7 {
		t.Error("a malformed line should be skipped, not end the parse")
	}
}

// ------------------------------------------------------------------- select ---

func TestSelectFillsWhatTheExporterCarried(t *testing.T) {
	samples, err := ParseExposition(strings.NewReader(sampleExposition(t)))
	if err != nil {
		t.Fatal(err)
	}

	var broker BrokerJMX
	Select(&broker, samples)

	if broker.HeapUsed <= 0 || broker.HeapMax <= 0 {
		t.Errorf("heap = %v / %v, want both filled", broker.HeapUsed, broker.HeapMax)
	}
	if fraction := broker.HeapFraction(); fraction <= 0 || fraction > 1 {
		t.Errorf("heap fraction = %v, want a ratio", fraction)
	}
	if !broker.Has("handler_idle") {
		t.Error("the request handler idle ratio should have been found")
	}
	if !broker.Has("gc") || broker.GCPause() <= 0 {
		t.Errorf("GC pause = %v, want the average of the collectors", broker.GCPause())
	}
	if broker.Leaders <= 0 || broker.Partitions <= 0 {
		t.Errorf("leaders %v of %v partitions, want both", broker.Leaders, broker.Partitions)
	}

	// A metric the exporter does not carry stays unknown rather than reading as
	// zero, which is what lets the panel say so.
	if broker.Has("nothing_like_this") {
		t.Error("an absent metric should not be marked present")
	}
}

func TestGCPauseAndHeapWithoutData(t *testing.T) {
	var empty BrokerJMX

	if got := empty.GCPause(); got != 0 {
		t.Errorf("GC pause without collections = %v, want 0", got)
	}
	if got := empty.HeapFraction(); got != 0 {
		t.Errorf("heap fraction without a maximum = %v, want 0", got)
	}
}

// Kafka's idle meters are documented as ratios but can read above 1; a gauge
// showing 200% idle would say nothing about the broker.
func TestIdlePercentSaturates(t *testing.T) {
	tests := []struct {
		ratio float64
		want  float64
	}{
		{ratio: 0, want: 0},
		{ratio: 0.925, want: 92.5},
		{ratio: 1, want: 100},
		{ratio: 2.0007, want: 100},
		{ratio: -1, want: 0},
		{ratio: math.NaN(), want: 0},
	}

	for _, test := range tests {
		if got := IdlePercent(test.ratio); got != test.want {
			t.Errorf("IdlePercent(%v) = %v, want %v", test.ratio, got, test.want)
		}
	}
}

// ------------------------------------------------------------------ scraper ---

func testCluster(targets ...string) *config.Cluster {
	cluster := &config.Cluster{Name: "test"}
	cluster.JMX.Enabled = len(targets) > 0
	for i, url := range targets {
		cluster.JMX.Targets = append(cluster.JMX.Targets,
			config.JMXTarget{BrokerID: int32(i + 1), URL: url})
	}
	return cluster
}

// A cluster without JMX has no scraper at all, which the caller reads as "not
// configured" rather than as a failure.
func TestNewScraperWithoutJMX(t *testing.T) {
	if s := NewScraper(nil, 0); s != nil {
		t.Error("a nil cluster should have no scraper")
	}
	if s := NewScraper(&config.Cluster{Name: "no-jmx"}, 0); s != nil {
		t.Error("a cluster with JMX disabled should have no scraper")
	}

	enabled := &config.Cluster{Name: "empty-targets"}
	enabled.JMX.Enabled = true
	if s := NewScraper(enabled, 0); s != nil {
		t.Error("enabled with no targets is still nothing to scrape")
	}
	if got := (*Scraper)(nil).Targets(); got != 0 {
		t.Errorf("a nil scraper has %d targets, want 0", got)
	}
	if got := (*Scraper)(nil).Scrape(context.Background()); got != nil {
		t.Error("a nil scraper should scrape nothing")
	}
}

func TestScrapeReadsEveryTarget(t *testing.T) {
	body := sampleExposition(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	scraper := NewScraper(testCluster(server.URL, server.URL), time.Second)
	results := scraper.Scrape(context.Background())

	if len(results) != 2 {
		t.Fatalf("got %d results, want one per target", len(results))
	}
	for _, result := range results {
		if !result.OK() {
			t.Errorf("broker %d: %v", result.BrokerID, result.Err)
		}
		if result.HeapUsed <= 0 {
			t.Errorf("broker %d reported no heap", result.BrokerID)
		}
	}
	// Ordered by broker id, so the panel does not shuffle between polls.
	if results[0].BrokerID != 1 || results[1].BrokerID != 2 {
		t.Errorf("results are not ordered by broker: %v %v", results[0].BrokerID, results[1].BrokerID)
	}
}

// One unreachable exporter must not cost the others, and the broker stays in
// the list carrying its error: "this one is unreachable" is worth showing.
func TestOneFailingTargetDoesNotHideTheOthers(t *testing.T) {
	body := sampleExposition(t)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer good.Close()

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()

	scraper := NewScraper(testCluster(good.URL, broken.URL, "http://127.0.0.1:1/metrics"), time.Second)
	results := scraper.Scrape(context.Background())

	if len(results) != 3 {
		t.Fatalf("got %d results, want one per target", len(results))
	}
	if !results[0].OK() {
		t.Errorf("the healthy target failed: %v", results[0].Err)
	}
	if results[1].OK() {
		t.Error("a 500 should be reported as a failure")
	} else if !strings.Contains(results[1].Err.Error(), "500") {
		t.Errorf("the error should name the status: %v", results[1].Err)
	}
	if results[2].OK() {
		t.Error("an unreachable endpoint should be reported as a failure")
	}
}

// A slow exporter must not hold up the dashboard.
func TestScrapeTimesOut(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()

	scraper := NewScraper(testCluster(slow.URL), 100*time.Millisecond)

	started := time.Now()
	results := scraper.Scrape(context.Background())
	elapsed := time.Since(started)

	if len(results) != 1 || results[0].OK() {
		t.Fatalf("a timeout should be reported as a failure: %+v", results)
	}
	if elapsed > 2*time.Second {
		t.Errorf("the scrape took %s; the timeout should have bounded it", elapsed)
	}
}

func TestScrapeHonoursContextCancellation(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	scraper := NewScraper(testCluster(slow.URL), 10*time.Second)
	results := scraper.Scrape(ctx)

	if len(results) != 1 || results[0].OK() {
		t.Fatalf("a cancelled scrape should not report success: %+v", results)
	}
}

// The collector keeps heap and handler-idle histories per broker: the JMX
// table draws them as sparklines.
func TestCollectorKeepsHeapAndIdleHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("jvm_memory_heap_used_bytes 4e9\n" +
			"jvm_memory_heap_max_bytes 8e9\n" +
			"kafka_server_requesthandlerpool_requesthandleravgidlepercent_oneminuterate 0.9\n"))
	}))
	t.Cleanup(server.Close)

	mock := kafka.NewMock(nil)
	cluster := mock.Cluster()
	cluster.JMX.Enabled = true
	cluster.JMX.Targets = []config.JMXTarget{{BrokerID: 1, URL: server.URL}}
	mock.ClusterFunc = func() *config.Cluster { return cluster }

	view := poll(t, NewCollector(mock, 5*time.Millisecond), 2)
	if len(view.JMX) != 1 {
		t.Fatalf("expected one broker, got %d", len(view.JMX))
	}
	broker := view.JMX[0]
	if len(broker.HeapSeries) == 0 {
		t.Error("the heap history should exist from the first scrape")
	}
	if len(broker.IdleSeries) == 0 {
		t.Error("the idle history should exist from the first scrape")
	}
	for _, p := range broker.IdleSeries {
		if p.Value < 0 || p.Value > 100 {
			t.Errorf("idle%% out of range: %v", p.Value)
		}
	}
}
