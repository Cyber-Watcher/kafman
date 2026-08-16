package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// DefaultScrapeTimeout bounds one scrape. A broker under load answers its
// metrics endpoint slowly, and the dashboard would rather show the previous
// reading than block the whole poll behind it.
const DefaultScrapeTimeout = 3 * time.Second

// Sample is one line of the Prometheus text exposition format.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Label returns one label, or "" when it is absent.
func (s Sample) Label(name string) string { return s.Labels[name] }

// ParseExposition reads the Prometheus text format.
//
// Only what a jmx_exporter emits is handled: gauges, counters and untyped
// samples. Histograms and summaries arrive as their component series
// (`_bucket`, `_sum`, `_count`) and are readable as plain samples, which is all
// kafman needs — it never aggregates them.
func ParseExposition(r io.Reader) ([]Sample, error) {
	scanner := bufio.NewScanner(r)
	// jmx_exporter lines are short, but a metric with many labels can be long;
	// the default 64 KB token limit is raised so one such line cannot truncate
	// the whole scrape.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var samples []Sample
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// HELP and TYPE carry no values; skipping them is what makes this a
		// reader of samples rather than of metadata.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		sample, ok := parseSample(line)
		if !ok {
			continue
		}
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read exposition: %w", err)
	}
	return samples, nil
}

// parseSample reads one `name{label="value",...} number [timestamp]` line.
//
// A line that cannot be read is skipped rather than failing the scrape: one
// malformed metric among two thousand should not blank a dashboard.
func parseSample(line string) (Sample, bool) {
	if open := strings.IndexByte(line, '{'); open >= 0 {
		shut := strings.LastIndexByte(line, '}')
		if shut < open {
			return Sample{}, false
		}

		sample := Sample{
			Name:   strings.TrimSpace(line[:open]),
			Labels: parseLabels(line[open+1 : shut]),
		}
		value, ok := parseValue(strings.TrimSpace(line[shut+1:]))
		if !ok {
			return Sample{}, false
		}
		sample.Value = value
		return sample, sample.Name != ""
	}

	fields := strings.Fields(line)
	if len(fields) < 2 {
		return Sample{}, false
	}
	value, ok := parseValue(strings.Join(fields[1:], " "))
	if !ok {
		return Sample{}, false
	}
	return Sample{Name: fields[0], Value: value}, true
}

// parseValue reads the number, ignoring an optional trailing timestamp.
func parseValue(rest string) (float64, bool) {
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return 0, false
	}

	// Go parses "3.9845888E8", "+Inf" and "NaN" the way the format defines them.
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// parseLabels reads `a="1",b="2"`, honouring the format's escapes.
func parseLabels(text string) map[string]string {
	labels := make(map[string]string)

	for len(text) > 0 {
		equals := strings.IndexByte(text, '=')
		if equals < 0 {
			break
		}
		key := strings.TrimSpace(strings.Trim(text[:equals], ", "))
		text = text[equals+1:]

		text = strings.TrimSpace(text)
		if len(text) == 0 || text[0] != '"' {
			break
		}
		text = text[1:]

		var value strings.Builder
		escaped := false
		closed := false
		for i := 0; i < len(text); i++ {
			c := text[i]
			switch {
			case escaped:
				// The format escapes only these three; anything else keeps its
				// backslash, which is what a JMX object name can contain.
				switch c {
				case 'n':
					value.WriteByte('\n')
				case '"', '\\':
					value.WriteByte(c)
				default:
					value.WriteByte('\\')
					value.WriteByte(c)
				}
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				text = text[i+1:]
				closed = true
			default:
				value.WriteByte(c)
			}
			if closed {
				break
			}
		}
		if !closed {
			break
		}
		if key != "" {
			labels[key] = value.String()
		}
		text = strings.TrimLeft(text, ", ")
	}

	if len(labels) == 0 {
		return nil
	}
	return labels
}

// ------------------------------------------------------------------ scraping ---

// BrokerJMX is what one broker's exporter reported.
//
// Every field is a value the exporter actually carried; a metric the exporter
// does not expose stays at its zero and is reported as unknown rather than
// drawn as zero.
type BrokerJMX struct {
	BrokerID int32
	URL      string
	At       time.Time
	Err      error

	HeapUsed    float64
	HeapMax     float64
	NonHeapUsed float64

	// GCCount and GCSeconds are totals since start, summed over collectors;
	// GCPause is their quotient, the average pause.
	GCCount   float64
	GCSeconds float64

	// HandlerIdle and NetworkIdle are ratios in 0..1 — the classic saturation
	// signals. Kafka's own meters can report slightly above 1, so read them
	// through IdlePercent.
	HandlerIdle float64
	NetworkIdle float64

	BytesIn    float64
	BytesOut   float64
	MessagesIn float64

	UnderReplicated float64
	OfflineReplicas float64
	IsrShrinks      float64
	Leaders         float64
	Partitions      float64

	CPULoad float64

	// present marks the metrics the exporter actually carried.
	present map[string]bool
}

// Has reports whether a metric was present in the scrape.
func (b BrokerJMX) Has(field string) bool { return b.present[field] }

// OK reports whether the scrape succeeded.
func (b BrokerJMX) OK() bool { return b.Err == nil }

// HeapFraction is heap used over heap max, or 0 when either is missing.
func (b BrokerJMX) HeapFraction() float64 {
	if b.HeapMax <= 0 {
		return 0
	}
	return b.HeapUsed / b.HeapMax
}

// GCPause is the average pause per collection, or 0 when nothing was collected.
func (b BrokerJMX) GCPause() time.Duration {
	if b.GCCount <= 0 {
		return 0
	}
	return time.Duration(b.GCSeconds / b.GCCount * float64(time.Second))
}

// IdlePercent turns an idle ratio into a percentage.
//
// Kafka's idle meters are documented as 0..1 but can read above 1: the meter is
// fed from several handler threads and its rate is not renormalised. The
// percentage saturates at 100 rather than showing 200% idle, which would say
// nothing about the broker.
func IdlePercent(ratio float64) float64 {
	if math.IsNaN(ratio) || ratio < 0 {
		return 0
	}
	return math.Min(ratio, 1) * 100
}

// Scraper reads the jmx_exporter endpoints of a cluster.
type Scraper struct {
	targets []config.JMXTarget
	client  *http.Client
}

// NewScraper builds a scraper for the cluster's configured targets, or nil when
// JMX is not configured — the caller treats that as "no JMX", not as an error.
func NewScraper(cluster *config.Cluster, timeout time.Duration) *Scraper {
	if cluster == nil || !cluster.JMX.Enabled || len(cluster.JMX.Targets) == 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = DefaultScrapeTimeout
	}

	return &Scraper{
		targets: cluster.JMX.Targets,
		client:  &http.Client{Timeout: timeout},
	}
}

// Targets is how many endpoints the scraper knows about.
func (s *Scraper) Targets() int {
	if s == nil {
		return 0
	}
	return len(s.targets)
}

// Scrape reads every target concurrently and returns one result per broker,
// ordered by broker id.
//
// A target that fails contributes its error rather than removing the broker
// from the list: "this broker's exporter is unreachable" is itself worth
// showing.
func (s *Scraper) Scrape(ctx context.Context) []BrokerJMX {
	if s == nil || len(s.targets) == 0 {
		return nil
	}

	endpoints := make([]endpoint, len(s.targets))
	for i, target := range s.targets {
		endpoints[i] = endpoint{brokerID: target.BrokerID, url: target.URL}
	}
	return scrapeAll(ctx, s.client, endpoints,
		func(ep endpoint, samples []Sample, err error) BrokerJMX {
			result := BrokerJMX{BrokerID: ep.brokerID, URL: ep.url, At: time.Now()}
			if err != nil {
				result.Err = err
				return result
			}
			Select(&result, samples)
			return result
		})
}

// endpoint is the broker-id/url pair both exporter scrapers share.
type endpoint struct {
	brokerID int32
	url      string
}

// scrapeAll fetches every endpoint concurrently, parses the exposition and
// hands each result — samples or a fetch error — to build. Results come back
// ordered by broker id.
func scrapeAll[T any](ctx context.Context, client *http.Client, endpoints []endpoint,
	build func(ep endpoint, samples []Sample, err error) T) []T {

	sorted := make([]endpoint, len(endpoints))
	copy(sorted, endpoints)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].brokerID < sorted[j].brokerID })

	results := make([]T, len(sorted))
	done := make(chan struct{}, len(sorted))
	for i, ep := range sorted {
		go func() {
			samples, err := fetchExposition(ctx, client, ep.url)
			results[i] = build(ep, samples, err)
			done <- struct{}{}
		}()
	}
	for range sorted {
		<-done
	}
	return results
}

func fetchExposition(ctx context.Context, client *http.Client, url string) ([]Sample, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", url, err)
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", url, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: %s", url, response.Status)
	}

	samples, err := ParseExposition(response.Body)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", url, err)
	}
	return samples, nil
}

// Select fills the fields kafman draws from a scrape's samples.
//
// The names are jmx_exporter's, produced by docker/jmx-exporter.yml; a
// deployment with different rules will simply not fill some fields, and the
// panel says which rather than showing zeroes.
func Select(into *BrokerJMX, samples []Sample) {
	into.present = make(map[string]bool)

	set := func(field string, target *float64, value float64) {
		*target = value
		into.present[field] = true
	}

	for _, sample := range samples {
		switch sample.Name {
		case "jvm_memory_heap_used_bytes":
			set("heap", &into.HeapUsed, sample.Value)
		case "jvm_memory_heap_max_bytes":
			into.HeapMax = sample.Value
		case "jvm_memory_nonheap_used_bytes":
			into.NonHeapUsed = sample.Value

		// GC is exposed per collector; the totals are their sum, which is what
		// an average pause has to be computed from.
		case "jvm_gc_collection_seconds_count":
			into.GCCount += sample.Value
			into.present["gc"] = true
		case "jvm_gc_collection_seconds_sum":
			into.GCSeconds += sample.Value

		case "kafka_server_requesthandlerpool_requesthandleravgidlepercent_oneminuterate":
			set("handler_idle", &into.HandlerIdle, sample.Value)
		case "kafka_network_socketserver_networkprocessoravgidlepercent":
			set("network_idle", &into.NetworkIdle, sample.Value)

		case "kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate":
			set("bytes_in", &into.BytesIn, sample.Value)
		case "kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate":
			set("bytes_out", &into.BytesOut, sample.Value)
		case "kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate":
			set("messages_in", &into.MessagesIn, sample.Value)

		case "kafka_server_replicamanager_underreplicatedpartitions_value":
			set("under_replicated", &into.UnderReplicated, sample.Value)
		case "kafka_server_replicamanager_offlinereplicacount_value":
			set("offline", &into.OfflineReplicas, sample.Value)
		case "kafka_server_replicamanager_isrshrinkspersec_count":
			set("isr_shrinks", &into.IsrShrinks, sample.Value)
		case "kafka_server_replicamanager_leadercount_value":
			set("leaders", &into.Leaders, sample.Value)
		case "kafka_server_replicamanager_partitioncount_value":
			set("partitions", &into.Partitions, sample.Value)

		case "jvm_os_processcpuload":
			set("cpu", &into.CPULoad, sample.Value)
		}
	}
}
