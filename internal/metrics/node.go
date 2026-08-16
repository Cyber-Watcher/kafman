package metrics

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// OS-level metrics from prometheus/node_exporter on the Kafka nodes. The
// exposition format and the scraping loop are shared with jmx.go; only the
// metric families differ.

// NodeOS is what one node's node_exporter reported. Counter fields are totals
// since boot; rates need two scrapes and are computed by the collector.
type NodeOS struct {
	BrokerID int32
	URL      string
	At       time.Time

	// Err is set when the endpoint could not be scraped; the node keeps its
	// row so "this node's exporter is down" stays visible.
	Err error

	// Counters, in seconds and bytes since boot.
	CPUTotalSeconds  float64 // node_cpu_seconds_total over every cpu and mode
	CPUIdleSeconds   float64 // the mode="idle" share of the same
	DiskReadBytes    float64 // node_disk_read_bytes_total over every device
	DiskWrittenBytes float64 // node_disk_written_bytes_total
	NetRxBytes       float64 // node_network_receive_bytes_total, device != "lo"
	NetTxBytes       float64 // node_network_transmit_bytes_total, device != "lo"

	// Gauges, valid from the first scrape.
	MemTotal     float64 // node_memory_MemTotal_bytes
	MemAvailable float64 // node_memory_MemAvailable_bytes
	Load1        float64 // node_load1
	Load5        float64
	Load15       float64

	// Filesystems the exporter reported, joined by mountpoint.
	Filesystems []FSStat

	// present marks which metric families the exporter actually exposed, so a
	// missing family renders as unknown rather than zero.
	present map[string]bool
}

// FSStat is one mounted filesystem the exporter reported.
type FSStat struct {
	Mountpoint string
	Device     string
	FSType     string
	SizeBytes  float64
	AvailBytes float64
}

// Has reports whether a metric family was present in the scrape: "cpu",
// "mem", "load", "disk", "net" or "fs".
func (n NodeOS) Has(field string) bool { return n.present[field] }

// OK is true when the scrape succeeded.
func (n NodeOS) OK() bool { return n.Err == nil }

// MemUsedFraction is 1 - available/total, or 0 when either side is missing.
func (n NodeOS) MemUsedFraction() float64 {
	if n.MemTotal <= 0 || n.MemAvailable < 0 {
		return 0
	}
	return 1 - n.MemAvailable/n.MemTotal
}

// FS returns the filesystem mounted exactly at mountpoint.
func (n NodeOS) FS(mountpoint string) (FSStat, bool) {
	for _, fs := range n.Filesystems {
		if fs.Mountpoint == mountpoint {
			return fs, true
		}
	}
	return FSStat{}, false
}

// UsedFraction is 1 - avail/size, or 0 when the size is missing.
func (f FSStat) UsedFraction() float64 {
	if f.SizeBytes <= 0 {
		return 0
	}
	return 1 - f.AvailBytes/f.SizeBytes
}

// SelectNode fills the fields kafman draws from a node_exporter scrape.
//
// The names are node_exporter's defaults; an exporter run with collectors
// disabled will simply not fill some fields, and the panel says which rather
// than showing zeroes.
func SelectNode(into *NodeOS, samples []Sample) {
	into.present = make(map[string]bool)

	type fsAgg struct {
		device, fstype  string
		size, avail     float64
		haveSize, haveA bool
	}
	filesystems := make(map[string]*fsAgg)
	fs := func(sample Sample) *fsAgg {
		mount := sample.Label("mountpoint")
		agg, ok := filesystems[mount]
		if !ok {
			agg = &fsAgg{device: sample.Label("device"), fstype: sample.Label("fstype")}
			filesystems[mount] = agg
		}
		return agg
	}

	for _, sample := range samples {
		switch sample.Name {
		case "node_cpu_seconds_total":
			into.CPUTotalSeconds += sample.Value
			if sample.Label("mode") == "idle" {
				into.CPUIdleSeconds += sample.Value
			}
			into.present["cpu"] = true
		case "node_memory_MemTotal_bytes":
			into.MemTotal = sample.Value
			into.present["mem"] = true
		case "node_memory_MemAvailable_bytes":
			into.MemAvailable = sample.Value
		case "node_load1":
			into.Load1 = sample.Value
			into.present["load"] = true
		case "node_load5":
			into.Load5 = sample.Value
		case "node_load15":
			into.Load15 = sample.Value
		case "node_disk_read_bytes_total":
			into.DiskReadBytes += sample.Value
			into.present["disk"] = true
		case "node_disk_written_bytes_total":
			into.DiskWrittenBytes += sample.Value
			into.present["disk"] = true
		case "node_network_receive_bytes_total":
			if sample.Label("device") != "lo" {
				into.NetRxBytes += sample.Value
				into.present["net"] = true
			}
		case "node_network_transmit_bytes_total":
			if sample.Label("device") != "lo" {
				into.NetTxBytes += sample.Value
				into.present["net"] = true
			}
		case "node_filesystem_size_bytes":
			agg := fs(sample)
			agg.size, agg.haveSize = sample.Value, true
		case "node_filesystem_avail_bytes":
			agg := fs(sample)
			agg.avail, agg.haveA = sample.Value, true
		}
	}

	for mount, agg := range filesystems {
		// A filesystem without a size (or with size 0, e.g. some virtual
		// mounts) has no usable usage fraction.
		if !agg.haveSize || !agg.haveA || agg.size <= 0 {
			continue
		}
		into.Filesystems = append(into.Filesystems, FSStat{
			Mountpoint: mount,
			Device:     agg.device,
			FSType:     agg.fstype,
			SizeBytes:  agg.size,
			AvailBytes: agg.avail,
		})
		into.present["fs"] = true
	}
	sort.Slice(into.Filesystems, func(i, j int) bool {
		return into.Filesystems[i].Mountpoint < into.Filesystems[j].Mountpoint
	})
}

// MatchDataDir finds the filesystem whose mountpoint is the longest
// path-prefix of any of the broker's log.dirs. ok is false when nothing
// matches or logDirs is empty — the UI must then say the data dir is unknown
// rather than invent a number.
func MatchDataDir(mounts []FSStat, logDirs []string) (fs FSStat, dir string, ok bool) {
	best := -1
	for _, d := range logDirs {
		for _, m := range mounts {
			if !pathHasPrefix(d, m.Mountpoint) {
				continue
			}
			if len(m.Mountpoint) > best {
				best, fs, dir, ok = len(m.Mountpoint), m, d, true
			}
		}
	}
	return fs, dir, ok
}

// pathHasPrefix reports whether dir lives under mount, matching whole path
// segments: /var/lib matches /var/lib/kafka but not /var/library.
func pathHasPrefix(dir, mount string) bool {
	if mount == "/" {
		return strings.HasPrefix(dir, "/")
	}
	return dir == mount || strings.HasPrefix(dir, mount+"/")
}

// NodeScraper reads the node_exporter endpoints of a cluster's nodes.
type NodeScraper struct {
	targets []config.NodeTarget
	client  *http.Client
}

// NewNodeScraper builds a scraper for the cluster's configured targets, or nil
// when node-exporter is not configured — the caller treats that as "no OS
// metrics", not as an error.
func NewNodeScraper(cluster *config.Cluster, timeout time.Duration) *NodeScraper {
	if cluster == nil || !cluster.NodeExporter.Enabled || len(cluster.NodeExporter.Targets) == 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = DefaultScrapeTimeout
	}

	return &NodeScraper{
		targets: cluster.NodeExporter.Targets,
		client:  &http.Client{Timeout: timeout},
	}
}

// Targets is how many endpoints the scraper knows about.
func (s *NodeScraper) Targets() int {
	if s == nil {
		return 0
	}
	return len(s.targets)
}

// Scrape reads every target concurrently and returns one result per node,
// ordered by broker id. A failed target keeps its row with Err set.
func (s *NodeScraper) Scrape(ctx context.Context) []NodeOS {
	if s == nil || len(s.targets) == 0 {
		return nil
	}

	endpoints := make([]endpoint, len(s.targets))
	for i, target := range s.targets {
		endpoints[i] = endpoint{brokerID: target.BrokerID, url: target.URL}
	}
	return scrapeAll(ctx, s.client, endpoints,
		func(ep endpoint, samples []Sample, err error) NodeOS {
			result := NodeOS{BrokerID: ep.brokerID, URL: ep.url, At: time.Now()}
			if err != nil {
				result.Err = err
				return result
			}
			SelectNode(&result, samples)
			return result
		})
}
