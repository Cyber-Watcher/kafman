package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// MaxWindow is the longest span the Monitoring tab can show, and therefore how
// much history the ring buffers have to hold.
const MaxWindow = time.Hour

// maxSamples caps the ring even at a very short poll interval: an hour at one
// second is 3600 samples per series, and nothing on screen can use more.
const maxSamples = 3600

// TopicLimit is how many topics keep their own series. The rest are still
// counted in the cluster total — a cluster can hold thousands of topics, and a
// series each would cost more memory than the numbers are worth.
const TopicLimit = 200

// Collector polls a cluster and keeps the series the Monitoring tab draws.
//
// It owns a goroutine and a ticker; the UI starts one per connection and stops
// it when the connection goes. Nothing here knows about bubbletea: the collector
// signals on a channel and the tab turns that into a message.
type Collector struct {
	client   kafka.Facade
	interval time.Duration

	mu sync.RWMutex

	cluster *Series
	written *Series
	topics  map[string]*Series
	groups  map[string]*Series
	states  map[string]string
	brokers []kafka.BrokerLoad
	sizes   map[int32]int64

	// Consumption, the mirror of the produce rates above: per-group read
	// rates from committed-offset deltas, their cluster-wide sum, member
	// counts and the never-committed partition counts.
	groupRead  map[string]*Series
	members    map[string]*Series
	noCommit   map[string]int
	prevCommit map[string]groupCommit
	consumed   *Series

	// Retention: how many records each topic keeps (end minus oldest) and how
	// fast the retention head advances — the deletion rate.
	topicHead  map[string]*Series
	retained   map[string]int64
	prevStarts map[string]int64

	// Replication health over time: cluster-wide counts of under-replicated
	// partitions and of partitions led by a non-preferred replica.
	urp          *Series
	notPreferred *Series

	// scraper is nil when the cluster has no JMX exporter configured, which is
	// a state the UI reports rather than an error.
	scraper *Scraper
	jmx     []BrokerJMX
	heap    map[int32]*Series
	idle    map[int32]*Series
	bytesIn map[int32]*Series

	// nodeScraper is the same arrangement for node_exporter on the nodes.
	// Counter families (CPU, disk, network) need two scrapes, so the previous
	// reading per node is kept alongside.
	nodeScraper *NodeScraper
	nodes       []NodeOS
	prevNodes   map[int32]NodeOS
	logDirs     map[int32][]string
	cpu         map[int32]*Series
	mem         map[int32]*Series
	netRx       map[int32]*Series
	netTx       map[int32]*Series
	diskRead    map[int32]*Series
	diskWrite   map[int32]*Series

	// previous readings, the other half of every delta.
	prevEnds  map[string]int64
	prevBytes int64
	prevAt    time.Time

	lastPoll time.Time
	lastErr  error
	polls    int

	updates chan struct{}
	poke    chan struct{}
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewCollector creates a collector polling every interval.
func NewCollector(client kafka.Facade, interval time.Duration) *Collector {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	c := &Collector{
		client:       client,
		interval:     interval,
		cluster:      NewSeries(capacityFor(interval)),
		written:      NewSeries(capacityFor(interval)),
		consumed:     NewSeries(capacityFor(interval)),
		urp:          NewSeries(capacityFor(interval)),
		notPreferred: NewSeries(capacityFor(interval)),
		topics:       make(map[string]*Series),
		groups:       make(map[string]*Series),
		states:       make(map[string]string),
		groupRead:    make(map[string]*Series),
		members:      make(map[string]*Series),
		noCommit:     make(map[string]int),
		prevCommit:   make(map[string]groupCommit),
		topicHead:    make(map[string]*Series),
		retained:     make(map[string]int64),
		prevStarts:   make(map[string]int64),
		sizes:        make(map[int32]int64),
		heap:         make(map[int32]*Series),
		idle:         make(map[int32]*Series),
		bytesIn:      make(map[int32]*Series),
		prevNodes:    make(map[int32]NodeOS),
		cpu:          make(map[int32]*Series),
		mem:          make(map[int32]*Series),
		netRx:        make(map[int32]*Series),
		netTx:        make(map[int32]*Series),
		diskRead:     make(map[int32]*Series),
		diskWrite:    make(map[int32]*Series),
		updates:      make(chan struct{}, 1),
		poke:         make(chan struct{}, 1),
		done:         make(chan struct{}),
	}

	// The scrape budget is the poll interval, capped: a dashboard that polls
	// every five seconds cannot wait ten for an exporter.
	if client != nil {
		c.scraper = NewScraper(client.Cluster(), min(interval, DefaultScrapeTimeout))
		c.nodeScraper = NewNodeScraper(client.Cluster(), min(interval, DefaultScrapeTimeout))
	}
	return c
}

// HasJMX reports whether this cluster has a JMX exporter configured at all.
func (c *Collector) HasJMX() bool { return c.scraper.Targets() > 0 }

// capacityFor is how many samples cover MaxWindow at this interval.
func capacityFor(interval time.Duration) int {
	samples := int(MaxWindow/interval) + 2
	return min(max(samples, 8), maxSamples)
}

// Interval is how often the collector polls.
func (c *Collector) Interval() time.Duration { return c.interval }

// Updates signals that a poll finished. It carries no data: the reader takes a
// snapshot when it is ready to draw, which keeps a slow renderer from making the
// collector wait.
func (c *Collector) Updates() <-chan struct{} { return c.updates }

// Start begins polling. The first poll happens immediately, because a dashboard
// that is blank for the first interval looks broken.
//
// Start is idempotent: the tab's Init runs on every switch onto it, and a
// second Start would add a second polling goroutine whose immediate poll
// resets every rate baseline.
func (c *Collector) Start(ctx context.Context) {
	if c.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		defer close(c.done)

		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		c.poll(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.poll(ctx)
			case <-c.poke:
				c.poll(ctx)
				// The cadence restarts from the manual poll, so a tick that was
				// almost due does not fire immediately afterwards and sample an
				// interval that is not one.
				ticker.Reset(c.interval)
			}
		}
	}()
}

// Stop ends the polling goroutine and waits for it.
func (c *Collector) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	<-c.done
}

// Refresh asks for a poll now, without waiting for the tick. A second request
// while one is pending is dropped: the answer would be the same.
func (c *Collector) Refresh() {
	select {
	case c.poke <- struct{}{}:
	default:
	}
}

// poll takes one reading of everything and turns it into rates.
//
// Every source is handled on its own: a cluster where log dirs are not
// describable still produces message rates, and a failure to list groups does
// not blank the throughput chart.
func (c *Collector) poll(ctx context.Context) {
	at := time.Now()

	// Ends first: TopicStartOffsets reuses the metadata this refreshes.
	ends, endsErr := c.client.TopicEndOffsets(ctx, false)
	starts, startsErr := c.client.TopicStartOffsets(ctx, false)
	sizes, sizesErr := c.client.BrokerSizes(ctx)
	groups, groupsErr := c.client.ConsumerGroupsWithLag(ctx)
	brokers, brokersErr := c.client.BrokerLoads(ctx)

	// The exporters are HTTP endpoints on the brokers, unrelated to the Kafka
	// protocol: they are scraped alongside, and one that is down costs its own
	// row and nothing else.
	jmx := c.scraper.Scrape(ctx)
	nodes := c.nodeScraper.Scrape(ctx)

	// log.dirs is what the data-dir gauge in the OS section is matched
	// against. Fetched once (the kafka layer caches it anyway) and retried on
	// the next poll after a failure.
	var logDirs map[int32][]string
	if c.nodeScraper.Targets() > 0 && !c.hasLogDirs() {
		logDirs, _ = c.client.BrokerLogDirs(ctx)
	}

	if ctx.Err() != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	elapsed := at.Sub(c.prevAt)
	first := c.prevAt.IsZero()

	// A rate over a fraction of a second says more about when the poll landed
	// than about the cluster: one record either side of the boundary swings it
	// by hundreds per second. Such a reading updates the baseline but adds no
	// sample, so pressing F5 twice cannot punch a hole in the chart.
	rateWorthKeeping := !first && elapsed >= c.interval/4

	if endsErr == nil {
		if rateWorthKeeping {
			c.recordRates(at, ends, elapsed)
		}
		if startsErr == nil {
			c.recordRetention(at, ends, starts, elapsed, rateWorthKeeping)
		}
		c.prevEnds = ends
	}

	if sizesErr == nil {
		var total int64
		for _, size := range sizes {
			total += size
		}
		if rateWorthKeeping {
			c.written.Add(at, Rate(c.prevBytes, total, elapsed))
		}
		c.prevBytes, c.sizes = total, sizes
	}

	if groupsErr == nil {
		c.recordGroups(at, groups, elapsed, rateWorthKeeping)
	}
	if brokersErr == nil {
		c.brokers = brokers

		// Replication health as gauges, valid from the first poll. Each
		// partition has exactly one leader, so the sums are honest totals.
		var urp, notPreferred int
		for _, broker := range brokers {
			urp += broker.UnderReplicated
			notPreferred += broker.NotPreferred
		}
		c.urp.Add(at, float64(urp))
		c.notPreferred.Add(at, float64(notPreferred))
	}
	if jmx != nil {
		c.recordJMX(at, jmx)
	}
	if len(logDirs) > 0 {
		c.logDirs = logDirs
	}
	if nodes != nil {
		c.recordNodes(at, nodes)
	}

	c.prevAt = at
	c.lastPoll = at
	c.lastErr = firstError(endsErr, startsErr, sizesErr, groupsErr, brokersErr)
	c.polls++

	if c.lastErr != nil {
		slog.Debug("metrics poll incomplete", "error", c.lastErr)
	}

	select {
	case c.updates <- struct{}{}:
	default:
	}
}

// recordRates turns end-offset readings into per-topic and cluster message
// rates.
func (c *Collector) recordRates(at time.Time, ends map[string]int64, elapsed time.Duration) {
	var clusterRate float64
	for topic, end := range ends {
		// A topic missing from the previous reading has no delta to measure: it
		// was created since, or its partitions had no leader to answer. Treating
		// the absent reading as zero would turn its whole retained history into
		// one poll's worth of traffic — a spike that never happened.
		previous, known := c.prevEnds[topic]
		if !known {
			continue
		}

		rate := Rate(previous, end, elapsed)
		clusterRate += rate

		series, ok := c.topics[topic]
		if !ok {
			if len(c.topics) >= TopicLimit {
				continue
			}
			series = NewSeries(capacityFor(c.interval))
			c.topics[topic] = series
		}
		series.Add(at, rate)
	}

	// A topic that disappeared keeps its series until it ages out of the window,
	// so a chart drawn just after a deletion does not lose its history.
	c.cluster.Add(at, clusterRate)
}

// groupCommit is the read-rate baseline: the committed sum and how many
// partitions produced it.
type groupCommit struct {
	sum   int64
	parts int
}

func (c *Collector) recordGroups(at time.Time, groups []kafka.ConsumerGroup, elapsed time.Duration, rateWorthKeeping bool) {
	seen := make(map[string]bool, len(groups))
	namedSeries := func(m map[string]*Series, name string) *Series {
		series, ok := m[name]
		if !ok {
			series = NewSeries(capacityFor(c.interval))
			m[name] = series
		}
		return series
	}

	var consumedTotal float64
	consumedAny := false

	for _, group := range groups {
		seen[group.Name] = true
		c.states[group.Name] = group.State
		c.noCommit[group.Name] = group.NoCommit

		// Member count is a gauge: its history starts with the first poll, and
		// a drop to zero while the lag climbs is the dead-consumer signal.
		namedSeries(c.members, group.Name).Add(at, float64(group.Members))

		if group.LagKnown {
			namedSeries(c.groups, group.Name).Add(at, float64(group.TotalLag))
		}

		// The read rate is the growth of the group's committed-offset sum. Two
		// honesty rules: a changed partition count means the sum jumped for a
		// structural reason (a new topic committed on, offsets expired) — the
		// baseline moves but no sample lands; and a sum that went backwards (an
		// offset reset) reads as zero via Rate's clamp, because records were
		// not un-read.
		if group.CommittedKnown {
			prev, ok := c.prevCommit[group.Name]
			if ok && rateWorthKeeping && prev.parts == group.CommittedParts {
				rate := Rate(prev.sum, group.CommittedSum, elapsed)
				namedSeries(c.groupRead, group.Name).Add(at, rate)
				consumedTotal += rate
				consumedAny = true
			}
			c.prevCommit[group.Name] = groupCommit{sum: group.CommittedSum, parts: group.CommittedParts}
		}
	}

	if consumedAny {
		c.consumed.Add(at, consumedTotal)
	}

	// A group that disappeared takes its whole history with it.
	for name := range c.states {
		if !seen[name] {
			delete(c.groups, name)
			delete(c.states, name)
			delete(c.groupRead, name)
			delete(c.members, name)
			delete(c.prevCommit, name)
			delete(c.noCommit, name)
		}
	}
}

// recordRetention keeps each topic's retained record count (a gauge) and the
// advance of its retention head — the deletion rate.
func (c *Collector) recordRetention(at time.Time, ends, starts map[string]int64, elapsed time.Duration, rateWorthKeeping bool) {
	for topic, start := range starts {
		if end, ok := ends[topic]; ok {
			c.retained[topic] = max(end-start, 0)
		}

		previous, known := c.prevStarts[topic]
		c.prevStarts[topic] = start
		if !known || !rateWorthKeeping {
			continue
		}
		series, ok := c.topicHead[topic]
		if !ok {
			// The same admission rule as the throughput series: a cluster can
			// hold thousands of topics, and a series each costs more memory
			// than the numbers are worth.
			if len(c.topicHead) >= TopicLimit {
				continue
			}
			series = NewSeries(capacityFor(c.interval))
			c.topicHead[topic] = series
		}
		series.Add(at, Rate(previous, start, elapsed))
	}

	// A topic that vanished takes its retention numbers with it.
	for topic := range c.retained {
		if _, ok := starts[topic]; !ok {
			delete(c.retained, topic)
			delete(c.prevStarts, topic)
			delete(c.topicHead, topic)
		}
	}
}

// recordJMX keeps the latest scrape and the series drawn from it.
//
// Heap and bytes-in are the two worth a shape over time; the rest are read as
// current values, because a number that only matters right now does not need a
// history.
func (c *Collector) recordJMX(at time.Time, jmx []BrokerJMX) {
	c.jmx = jmx

	for _, broker := range jmx {
		if !broker.OK() {
			continue
		}

		if broker.Has("heap") && broker.HeapMax > 0 {
			series, ok := c.heap[broker.BrokerID]
			if !ok {
				series = NewSeries(capacityFor(c.interval))
				c.heap[broker.BrokerID] = series
			}
			series.Add(at, broker.HeapFraction())
		}

		if broker.Has("handler_idle") {
			series, ok := c.idle[broker.BrokerID]
			if !ok {
				series = NewSeries(capacityFor(c.interval))
				c.idle[broker.BrokerID] = series
			}
			series.Add(at, IdlePercent(broker.HandlerIdle))
		}

		if broker.Has("bytes_in") {
			series, ok := c.bytesIn[broker.BrokerID]
			if !ok {
				series = NewSeries(capacityFor(c.interval))
				c.bytesIn[broker.BrokerID] = series
			}
			series.Add(at, broker.BytesIn)
		}
	}
}

// hasLogDirs is read outside the poll lock, before the fetch it guards.
func (c *Collector) hasLogDirs() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.logDirs) > 0
}

// recordNodes keeps the latest node_exporter scrape and the series drawn from
// it. CPU, network and disk are counters since boot: the first scrape of a
// node is only a baseline, and a sample lands from the second one on. Gauges
// (memory, load, filesystems) are current values and need no history.
func (c *Collector) recordNodes(at time.Time, nodes []NodeOS) {
	c.nodes = nodes

	nodeSeries := func(m map[int32]*Series, id int32) *Series {
		series, ok := m[id]
		if !ok {
			series = NewSeries(capacityFor(c.interval))
			m[id] = series
		}
		return series
	}

	for _, node := range nodes {
		if !node.OK() {
			continue
		}

		// Memory is a gauge, so its history starts with the first scrape.
		if node.Has("mem") {
			nodeSeries(c.mem, node.BrokerID).Add(at, node.MemUsedFraction()*100)
		}

		previous, known := c.prevNodes[node.BrokerID]
		c.prevNodes[node.BrokerID] = node

		// The same guard as rateWorthKeeping: a delta over a fraction of the
		// interval says more about when the polls landed than about the node.
		if !known {
			continue
		}
		elapsed := node.At.Sub(previous.At)
		if elapsed < c.interval/4 {
			continue
		}

		if node.Has("cpu") && previous.Has("cpu") {
			// Busy share of the elapsed CPU time across every core. A counter
			// reset (reboot) makes the totals go backwards; RateF reports both
			// deltas as zero and the guard below skips the sample entirely.
			total := RateF(previous.CPUTotalSeconds, node.CPUTotalSeconds, elapsed)
			idle := RateF(previous.CPUIdleSeconds, node.CPUIdleSeconds, elapsed)
			if total > 0 {
				percent := 100 * (1 - idle/total)
				nodeSeries(c.cpu, node.BrokerID).Add(at, min(max(percent, 0), 100))
			}
		}
		if node.Has("net") && previous.Has("net") {
			nodeSeries(c.netRx, node.BrokerID).Add(at, RateF(previous.NetRxBytes, node.NetRxBytes, elapsed))
			nodeSeries(c.netTx, node.BrokerID).Add(at, RateF(previous.NetTxBytes, node.NetTxBytes, elapsed))
		}
		if node.Has("disk") && previous.Has("disk") {
			nodeSeries(c.diskRead, node.BrokerID).Add(at, RateF(previous.DiskReadBytes, node.DiskReadBytes, elapsed))
			nodeSeries(c.diskWrite, node.BrokerID).Add(at, RateF(previous.DiskWrittenBytes, node.DiskWrittenBytes, elapsed))
		}
	}
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------------------ reading ---

// View is a rendering-ready copy of the series over one window.
//
// The collector writes from its own goroutine, so callers get a copy rather than
// a pointer into live state; a window is a few hundred floats, which is cheaper
// than holding a lock across a render.
type View struct {
	At       time.Time
	Window   time.Duration
	Interval time.Duration
	Polls    int
	Err      error

	// Cluster is the message rate over the whole cluster, Written the estimated
	// disk growth in bytes per second, Consumed the summed read rate of every
	// consumer group.
	Cluster  []Point
	Written  []Point
	Consumed []Point

	// URP and NotPreferred are cluster-wide partition counts over the window:
	// under-replicated, and led by a non-preferred replica.
	URP          []Point
	NotPreferred []Point

	Topics []NamedSeries
	Groups []GroupSeries

	Brokers []BrokerStat

	// JMXConfigured says whether the cluster has an exporter at all, which is a
	// different statement from "the scrape returned nothing".
	JMXConfigured bool
	JMX           []JMXStat

	// NodesConfigured is the same statement for node_exporter.
	NodesConfigured bool
	Nodes           []NodeStat
}

// JMXStat is one broker's exporter reading with the series drawn from it.
//
// The series are named apart from the embedded readings on purpose: a field
// called BytesIn would shadow the current value it is the history of.
type JMXStat struct {
	BrokerJMX
	HeapSeries    []Point // fraction of the heap, 0..1
	IdleSeries    []Point // handler idle percent, 0..100
	BytesInSeries []Point
}

// NodeStat is one node's exporter reading with the series drawn from it.
//
// CPUSeries is a percentage; the byte series are rates per second. All of
// them are empty until the second scrape, because a counter needs a delta.
type NodeStat struct {
	NodeOS
	CPUSeries       []Point
	RAMSeries       []Point // used percent, 0..100; a gauge, so from the first scrape
	NetRxSeries     []Point
	NetTxSeries     []Point
	DiskReadSeries  []Point
	DiskWriteSeries []Point

	// DataDirPath is the broker's log.dirs entry and DataDirFS the filesystem
	// it lives on. DataDirKnown false means the pair could not be resolved —
	// the screen says "unknown" rather than inventing a number.
	DataDirKnown bool
	DataDirPath  string
	DataDirFS    FSStat
}

// NamedSeries is one topic's rate over the window.
type NamedSeries struct {
	Name   string
	Points []Point
	Last   float64

	// Retained is how many records the topic currently keeps (end minus
	// oldest offset); Head the deletion rate — how fast the retention head
	// advances. HeadLast is the last Head sample.
	Retained      int64
	RetainedKnown bool
	Head          []Point
	HeadLast      float64
}

// GroupSeries is one consumer group's lag over the window.
type GroupSeries struct {
	Name   string
	State  string
	Points []Point
	Last   float64
	Trend  Trend

	// Read is the group's consumption rate from committed-offset deltas;
	// empty until the second poll, and ReadKnown false renders as "—", never
	// as zero. Members is the member count over the window, MembersNow its
	// latest value, NoCommit how many assigned partitions carry no commit.
	Read       []Point
	ReadLast   float64
	ReadKnown  bool
	Members    []Point
	MembersNow int
	NoCommit   int
}

// BrokerStat is one broker's share of the cluster.
type BrokerStat struct {
	kafka.BrokerLoad
	Bytes int64
}

// Ready reports whether at least one rate has been measured. The first poll only
// takes a baseline, so a view can be legitimately empty for one interval.
func (v View) Ready() bool { return len(v.Cluster) > 0 }

// View copies the series covering the last window.
func (c *Collector) View(window time.Duration) View {
	c.mu.RLock()
	defer c.mu.RUnlock()

	view := View{
		At:           c.lastPoll,
		Window:       window,
		Interval:     c.interval,
		Polls:        c.polls,
		Err:          c.lastErr,
		Cluster:      c.cluster.Window(window),
		Written:      c.written.Window(window),
		Consumed:     c.consumed.Window(window),
		URP:          c.urp.Window(window),
		NotPreferred: c.notPreferred.Window(window),
	}

	for _, name := range TopSeries(c.topics, 0) {
		points := c.topics[name].Window(window)
		if len(points) == 0 {
			continue
		}
		entry := NamedSeries{
			Name:   name,
			Points: points,
			Last:   points[len(points)-1].Value,
		}
		if retained, ok := c.retained[name]; ok {
			entry.Retained, entry.RetainedKnown = retained, true
		}
		if series, ok := c.topicHead[name]; ok {
			entry.Head = series.Window(window)
			if len(entry.Head) > 0 {
				entry.HeadLast = entry.Head[len(entry.Head)-1].Value
			}
		}
		view.Topics = append(view.Topics, entry)
	}

	for _, name := range TopSeries(c.groups, 0) {
		points := c.groups[name].Window(window)
		if len(points) == 0 {
			continue
		}
		entry := GroupSeries{
			Name:     name,
			State:    c.states[name],
			Points:   points,
			Last:     points[len(points)-1].Value,
			Trend:    TrendOf(points),
			NoCommit: c.noCommit[name],
		}
		if series, ok := c.groupRead[name]; ok {
			entry.Read = series.Window(window)
			if len(entry.Read) > 0 {
				entry.ReadLast = entry.Read[len(entry.Read)-1].Value
				entry.ReadKnown = true
			}
		}
		if series, ok := c.members[name]; ok {
			entry.Members = series.Window(window)
			if len(entry.Members) > 0 {
				entry.MembersNow = int(entry.Members[len(entry.Members)-1].Value)
			}
		}
		view.Groups = append(view.Groups, entry)
	}

	for _, broker := range c.brokers {
		view.Brokers = append(view.Brokers, BrokerStat{
			BrokerLoad: broker,
			Bytes:      c.sizes[broker.ID],
		})
	}

	view.JMXConfigured = c.scraper.Targets() > 0
	for _, broker := range c.jmx {
		stat := JMXStat{BrokerJMX: broker}
		if series, ok := c.heap[broker.BrokerID]; ok {
			stat.HeapSeries = series.Window(window)
		}
		if series, ok := c.idle[broker.BrokerID]; ok {
			stat.IdleSeries = series.Window(window)
		}
		if series, ok := c.bytesIn[broker.BrokerID]; ok {
			stat.BytesInSeries = series.Window(window)
		}
		view.JMX = append(view.JMX, stat)
	}

	view.NodesConfigured = c.nodeScraper.Targets() > 0
	for _, node := range c.nodes {
		stat := NodeStat{NodeOS: node}
		if series, ok := c.cpu[node.BrokerID]; ok {
			stat.CPUSeries = series.Window(window)
		}
		if series, ok := c.mem[node.BrokerID]; ok {
			stat.RAMSeries = series.Window(window)
		}
		if series, ok := c.netRx[node.BrokerID]; ok {
			stat.NetRxSeries = series.Window(window)
		}
		if series, ok := c.netTx[node.BrokerID]; ok {
			stat.NetTxSeries = series.Window(window)
		}
		if series, ok := c.diskRead[node.BrokerID]; ok {
			stat.DiskReadSeries = series.Window(window)
		}
		if series, ok := c.diskWrite[node.BrokerID]; ok {
			stat.DiskWriteSeries = series.Window(window)
		}
		if fs, dir, ok := MatchDataDir(node.Filesystems, c.logDirs[node.BrokerID]); ok {
			stat.DataDirKnown, stat.DataDirFS, stat.DataDirPath = true, fs, dir
		}
		view.Nodes = append(view.Nodes, stat)
	}

	return view
}
