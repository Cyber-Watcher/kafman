package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// batchWindow is how long records are collected before being handed to the UI.
//
// Without it a busy topic delivers thousands of messages a second straight into
// the render loop and the interface stops responding. 50 ms is short enough to
// feel live and long enough to coalesce a burst. Borrowed from ktea.
const batchWindow = 50 * time.Millisecond

// StartKind is where consumption begins.
type StartKind int

// Starting positions.
const (
	// StartEnd reads only what arrives from now on.
	StartEnd StartKind = iota
	// StartBeginning reads everything still retained.
	StartBeginning
	// StartLastN reads the last N records of each partition.
	StartLastN
	// StartOffset reads from an explicit offset.
	StartOffset
	// StartTime reads from the first record at or after a timestamp.
	StartTime
)

// Start describes where to begin reading.
type Start struct {
	Kind   StartKind
	N      int64
	Offset int64
	Time   time.Time
}

// ReadOptions configures a read.
type ReadOptions struct {
	Topics []string

	// Partitions restricts the read; empty means every partition.
	Partitions []int32

	Start Start

	// Limit stops the read after this many records. Zero means no limit.
	Limit int

	// Follow keeps the consumer running once it reaches the end. When false the
	// read finishes at the high watermark it saw when it started.
	Follow bool
}

// Header is one record header.
type Header struct {
	Key   string
	Value string
}

// Record is one Kafka record.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time

	// Size is the payload size in bytes: key plus value.
	Size int
}

// Batch is a group of records delivered together, with the running totals so
// the UI can show "matched / read" without counting them itself.
type Batch struct {
	Records []Record

	// Read is how many records have been consumed in total, matched how many
	// passed the filter. Until the query language lands they are equal.
	Read    int64
	Matched int64
}

// ReadSession is a running consumption. Records arrives in batches; the channel
// closes when the read finishes, is cancelled, or hits its limit.
type ReadSession struct {
	Records <-chan Batch

	// Err holds the reason the session ended, if it was not a clean finish.
	// Read it only after Records has closed.
	Err func() error

	cancel context.CancelFunc
}

// Stop ends the session. Safe to call more than once.
func (s *ReadSession) Stop() { s.cancel() }

// ReadRecords starts consuming.
//
// It assigns partitions directly rather than joining a consumer group, and
// never commits an offset. kafman therefore cannot disturb a production
// consumer no matter what the user does here — a property borrowed from yozefu
// and worth keeping deliberate.
func (c *Client) ReadRecords(ctx context.Context, opts ReadOptions) (*ReadSession, error) {
	if len(opts.Topics) == 0 {
		return nil, errors.New("no topics selected")
	}

	consumer, err := sarama.NewConsumerFromClient(c.client)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	assignments, err := c.resolveAssignments(opts)
	if err != nil {
		_ = consumer.Close()
		return nil, err
	}
	if len(assignments) == 0 {
		_ = consumer.Close()
		return nil, errors.New("the selected topics have no partitions to read")
	}

	ctx, cancel := context.WithCancel(ctx)

	var (
		raw     = make(chan Record, 1024)
		batches = make(chan Batch, 8)
		errOnce sync.Once
		readErr error
	)
	fail := func(err error) {
		errOnce.Do(func() { readErr = err })
	}

	var wg sync.WaitGroup
	for _, a := range assignments {
		wg.Add(1)
		go func(a assignment) {
			defer wg.Done()
			if err := c.consumePartition(ctx, consumer, a, raw, opts); err != nil {
				fail(err)
			}
		}(a)
	}

	// Closing raw once every partition is done is what tells the batcher to
	// flush and finish.
	go func() {
		wg.Wait()
		close(raw)
		_ = consumer.Close()
	}()

	go batch(ctx, raw, batches, opts.Limit, cancel)

	slog.Info("reading records",
		"cluster", c.cluster.Name,
		"topics", opts.Topics,
		"partitions", len(assignments),
		"follow", opts.Follow)

	return &ReadSession{
		Records: batches,
		Err:     func() error { return readErr },
		cancel:  cancel,
	}, nil
}

// assignment is one partition to read, with its resolved starting offset.
type assignment struct {
	topic     string
	partition int32
	offset    int64

	// stopAt bounds a non-following read: consumption ends when this offset is
	// reached. -1 means no bound.
	stopAt int64
}

// resolveAssignments turns the requested start position into concrete offsets.
func (c *Client) resolveAssignments(opts ReadOptions) ([]assignment, error) {
	var out []assignment

	for _, topic := range opts.Topics {
		ids, err := c.client.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("partitions of %s: %w", topic, err)
		}
		if len(opts.Partitions) > 0 {
			ids = intersect(ids, opts.Partitions)
		}

		for _, id := range ids {
			offset, stopAt, err := c.resolveOffset(topic, id, opts)
			if err != nil {
				// A single unavailable partition must not abort the whole read;
				// the others still carry data.
				slog.Warn("skipping partition", "topic", topic, "partition", id, "error", err)
				continue
			}
			out = append(out, assignment{topic: topic, partition: id, offset: offset, stopAt: stopAt})
		}
	}
	return out, nil
}

func (c *Client) resolveOffset(topic string, id int32, opts ReadOptions) (start, stopAt int64, err error) {
	newest, err := c.client.GetOffset(topic, id, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, err
	}

	stopAt = int64(-1)
	if !opts.Follow {
		// A bounded read stops at the end as it was when the read began, so a
		// busy topic cannot keep it running forever.
		stopAt = newest
	}

	switch opts.Start.Kind {
	case StartBeginning:
		oldest, err := c.client.GetOffset(topic, id, sarama.OffsetOldest)
		if err != nil {
			return 0, 0, err
		}
		return oldest, stopAt, nil

	case StartLastN:
		oldest, err := c.client.GetOffset(topic, id, sarama.OffsetOldest)
		if err != nil {
			return 0, 0, err
		}
		start := max(newest-opts.Start.N, oldest)
		return start, stopAt, nil

	case StartOffset:
		oldest, err := c.client.GetOffset(topic, id, sarama.OffsetOldest)
		if err != nil {
			return 0, 0, err
		}
		return min(max(opts.Start.Offset, oldest), newest), stopAt, nil

	case StartTime:
		at, err := c.client.GetOffset(topic, id, opts.Start.Time.UnixMilli())
		if err != nil {
			return 0, 0, err
		}
		// -1 means no record at or after that time.
		if at < 0 {
			at = newest
		}
		return at, stopAt, nil

	default: // StartEnd
		return newest, -1, nil
	}
}

// consumePartition reads one partition until the context ends, the partition is
// exhausted, or its bound is reached.
func (c *Client) consumePartition(
	ctx context.Context,
	consumer sarama.Consumer,
	a assignment,
	out chan<- Record,
	opts ReadOptions,
) error {
	// Nothing to read: the partition is empty, or already at its bound.
	if a.stopAt >= 0 && a.offset >= a.stopAt {
		return nil
	}

	pc, err := consumer.ConsumePartition(a.topic, a.partition, a.offset)
	if err != nil {
		return fmt.Errorf("consume %s/%d from offset %d: %w", a.topic, a.partition, a.offset, err)
	}
	defer func() { _ = pc.Close() }()

	for {
		select {
		case <-ctx.Done():
			return nil

		case err, ok := <-pc.Errors():
			if !ok {
				return nil
			}
			return fmt.Errorf("consume %s/%d: %w", a.topic, a.partition, err)

		case message, ok := <-pc.Messages():
			if !ok {
				return nil
			}

			select {
			case out <- toRecord(message):
			case <-ctx.Done():
				return nil
			}

			if a.stopAt >= 0 && message.Offset >= a.stopAt-1 {
				return nil
			}
		}
	}
}

// batch groups records arriving within batchWindow and forwards them.
func batch(ctx context.Context, in <-chan Record, out chan<- Batch, limit int, cancel context.CancelFunc) {
	defer close(out)

	var (
		pending []Record
		read    int64
		timer   = time.NewTimer(batchWindow)
	)
	defer timer.Stop()

	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		batch := Batch{Records: pending, Read: read, Matched: read}
		select {
		case out <- batch:
			pending = nil
			return true
		case <-ctx.Done():
			return false
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case record, ok := <-in:
			if !ok {
				flush()
				return
			}
			pending = append(pending, record)
			read++

			if limit > 0 && read >= int64(limit) {
				flush()
				// The limit is reached: stop the partition readers too.
				cancel()
				return
			}

		case <-timer.C:
			if !flush() {
				return
			}
			timer.Reset(batchWindow)
		}
	}
}

func toRecord(m *sarama.ConsumerMessage) Record {
	record := Record{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Size:      len(m.Key) + len(m.Value),
	}
	for _, h := range m.Headers {
		if h == nil {
			continue
		}
		record.Headers = append(record.Headers, Header{
			Key:   string(h.Key),
			Value: string(h.Value),
		})
	}
	return record
}

func intersect(all, wanted []int32) []int32 {
	want := make(map[int32]struct{}, len(wanted))
	for _, id := range wanted {
		want[id] = struct{}{}
	}

	var out []int32
	for _, id := range all {
		if _, ok := want[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
