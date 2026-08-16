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

// ProduceRequest is one record to publish.
type ProduceRequest struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []Header

	// Partition is honoured only when Manual is set; otherwise the partition is
	// chosen from the key.
	Partition int32
	Manual    bool
}

// ProduceResult is where a published record landed.
type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Produce publishes one record.
//
// The producer is created on first use and kept: building one costs a metadata
// round trip, and a read-only session should not pay for it.
func (c *Client) Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	return run(ctx, func() (ProduceResult, error) {
		maybeDelay("produce")

		producer, err := c.syncProducer()
		if err != nil {
			return ProduceResult{}, err
		}

		message := &sarama.ProducerMessage{
			Topic: req.Topic,
			Value: sarama.ByteEncoder(req.Value),
		}
		if req.Key != nil {
			message.Key = sarama.ByteEncoder(req.Key)
		}
		for _, header := range req.Headers {
			message.Headers = append(message.Headers, sarama.RecordHeader{
				Key:   []byte(header.Key),
				Value: []byte(header.Value),
			})
		}
		if req.Manual {
			message.Partition = req.Partition
			message.Metadata = manualPartition{}
		}

		partition, offset, err := c.send(producer, message, req.Topic)
		if err != nil {
			return ProduceResult{}, fmt.Errorf("publish to %s: %w", req.Topic, err)
		}

		slog.Info("published record",
			"cluster", c.cluster.Name, "topic", req.Topic,
			"partition", partition, "offset", offset)

		return ProduceResult{Topic: req.Topic, Partition: partition, Offset: offset}, nil
	})
}

// send publishes the message, retrying while the brokers still disagree about
// whether the topic exists.
//
// A topic created moments ago is usable through the admin path before every
// broker's metadata has caught up, and the producer keeps its own cache on top
// of that. Publishing to a topic just created in the Manage tab would otherwise
// fail once with "topic or partition does not exist" and work on the second try
// — the kind of error that teaches people to press the key twice.
func (c *Client) send(producer sarama.SyncProducer, message *sarama.ProducerMessage, topic string) (int32, int64, error) {
	var err error
	for attempt := range produceAttempts {
		var partition int32
		var offset int64

		partition, offset, err = producer.SendMessage(message)
		if err == nil {
			return partition, offset, nil
		}
		if !errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			return 0, 0, err
		}

		if attempt < produceAttempts-1 {
			slog.Debug("topic not visible to the producer yet, refreshing metadata",
				"topic", topic, "attempt", attempt+1)
			_ = c.client.RefreshMetadata(topic)
			time.Sleep(produceRetryDelay)
		}
	}
	return 0, 0, err
}

// How long publishing waits for a new topic to become visible to every broker.
const (
	produceAttempts   = 5
	produceRetryDelay = 200 * time.Millisecond
)

// syncProducer returns the shared producer, creating it on first use.
//
// The producer inherits the client's configuration, which is why the
// partitioner is set in BuildSaramaConfig rather than here.
func (c *Client) syncProducer() (sarama.SyncProducer, error) {
	c.producerOnce.Do(func() {
		c.producer, c.producerErr = sarama.NewSyncProducerFromClient(c.client)
		if c.producerErr != nil {
			c.producerErr = fmt.Errorf("create producer: %w", c.producerErr)
		}
	})
	return c.producer, c.producerErr
}

// manualPartition marks a message whose partition the user chose explicitly.
//
// The partitioner cannot tell an explicit partition 0 from an unset one, since
// ProducerMessage.Partition is simply an int32, so the intent travels in the
// message metadata instead.
type manualPartition struct{}

// ------------------------------------------------------------ partitioning ---

// kafmanPartitioner routes by key the way the Java client does, and honours an
// explicitly chosen partition.
//
// This matters more than it looks: sarama's default hash partitioner uses FNV-1a
// while the Java client uses murmur2. A record published by kafman with the same
// key as one published by a Java producer would land in a different partition,
// which breaks per-key ordering for every consumer that relies on it.
type kafmanPartitioner struct {
	random sarama.Partitioner
}

func newKafmanPartitioner(topic string) sarama.Partitioner {
	return &kafmanPartitioner{random: sarama.NewRandomPartitioner(topic)}
}

func (p *kafmanPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// An explicitly chosen partition wins; see manualPartition for why the flag
	// travels in the metadata.
	if _, manual := message.Metadata.(manualPartition); manual {
		if message.Partition < 0 || message.Partition >= numPartitions {
			return 0, fmt.Errorf("partition %d does not exist; the topic has %d",
				message.Partition, numPartitions)
		}
		return message.Partition, nil
	}
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}

	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}
	// The mask matches Kafka's own toPositive(): the sign bit is cleared rather
	// than the value being made absolute.
	return (murmur2(key) & 0x7fffffff) % numPartitions, nil
}

// RequiresConsistency reports that the partitioner is deterministic for a key.
func (p *kafmanPartitioner) RequiresConsistency() bool { return true }

// murmur2 is the hash Kafka's Java client uses to route keys to partitions,
// ported from org.apache.kafka.common.utils.Utils.murmur2.
func murmur2(data []byte) int32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)

	length := len(data)
	h := seed ^ uint32(length)

	// Four bytes at a time.
	for i := 0; i+4 <= length; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}

	// The remaining bytes, in the same order Java processes them.
	switch length & 3 {
	case 3:
		h ^= uint32(data[(length & ^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length & ^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length & ^3])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return int32(h)
}

// producerState is embedded in Client; kept here so the producer's lifecycle
// lives beside its use.
type producerState struct {
	producerOnce sync.Once
	producer     sarama.SyncProducer
	producerErr  error
}
