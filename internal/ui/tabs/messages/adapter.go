package messages

import (
	"time"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/query"
)

// recordAdapter presents a Kafka record as something the query package can
// evaluate.
//
// The adapter exists so internal/query does not depend on the Kafka client: the
// language is about records, not about sarama, and keeping it that way makes it
// testable with a two-line fake.
type recordAdapter struct {
	record kafka.Record
}

var _ query.Record = recordAdapter{}

func (a recordAdapter) Topic() string        { return a.record.Topic }
func (a recordAdapter) Partition() int32     { return a.record.Partition }
func (a recordAdapter) Offset() int64        { return a.record.Offset }
func (a recordAdapter) Key() []byte          { return a.record.Key }
func (a recordAdapter) Value() []byte        { return a.record.Value }
func (a recordAdapter) Timestamp() time.Time { return a.record.Timestamp }
func (a recordAdapter) Size() int            { return a.record.Size }

func (a recordAdapter) Header(name string) (string, bool) {
	for _, header := range a.record.Headers {
		if header.Key == name {
			return header.Value, true
		}
	}
	return "", false
}

// fromClause turns the query's start position into the reader's, falling back
// to the tab's current setting when the query does not say.
func fromClause(from query.From, fallback kafka.Start) kafka.Start {
	switch from.Kind {
	case query.FromBeginning:
		return kafka.Start{Kind: kafka.StartBeginning}
	case query.FromLastN:
		return kafka.Start{Kind: kafka.StartLastN, N: from.N}
	case query.FromOffset:
		return kafka.Start{Kind: kafka.StartOffset, Offset: from.Offset}
	case query.FromTime:
		return kafka.Start{Kind: kafka.StartTime, Time: from.Time}
	default:
		// The query language's default is "end", but an unwritten clause should
		// leave whatever the user last chose with the s key alone.
		return fallback
	}
}
