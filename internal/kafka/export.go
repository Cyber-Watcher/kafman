package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"
)

// ExportedRecord is a record prepared for a file.
//
// It carries more than the record itself: the deltas say how far apart events
// were without the reader having to subtract timestamps, and the query says how
// the set was selected. An export that cannot be interpreted six months later is
// not much of an export. Idea from yozefu.
type ExportedRecord struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`

	Timestamp int64  `json:"timestamp_ms"`
	DateTime  string `json:"date_time"`

	// AbsoluteDeltaMs is the offset in time from the first exported record,
	// RelativeDeltaMs from the previous one.
	AbsoluteDeltaMs int64 `json:"absolute_delta_ms"`
	RelativeDeltaMs int64 `json:"relative_delta_ms"`

	Key     any               `json:"key"`
	Value   any               `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`

	Size int `json:"size_bytes"`
}

// Export is the file written to disk.
type Export struct {
	// Query is the search that produced these records, so the export can be
	// reproduced.
	Query string `json:"query,omitempty"`

	Cluster    string    `json:"cluster"`
	Topics     []string  `json:"topics"`
	ExportedAt time.Time `json:"exported_at"`
	Count      int       `json:"count"`

	Records []ExportedRecord `json:"records"`
}

// BuildExport prepares records for writing.
func BuildExport(cluster, query string, records []Record) Export {
	export := Export{
		Query:      query,
		Cluster:    cluster,
		ExportedAt: time.Now(),
		Count:      len(records),
		Records:    make([]ExportedRecord, 0, len(records)),
	}

	seen := make(map[string]struct{})
	var first, previous time.Time

	for i, record := range records {
		if _, ok := seen[record.Topic]; !ok {
			seen[record.Topic] = struct{}{}
			export.Topics = append(export.Topics, record.Topic)
		}

		if i == 0 {
			first, previous = record.Timestamp, record.Timestamp
		}

		exported := ExportedRecord{
			Topic:           record.Topic,
			Partition:       record.Partition,
			Offset:          record.Offset,
			Timestamp:       record.Timestamp.UnixMilli(),
			DateTime:        record.Timestamp.Format(time.RFC3339Nano),
			AbsoluteDeltaMs: record.Timestamp.Sub(first).Milliseconds(),
			RelativeDeltaMs: record.Timestamp.Sub(previous).Milliseconds(),
			Key:             exportPayload(record.Key),
			Value:           exportPayload(record.Value),
			Size:            record.Size,
		}
		if len(record.Headers) > 0 {
			exported.Headers = make(map[string]string, len(record.Headers))
			for _, header := range record.Headers {
				exported.Headers[header.Key] = header.Value
			}
		}

		export.Records = append(export.Records, exported)
		previous = record.Timestamp
	}

	return export
}

// exportPayload renders a payload so the file stays valid JSON whatever the
// records contained.
//
// JSON payloads are embedded as JSON rather than as an escaped string, so the
// export can be queried with jq. Anything that is not valid UTF-8 becomes
// base64, tagged so a reader knows what happened.
func exportPayload(data []byte) any {
	switch {
	case data == nil:
		return nil
	case len(data) == 0:
		return ""
	}

	if json.Valid(data) {
		var value any
		if err := json.Unmarshal(data, &value); err == nil {
			return value
		}
	}

	if utf8.Valid(data) {
		return string(data)
	}
	return map[string]string{
		"encoding": "base64",
		"data":     base64.StdEncoding.EncodeToString(data),
	}
}

// WriteExport writes an export to path, creating the directory if needed.
func WriteExport(path string, export Export) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create export directory %s: %w", dir, err)
		}
	}

	data, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return fmt.Errorf("encode export: %w", err)
	}

	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil { //nolint:gosec // an export is meant to be read
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// ExportPath builds a default file name in dir, stamped with the time so
// repeated exports do not overwrite each other.
func ExportPath(dir string) string {
	name := fmt.Sprintf("kafman-export-%s.json", time.Now().Format("20060102-150405"))
	if dir == "" {
		return name
	}
	return filepath.Join(dir, name)
}
