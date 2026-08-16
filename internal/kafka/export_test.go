package kafka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func exportSample() []Record {
	base := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	return []Record{
		{
			Topic: "orders.events", Partition: 3, Offset: 100,
			Key: []byte("ord-1"), Value: []byte(`{"status":"NEW"}`),
			Headers:   []Header{{Key: "source", Value: "api"}},
			Timestamp: base, Size: 21,
		},
		{
			Topic: "orders.events", Partition: 3, Offset: 101,
			Key: []byte("ord-2"), Value: []byte("plain text"),
			Timestamp: base.Add(250 * time.Millisecond), Size: 15,
		},
		{
			Topic: "payments.tx", Partition: 1, Offset: 55,
			Key: []byte("pay-1"), Value: nil,
			Timestamp: base.Add(1500 * time.Millisecond), Size: 5,
		},
	}
}

func TestBuildExportCarriesContext(t *testing.T) {
	export := BuildExport("prod-dc1", `value.status == "NEW"`, exportSample())

	// An export that cannot be interpreted later is not much of an export.
	if export.Query != `value.status == "NEW"` {
		t.Errorf("query = %q, want the search that produced the records", export.Query)
	}
	if export.Cluster != "prod-dc1" {
		t.Errorf("cluster = %q", export.Cluster)
	}
	if export.Count != 3 {
		t.Errorf("count = %d, want 3", export.Count)
	}
	if len(export.Topics) != 2 {
		t.Errorf("topics = %v, want both topics listed once each", export.Topics)
	}
}

func TestExportDeltas(t *testing.T) {
	export := BuildExport("c", "", exportSample())

	// Deltas save the reader from subtracting timestamps by hand.
	cases := []struct{ absolute, relative int64 }{
		{0, 0},
		{250, 250},
		{1500, 1250},
	}
	for i, want := range cases {
		record := export.Records[i]
		if record.AbsoluteDeltaMs != want.absolute {
			t.Errorf("record %d absolute delta = %d, want %d", i, record.AbsoluteDeltaMs, want.absolute)
		}
		if record.RelativeDeltaMs != want.relative {
			t.Errorf("record %d relative delta = %d, want %d", i, record.RelativeDeltaMs, want.relative)
		}
	}
}

func TestExportEmbedsJSONAsJSON(t *testing.T) {
	export := BuildExport("c", "", exportSample())

	// A JSON payload must be embedded as JSON so the file can be queried with
	// jq, not escaped into a string.
	value, ok := export.Records[0].Value.(map[string]any)
	if !ok {
		t.Fatalf("value is %T, want an embedded object", export.Records[0].Value)
	}
	if value["status"] != "NEW" {
		t.Errorf("value = %v, want the parsed object", value)
	}

	// Text stays text, and a nil value stays null.
	if got, ok := export.Records[1].Value.(string); !ok || got != "plain text" {
		t.Errorf("text value = %#v, want the string", export.Records[1].Value)
	}
	if export.Records[2].Value != nil {
		t.Errorf("a tombstone should export as null, got %#v", export.Records[2].Value)
	}
}

func TestExportBase64EncodesBinary(t *testing.T) {
	records := []Record{{
		Topic: "t", Value: []byte{0xff, 0xfe, 0x00, 0x01}, Timestamp: time.Now(),
	}}

	export := BuildExport("c", "", records)
	encoded, ok := export.Records[0].Value.(map[string]string)
	if !ok {
		t.Fatalf("value is %T, want a tagged base64 object", export.Records[0].Value)
	}
	if encoded["encoding"] != "base64" {
		t.Errorf("encoding = %q, want it tagged so a reader knows", encoded["encoding"])
	}
	if encoded["data"] == "" {
		t.Error("the encoded data is missing")
	}
}

func TestWriteExportProducesValidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "export.json")

	if err := WriteExport(path, BuildExport("c", "q", exportSample())); err != nil {
		t.Fatalf("WriteExport: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the export was not written: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("the export is not valid JSON:\n%s", data)
	}

	var reloaded Export
	if err := json.Unmarshal(data, &reloaded); err != nil {
		t.Fatalf("the export does not round-trip: %v", err)
	}
	if reloaded.Count != 3 || len(reloaded.Records) != 3 {
		t.Errorf("reloaded %d records, want 3", len(reloaded.Records))
	}
}

func TestExportPathIsTimestamped(t *testing.T) {
	first := ExportPath("/tmp/exports")
	if !strings.HasPrefix(first, "/tmp/exports/") {
		t.Errorf("path = %q, want it inside the given directory", first)
	}
	if !strings.HasSuffix(first, ".json") {
		t.Errorf("path = %q, want a .json file", first)
	}
	// The name carries a timestamp so repeated exports do not overwrite.
	if !strings.Contains(first, "kafman-export-") {
		t.Errorf("path = %q, want a recognisable name", first)
	}
}

func TestBuildExportOfNothing(t *testing.T) {
	export := BuildExport("c", "", nil)
	if export.Count != 0 || len(export.Records) != 0 {
		t.Error("an empty export should be empty rather than malformed")
	}
	if err := WriteExport(filepath.Join(t.TempDir(), "e.json"), export); err != nil {
		t.Errorf("writing an empty export: %v", err)
	}
}
