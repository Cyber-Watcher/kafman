package serdes

import (
	"strings"
	"testing"
)

func TestDecodeJSON(t *testing.T) {
	data := []byte(`{"id":"ord-99f","status":"FAILED","items":[{"sku":"A-1002","qty":2}]}`)

	got := Decode(data)
	if got.Format != FormatJSON {
		t.Fatalf("format = %v, want JSON", got.Format)
	}
	if !strings.Contains(got.Pretty, "\n  \"id\"") {
		t.Errorf("Pretty should be indented:\n%s", got.Pretty)
	}
	// The one-line rendering must stay on one line for a table cell.
	if strings.Contains(got.Line, "\n") {
		t.Errorf("Line should be a single line: %q", got.Line)
	}
	if got.Size != len(data) {
		t.Errorf("Size = %d, want %d", got.Size, len(data))
	}
}

func TestDecodeRejectsBareScalars(t *testing.T) {
	// "5" and "true" are valid JSON, but calling them JSON tells the user
	// nothing they cannot already see.
	for _, input := range []string{"5", "true", `"a string"`, "null "} {
		got := Decode([]byte(input))
		if got.Format == FormatJSON {
			t.Errorf("Decode(%q) reported JSON; a bare scalar should be text", input)
		}
	}
}

func TestDecodeInvalidJSONFallsBackToText(t *testing.T) {
	got := Decode([]byte(`{"unclosed": `))
	if got.Format != FormatText {
		t.Errorf("format = %v, want text for malformed JSON", got.Format)
	}
}

func TestDecodeConfluentWireFormat(t *testing.T) {
	// Magic byte 0x00 then a big-endian schema id of 42, then Avro bytes.
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0x06, 0x66, 0x6f, 0x6f}

	got := Decode(data)
	if got.Format != FormatConfluent {
		t.Fatalf("format = %v, want Confluent", got.Format)
	}
	if got.SchemaID != 42 {
		t.Errorf("schema id = %d, want 42", got.SchemaID)
	}
	// The user must be told why the payload is not shown decoded, rather than
	// being handed bytes that look like corruption.
	if !strings.Contains(got.Pretty, "Schema Registry") {
		t.Errorf("Pretty should explain what is missing:\n%s", got.Pretty)
	}
	if !strings.Contains(got.Line, "schema id 42") {
		t.Errorf("Line should name the schema id: %q", got.Line)
	}
}

func TestDecodeBinary(t *testing.T) {
	data := []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a} // PNG header

	got := Decode(data)
	if got.Format != FormatBinary {
		t.Fatalf("format = %v, want binary", got.Format)
	}
	if !strings.Contains(got.Pretty, "89 50 4e 47") {
		t.Errorf("Pretty should be a hex dump:\n%s", got.Pretty)
	}
	if !strings.Contains(got.Line, "8 bytes") {
		t.Errorf("Line should state the size: %q", got.Line)
	}
}

func TestDecodeText(t *testing.T) {
	got := Decode([]byte("a plain log line\nwith a second line"))
	if got.Format != FormatText {
		t.Fatalf("format = %v, want text", got.Format)
	}
	// Newlines are folded for the table cell but kept in the detail view.
	if strings.Contains(got.Line, "\n") {
		t.Errorf("Line should be folded: %q", got.Line)
	}
	if !strings.Contains(got.Pretty, "\n") {
		t.Error("Pretty should keep the original line breaks")
	}
}

func TestDecodeXML(t *testing.T) {
	got := Decode([]byte("<order><id>99</id></order>"))
	if got.Format != FormatXML {
		t.Errorf("format = %v, want XML", got.Format)
	}
}

func TestDecodeEmptyAndNull(t *testing.T) {
	// A tombstone (nil value) and an empty payload mean different things, and
	// the difference matters when reading a compacted topic.
	if got := Decode(nil); got.Format != FormatNull {
		t.Errorf("nil value = %v, want null", got.Format)
	}
	if got := Decode([]byte{}); got.Format != FormatEmpty {
		t.Errorf("empty value = %v, want empty", got.Format)
	}
}

func TestDecodeTolerateUTF8(t *testing.T) {
	got := Decode([]byte("заказ принят ✓"))
	if got.Format != FormatText {
		t.Errorf("format = %v, want text for valid UTF-8", got.Format)
	}
	if got.Line != "заказ принят ✓" {
		t.Errorf("Line = %q, the text should survive intact", got.Line)
	}
}

func TestDecodeRejectsControlBytes(t *testing.T) {
	// Control characters would corrupt the terminal if printed.
	got := Decode([]byte("text with a \x00 null"))
	if got.Format != FormatBinary {
		t.Errorf("format = %v, want binary for a payload with control bytes", got.Format)
	}
}

func TestDecodeAllowsWhitespaceControlBytes(t *testing.T) {
	got := Decode([]byte("column\tvalue\r\nnext\trow"))
	if got.Format != FormatText {
		t.Errorf("format = %v, tabs and newlines are legitimate in text", got.Format)
	}
}

func TestHexDumpIsBounded(t *testing.T) {
	// A multi-megabyte payload must not be rendered in full.
	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = byte(1 + i%250) // never zero, so it cannot look like a Confluent header
	}

	got := Decode(big)
	if got.Format != FormatBinary {
		t.Fatalf("format = %v, want binary", got.Format)
	}
	if !strings.Contains(got.Pretty, "truncated") {
		t.Error("a large hex dump should be truncated and say so")
	}
	if len(got.Pretty) > 200_000 {
		t.Errorf("the dump is %d characters, too large to render", len(got.Pretty))
	}
}

func TestFormatNames(t *testing.T) {
	names := map[Format]string{
		FormatJSON: "JSON", FormatText: "text", FormatBinary: "binary",
		FormatConfluent: "Confluent", FormatXML: "XML", FormatNull: "null",
		FormatEmpty: "empty",
	}
	for format, want := range names {
		if got := format.String(); got != want {
			t.Errorf("Format(%d).String() = %q, want %q", format, got, want)
		}
	}
}

func TestConfluentDetectionIsBounded(t *testing.T) {
	// The five-byte framing is a weak signal, so implausible schema ids are
	// treated as ordinary binary rather than claimed as Avro.
	cases := []struct {
		name string
		data []byte
		want Format
	}{
		{
			"plausible id",
			[]byte{0x00, 0x00, 0x00, 0x00, 0x2a, 0xff},
			FormatConfluent,
		},
		{
			"id zero is not issued by a registry",
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xff},
			FormatBinary,
		},
		{
			"an implausibly large id is just binary data",
			[]byte{0x00, 0x7f, 0xff, 0xff, 0xff, 0xff},
			FormatBinary,
		},
		{
			"header with no body",
			[]byte{0x00, 0x00, 0x00, 0x00, 0x2a},
			FormatBinary,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decode(tc.data); got.Format != tc.want {
				t.Errorf("format = %v, want %v", got.Format, tc.want)
			}
		})
	}
}
