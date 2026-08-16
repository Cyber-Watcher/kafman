// Package serdes turns raw Kafka payloads into something a person can read.
//
// Kafka records carry opaque bytes, so the format has to be guessed. The
// detection here is deliberately conservative: it would rather show a payload as
// text than claim a structure it is not sure about. Schema Registry support is
// not implemented, but the Confluent wire format is recognised, so a record
// encoded that way is labelled honestly instead of being rendered as garbage.
package serdes

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Format is what a payload was recognised as.
type Format int

// Recognised payload formats.
const (
	FormatEmpty Format = iota
	FormatNull
	FormatJSON
	FormatXML
	FormatText
	FormatBinary

	// FormatConfluent is the Confluent wire format: a zero magic byte followed
	// by a four-byte schema id. Decoding the body needs the Schema Registry,
	// which kafman does not talk to yet.
	FormatConfluent
)

// String names the format for display.
func (f Format) String() string {
	switch f {
	case FormatEmpty:
		return "empty"
	case FormatNull:
		return "null"
	case FormatJSON:
		return "JSON"
	case FormatXML:
		return "XML"
	case FormatText:
		return "text"
	case FormatConfluent:
		return "Confluent"
	default:
		return "binary"
	}
}

// Decoded is a payload prepared for display.
type Decoded struct {
	Format Format

	// Line is a single-line rendering for a table cell.
	Line string

	// Pretty is the multi-line rendering for the details view. For JSON it is
	// indented; for binary it is a hex dump.
	Pretty string

	// SchemaID is set for FormatConfluent.
	SchemaID int32

	// Size is the length of the original payload in bytes.
	Size int
}

// Confluent wire format: one magic byte 0x00 then a big-endian schema id.
const (
	confluentMagicByte = 0x00
	confluentHeaderLen = 5
)

// maxScan bounds how much of a payload is examined when guessing the format,
// so a multi-megabyte record does not stall the render loop.
const maxScan = 64 * 1024

// Decode inspects a payload and prepares it for display. It never fails: an
// unrecognised payload becomes a hex dump.
func Decode(data []byte) Decoded {
	switch {
	case data == nil:
		return Decoded{Format: FormatNull, Line: "null", Pretty: "null"}
	case len(data) == 0:
		return Decoded{Format: FormatEmpty, Line: "", Pretty: ""}
	}

	result := Decoded{Size: len(data)}

	// The Confluent header has to be checked first: its body is binary and
	// would otherwise be reported as such, hiding the fact that a schema exists.
	if id, ok := confluentSchemaID(data); ok {
		result.Format = FormatConfluent
		result.SchemaID = id
		result.Line = fmt.Sprintf("<Confluent wire format, schema id %d, %d bytes>", id, len(data))
		result.Pretty = result.Line + "\n\n" +
			"Decoding needs a Schema Registry, which kafman does not use yet.\n\n" +
			hexDump(data)
		return result
	}

	scan := data
	if len(scan) > maxScan {
		scan = scan[:maxScan]
	}

	if pretty, ok := prettyJSON(data); ok {
		result.Format = FormatJSON
		result.Line = collapse(string(data))
		result.Pretty = pretty
		return result
	}

	if !utf8.Valid(scan) || hasControlBytes(scan) {
		result.Format = FormatBinary
		result.Line = fmt.Sprintf("<binary, %d bytes>", len(data))
		result.Pretty = hexDump(data)
		return result
	}

	text := string(data)
	if isXML(text) {
		result.Format = FormatXML
		result.Line = collapse(text)
		result.Pretty = text
		return result
	}

	result.Format = FormatText
	result.Line = collapse(text)
	result.Pretty = text
	return result
}

// maxPlausibleSchemaID bounds what is accepted as a Confluent schema id.
//
// The wire format is only five bytes of framing, so the check is a heuristic and
// cannot be exact: any binary payload starting with a zero byte looks like it.
// Registries hand out ids sequentially from 1, and a million subjects would be
// an extraordinary deployment, so an id outside that range is far more likely to
// be ordinary binary data than a real schema reference.
const maxPlausibleSchemaID = 1_000_000

// confluentSchemaID reports the schema id when the payload looks like it carries
// the Confluent header. See maxPlausibleSchemaID for why this is a guess.
func confluentSchemaID(data []byte) (int32, bool) {
	// Five bytes of header plus at least one byte of body.
	if len(data) <= confluentHeaderLen || data[0] != confluentMagicByte {
		return 0, false
	}

	id := binary.BigEndian.Uint32(data[1:confluentHeaderLen])
	if id == 0 || id > maxPlausibleSchemaID {
		return 0, false
	}
	return int32(id), true
}

// prettyJSON indents a payload if it is valid JSON. Bare scalars are rejected:
// "5" and "true" are valid JSON but reporting them as such tells the user
// nothing they cannot see.
func prettyJSON(data []byte) (string, bool) {
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 0 {
		return "", false
	}
	if trimmed[0] != '{' && trimmed[0] != '[' {
		return "", false
	}
	if !json.Valid(data) {
		return "", false
	}

	var buf strings.Builder
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	// SetEscapeHTML(false) keeps URLs and angle brackets readable.
	encoder.SetEscapeHTML(false)

	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return "", false
	}
	if err := encoder.Encode(value); err != nil {
		return "", false
	}
	return strings.TrimRight(buf.String(), "\n"), true
}

func isXML(s string) bool {
	trimmed := strings.TrimSpace(s)
	return strings.HasPrefix(trimmed, "<") && strings.HasSuffix(trimmed, ">")
}

// hasControlBytes reports whether the data contains control characters that
// would corrupt the terminal if printed. Tab, newline and carriage return are
// allowed: they occur in legitimate text payloads.
func hasControlBytes(data []byte) bool {
	for _, b := range data {
		if b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b < 0x20 || b == 0x7f {
			return true
		}
	}
	return false
}

// collapse folds a payload onto one line for a table cell.
func collapse(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	space := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

// hexDump renders bytes as offset, hex and ASCII columns.
func hexDump(data []byte) string {
	const limit = 4 * 1024
	truncated := false
	if len(data) > limit {
		data = data[:limit]
		truncated = true
	}

	dump := hex.Dump(data)
	if truncated {
		dump += fmt.Sprintf("\n… truncated, showing the first %d bytes\n", limit)
	}
	return dump
}

// DecodeKey is Decode for record keys, which are usually short identifiers.
// A key that is valid JSON is still shown as text: pretty-printing a key adds
// nothing in a table.
func DecodeKey(data []byte) Decoded {
	decoded := Decode(data)
	if decoded.Format == FormatJSON {
		decoded.Line = collapse(string(data))
	}
	return decoded
}
