package messages

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/serdes"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// recordDetails is the overlay showing one record in full.
//
// It has two modes. In list mode the arrow keys are not handled here: the tab
// forwards them to the record list underneath, so the user can step through
// records without closing the overlay — that one detail is what makes the view
// usable for scanning. Tab switches to value mode, where the same keys scroll
// the record itself: a message can be far taller than the screen.
type recordDetails struct {
	row     row
	offset  int
	height  int
	focused bool
}

func newRecordDetails(item row) *recordDetails {
	return &recordDetails{row: item}
}

// SetRow replaces the record shown, resetting the scroll.
func (d *recordDetails) SetRow(item row) {
	d.row = item
	d.offset = 0
}

// Name implements ui.Overlay.
func (d *recordDetails) Name() string { return "Record" }

// Focused reports whether the value itself, not the list underneath, owns the
// arrow keys.
func (d *recordDetails) Focused() bool { return d.focused }

// Blur hands the arrow keys back to the record list.
func (d *recordDetails) Blur() { d.focused = false }

// Shortcuts implements ui.ShortcutProvider.
func (d *recordDetails) Shortcuts() []chrome.Shortcut {
	if d.focused {
		return []chrome.Shortcut{
			{Key: "↑/↓", Label: "Scroll"},
			{Key: "PgUp/PgDn", Label: "Page"},
			{Key: "g/G", Label: "Top/bottom"},
			{Key: "Tab", Label: "Back to list"},
			{Key: "Esc", Label: "Back"},
		}
	}
	return []chrome.Shortcut{
		{Key: "↑/↓", Label: "Prev/next record"},
		{Key: "Tab", Label: "Scroll the value"},
		{Key: "J/K", Label: "Scroll"},
		{Key: "C", Label: "Copy"},
		{Key: "Esc", Label: "Back"},
	}
}

// Update handles scrolling; in list mode the record navigation belongs to the
// list underneath and only the shifted keys scroll.
func (d *recordDetails) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return nil
	}
	switch key.String() {
	case "tab", "shift+tab":
		d.focused = !d.focused
	case "J":
		d.offset++
	case "K":
		d.offset = max(d.offset-1, 0)
	case "pgdown":
		d.offset += d.height
	case "pgup":
		d.offset = max(d.offset-d.height, 0)
	case "home":
		d.offset = 0
	case "end", "G":
		// Clamped to the last page in View, where the line count is known.
		d.offset = 1 << 30
	}
	if !d.focused {
		return nil
	}
	switch key.String() {
	case "up", "k":
		d.offset = max(d.offset-1, 0)
	case "down", "j":
		d.offset++
	case "g":
		d.offset = 0
	}
	return nil
}

// Text returns the record as plain text, for the clipboard.
func (d *recordDetails) Text() string {
	record := d.row.record

	var b strings.Builder
	fmt.Fprintf(&b, "topic: %s\npartition: %d\noffset: %d\ntimestamp: %s\n",
		record.Topic, record.Partition, record.Offset,
		record.Timestamp.Format("2006-01-02T15:04:05.000Z07:00"))
	for _, h := range record.Headers {
		fmt.Fprintf(&b, "header %s: %s\n", h.Key, h.Value)
	}
	fmt.Fprintf(&b, "key: %s\n\n%s\n", d.row.key.Line, d.row.value.Pretty)
	return b.String()
}

// View implements ui.View.
func (d *recordDetails) View(k *ktx.Context) string {
	th := k.Theme
	d.height = k.AvailableHeight
	width := k.WindowWidth - 12

	record := d.row.record

	label := func(s string) string {
		return th.Label.Render(padLeft(s, 12)) + ": "
	}

	lines := []string{
		label("Topic") + th.Value.Render(record.Topic),
		label("Partition") + th.Warning.Render(fmt.Sprint(record.Partition)),
		label("Offset") + th.Value.Render(fmt.Sprint(record.Offset)),
		label("Timestamp") + th.Value.Render(
			record.Timestamp.Format("2006-01-02T15:04:05.000Z07:00")),
		label("Age") + th.Dim.Render(humanSince(time.Since(record.Timestamp))),
		label("Size") + th.Value.Render(fmt.Sprintf("%d bytes", record.Size)),
		label("Format") + d.formatLine(th),
	}

	if len(record.Headers) > 0 {
		for i, h := range record.Headers {
			name := ""
			if i == 0 {
				name = "Headers"
			}
			lines = append(lines, label(name)+
				th.Dim.Render(h.Key)+th.Value.Render(" = "+h.Value))
		}
	}

	// The focus is already announced by the accent border and the footer; the
	// highlighted label is just the local cue, with no text of its own.
	valueLabel := label("Value")
	if d.focused {
		valueLabel = th.Highlight.Render(padLeft("Value", 12)) + ": "
	}
	lines = append(lines,
		label("Key")+th.Success.Render(d.row.key.Line),
		valueLabel,
		"",
	)

	highlight := d.row.value.Format == serdes.FormatJSON
	for _, line := range strings.Split(d.row.value.Pretty, "\n") {
		for wrapped := range strings.SplitSeq(wrapHard(line, width), "\n") {
			if highlight {
				lines = append(lines, colourJSONLine(wrapped, th))
			} else {
				lines = append(lines, th.Value.Render(wrapped))
			}
		}
	}

	// Scroll.
	flat := strings.Split(strings.Join(lines, "\n"), "\n")
	if d.offset > max(len(flat)-d.height, 0) {
		d.offset = max(len(flat)-d.height, 0)
	}
	visible := flat[min(d.offset, len(flat)):]
	if len(visible) > d.height {
		visible = visible[:d.height]
	}
	return lipgloss.JoinVertical(lipgloss.Left, visible...)
}

// formatLine names the payload format, and says plainly when a schema would be
// needed to decode it rather than showing bytes as though they were text.
func (d *recordDetails) formatLine(th *theme.Theme) string {
	value := d.row.value
	if value.Format != serdes.FormatConfluent {
		return th.Value.Render(value.Format.String())
	}
	return th.Warning.Render(fmt.Sprintf("Avro or Protobuf, schema id %d", value.SchemaID)) +
		th.Dim.Render("  — Schema Registry is not configured")
}

// colourJSONLine renders one line of pretty-printed JSON with every token in
// its own colour: keys, strings, numbers, literals, punctuation. It works on a
// single display line, after hard wrapping — a long string cut at the width
// loses its quotes at the break and its remainder renders plain, which is the
// acceptable price of wrapping before lexing.
func colourJSONLine(line string, th *theme.Theme) string {
	var b strings.Builder
	runes := []rune(line)

	for i := 0; i < len(runes); {
		c := runes[i]
		switch {
		case c == '"':
			j := i + 1
			for j < len(runes) && runes[j] != '"' {
				if runes[j] == '\\' {
					j++ // the escaped rune is part of the string, even a quote
				}
				j++
			}
			end := min(j+1, len(runes))
			token := string(runes[i:end])

			// A string with a colon after it is a key, everything else a value.
			k := end
			for k < len(runes) && runes[k] == ' ' {
				k++
			}
			if k < len(runes) && runes[k] == ':' {
				b.WriteString(th.Label.Render(token))
			} else {
				b.WriteString(th.Success.Render(token))
			}
			i = end

		case c == '-' || (c >= '0' && c <= '9'):
			j := i
			for j < len(runes) && strings.ContainsRune("+-.eE0123456789", runes[j]) {
				j++
			}
			b.WriteString(th.Warning.Render(string(runes[i:j])))
			i = j

		case jsonLiteralAt(runes, i) != "":
			literal := jsonLiteralAt(runes, i)
			b.WriteString(th.Warning.Render(literal))
			i += len(literal)

		case strings.ContainsRune("{}[]:,", c):
			b.WriteString(th.Dim.Render(string(c)))
			i++

		case c == ' ' || c == '\t':
			b.WriteRune(c)
			i++

		default:
			// Anything else — usually the tail of a wrapped string — as plain
			// text, gathered into one run to keep the escape codes down.
			j := i
			for j < len(runes) && !strings.ContainsRune(`"{}[]:, `, runes[j]) {
				j++
			}
			b.WriteString(th.Value.Render(string(runes[i:j])))
			i = j
		}
	}
	return b.String()
}

// jsonLiteralAt reports the JSON literal starting at position i, or "" when
// there is none. The rune after it must not extend the word, so the "true" in
// a wrapped fragment like "truex" stays plain text.
func jsonLiteralAt(runes []rune, i int) string {
	for _, literal := range []string{"true", "false", "null"} {
		end := i + len(literal)
		if end > len(runes) || string(runes[i:end]) != literal {
			continue
		}
		if end < len(runes) && (isWordRune(runes[end])) {
			continue
		}
		return literal
	}
	return ""
}

func isWordRune(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// wrapHard breaks a line at the width, without looking for word boundaries:
// JSON and hex dumps have none worth respecting.
func wrapHard(s string, width int) string {
	if width <= 0 || lipgloss.Width(s) <= width {
		return s
	}

	var b strings.Builder
	runes := []rune(s)
	for i := 0; i < len(runes); i += width {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(string(runes[i:min(i+width, len(runes))]))
	}
	return b.String()
}
