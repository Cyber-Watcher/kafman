package messages

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/query"
	"github.com/Cyber-Watcher/kafman/internal/serdes"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// timeFormat is how record timestamps are rendered.
type timeFormat int

const (
	timeClock timeFormat = iota // 12:06:01.884
	timeFull                    // 2026-08-04T12:06:01.884+03:00
	timeAgo                     // 5 minutes ago
	timeFormatCount
)

// row is one record with its payload already decoded, so scrolling does not
// re-parse JSON on every frame.
type row struct {
	record kafka.Record
	key    serdes.Decoded
	value  serdes.Decoded
}

// recordList is the record table: a ring buffer with follow mode and adjustable
// columns.
type recordList struct {
	rows     []row
	capacity int

	cursor  int
	offset  int
	focused bool

	// follow keeps the newest record selected as data arrives. Any manual
	// movement switches it off, because fighting the cursor is worse than
	// having to press a key.
	follow bool

	// reversed shows newest first.
	reversed bool

	format timeFormat

	// columnPadding widens the topic and key columns, adjusted with the arrows.
	columnPadding int

	// sortVar is the query variable the buffer is ordered by, or "" for arrival
	// order.
	sortVar  string
	sortDesc bool

	pageSize int
}

func newRecordList(capacity int) *recordList {
	if capacity <= 0 {
		capacity = 5000
	}
	return &recordList{capacity: capacity, follow: true}
}

// Append adds a batch, dropping the oldest records once the buffer is full.
func (r *recordList) Append(records []kafka.Record) {
	for _, record := range records {
		r.rows = append(r.rows, row{
			record: record,
			key:    serdes.DecodeKey(record.Key),
			value:  serdes.Decode(record.Value),
		})
	}

	if overflow := len(r.rows) - r.capacity; overflow > 0 {
		r.rows = r.rows[overflow:]
		r.cursor = max(r.cursor-overflow, 0)
		r.offset = max(r.offset-overflow, 0)
	}

	// A sorted view has to be re-ordered as records arrive, or new ones would
	// pile up at the end regardless of the sort.
	r.applySort()

	if r.follow {
		r.cursor = len(r.rows) - 1
	}
	r.clamp()
}

// Clear empties the buffer.
func (r *recordList) Clear() {
	r.rows = nil
	r.cursor, r.offset = 0, 0
	r.follow = true
}

// Len is the number of buffered records.
func (r *recordList) Len() int { return len(r.rows) }

// Records returns the buffered records in display order, which is what an
// export should contain: what the user is looking at, not the internal order.
func (r *recordList) Records() []kafka.Record {
	out := make([]kafka.Record, 0, len(r.rows))
	for i := range r.rows {
		out = append(out, r.rows[r.displayIndex(i)].record)
	}
	return out
}

// Sort orders the buffer by a query variable.
//
// Sorting applies to what has already been read, not to the stream: Kafka
// delivers per partition, so a global order only exists once records are in
// hand. Follow mode is switched off, because a sorted view has no "newest row"
// to chase.
func (r *recordList) Sort(variable string, descending bool) {
	if variable == "" {
		r.sortVar, r.sortDesc = "", false
		return
	}
	r.sortVar, r.sortDesc = variable, descending
	r.follow = false
	r.applySort()
}

func (r *recordList) applySort() {
	if r.sortVar == "" {
		return
	}

	less := func(a, b row) bool {
		switch r.sortVar {
		case query.VarTopic:
			return a.record.Topic < b.record.Topic
		case query.VarPartition:
			return a.record.Partition < b.record.Partition
		case query.VarKey:
			return string(a.record.Key) < string(b.record.Key)
		case query.VarValue:
			return a.value.Line < b.value.Line
		case query.VarSize:
			return a.record.Size < b.record.Size
		case query.VarTimestamp:
			return a.record.Timestamp.Before(b.record.Timestamp)
		default: // offset
			return a.record.Offset < b.record.Offset
		}
	}

	sort.SliceStable(r.rows, func(i, j int) bool {
		if r.sortDesc {
			return less(r.rows[j], r.rows[i])
		}
		return less(r.rows[i], r.rows[j])
	})
}

// Selected returns the record under the cursor, or nil.
func (r *recordList) Selected() *row {
	if len(r.rows) == 0 {
		return nil
	}
	index := r.displayIndex(r.cursor)
	if index < 0 || index >= len(r.rows) {
		return nil
	}
	return &r.rows[index]
}

// Following reports whether follow mode is on.
func (r *recordList) Following() bool { return r.follow }

func (r *recordList) Focus() { r.focused = true }
func (r *recordList) Blur()  { r.focused = false }

// displayIndex maps a screen position to a buffer index, honouring the display
// order.
func (r *recordList) displayIndex(position int) int {
	if !r.reversed {
		return position
	}
	return len(r.rows) - 1 - position
}

func (r *recordList) clamp() {
	if r.cursor >= len(r.rows) {
		r.cursor = len(r.rows) - 1
	}
	if r.cursor < 0 {
		r.cursor = 0
	}
	if r.pageSize > 0 {
		switch {
		case r.cursor < r.offset:
			r.offset = r.cursor
		case r.cursor >= r.offset+r.pageSize:
			r.offset = r.cursor - r.pageSize + 1
		}
	}
	if r.offset < 0 {
		r.offset = 0
	}
}

// Update handles navigation and display toggles.
func (r *recordList) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok || !r.focused {
		return nil
	}

	switch key.String() {
	case "up", "k":
		r.move(-1)
	case "down", "j":
		r.move(1)
	case "pgup":
		r.move(-r.pageSize)
	case "pgdown":
		r.move(r.pageSize)
	case "home", "g":
		r.follow = false
		r.cursor = 0
		r.clamp()
	case "end", "G":
		r.cursor = len(r.rows) - 1
		r.clamp()
	case "f":
		r.follow = !r.follow
		if r.follow {
			r.cursor = len(r.rows) - 1
			r.clamp()
		}
	case "r":
		r.reversed = !r.reversed
		// The cursor is a screen position, so flipping the order would otherwise
		// jump it to the far end of the buffer.
		r.cursor = len(r.rows) - 1 - r.cursor
		r.clamp()
	case "t":
		r.format = (r.format + 1) % timeFormatCount
	case "left":
		r.columnPadding = max(r.columnPadding-4, 0)
	case "right":
		r.columnPadding = min(r.columnPadding+4, 40)
	}
	return nil
}

func (r *recordList) move(delta int) {
	if delta == 0 {
		return
	}
	// Moving by hand means the user wants to look at something, so stop dragging
	// the selection to the newest record.
	r.follow = false
	r.cursor += delta
	r.clamp()
}

// View renders the table.
func (r *recordList) View(th *theme.Theme, width, height int) string {
	if height < 2 {
		return ""
	}
	r.pageSize = height - 1
	r.clamp()

	widths := r.columnWidths(width)

	lines := []string{r.header(th, widths)}

	end := min(r.offset+r.pageSize, len(r.rows))
	for i := r.offset; i < end; i++ {
		lines = append(lines, r.renderRow(th, r.rows[r.displayIndex(i)], widths, i == r.cursor))
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// Column widths. TOPIC and KEY grow with columnPadding; VALUE takes the rest.
type widths struct {
	timestamp, topic, offset, key, value int
}

func (r *recordList) columnWidths(total int) widths {
	w := widths{
		timestamp: 13,
		topic:     19 + r.columnPadding,
		offset:    9,
		key:       10 + r.columnPadding,
	}
	if r.format == timeFull {
		w.timestamp = 29
	}
	if r.format == timeAgo {
		w.timestamp = 15
	}

	const gaps = 4 * 2
	w.value = max(total-w.timestamp-w.topic-w.offset-w.key-gaps, 10)
	return w
}

func (r *recordList) header(th *theme.Theme, w widths) string {
	label := "TIMESTAMP"
	if r.reversed {
		label += " ▼"
	} else {
		label += " ▲"
	}

	return th.TableHeader.Render(strings.Join([]string{
		padRight(label, w.timestamp),
		padLeft("TOPIC", w.topic),
		padLeft("OFFSET", w.offset),
		padLeft("KEY", w.key),
		"VALUE",
	}, "  "))
}

func (r *recordList) renderRow(th *theme.Theme, item row, w widths, selected bool) string {
	timestamp := r.formatTime(th, item.record.Timestamp, w.timestamp, selected)

	// The partition rides with the topic: they are read together, and it saves
	// a column.
	topic := fmt.Sprintf("%s[%d]", item.record.Topic, item.record.Partition)
	topic = panel.TruncateLeft(topic, w.topic)

	offset := strconv.FormatInt(item.record.Offset, 10)
	key := panel.TruncateLeft(item.key.Line, w.key)
	value := panel.Truncate(item.value.Line, w.value)

	if selected {
		style := th.TableRowSelected
		if !r.focused {
			style = th.TableRowUnfocused
		}
		return style.Render(strings.Join([]string{
			padRight(plainTime(item.record.Timestamp, r.format), w.timestamp),
			padLeft(topic, w.topic),
			padLeft(offset, w.offset),
			padLeft(key, w.key),
			padRight(value, w.value),
		}, "  "))
	}

	return strings.Join([]string{
		timestamp,
		th.Dim.Render(padLeft(topic, w.topic)),
		th.Value.Render(padLeft(offset, w.offset)),
		th.Success.Render(padLeft(key, w.key)),
		th.Value.Render(value),
	}, "  ")
}

// formatTime colours the parts of a timestamp differently, so a change in the
// seconds is easy to spot in a fast-scrolling list. Idea from yozefu.
func (r *recordList) formatTime(th *theme.Theme, ts time.Time, width int, plain bool) string {
	text := plainTime(ts, r.format)
	if plain || r.format == timeAgo {
		return th.Dim.Render(padRight(text, width))
	}

	if r.format == timeFull {
		// 2026-08-04T12:06:01.884+03:00 — date, then time, then zone.
		date, rest, found := strings.Cut(text, "T")
		if !found {
			return th.Dim.Render(padRight(text, width))
		}
		clock, zone := splitZone(rest)
		return padRight(
			lipgloss.NewStyle().Foreground(th.Warn).Render(date)+
				th.Dim.Render("T")+
				th.Label.Render(clock)+
				th.Success.Render(zone), width)
	}

	// 12:06:01.884 — the milliseconds dimmed so the seconds stand out.
	clock, millis, found := strings.Cut(text, ".")
	if !found {
		return th.Label.Render(padRight(text, width))
	}
	return padRight(th.Label.Render(clock)+th.Dim.Render("."+millis), width)
}

func plainTime(ts time.Time, format timeFormat) string {
	switch format {
	case timeFull:
		return ts.Format("2006-01-02T15:04:05.000Z07:00")
	case timeAgo:
		return humanSince(time.Since(ts))
	default:
		return ts.Format("15:04:05.000")
	}
}

func splitZone(s string) (clock, zone string) {
	for i, r := range s {
		if r == '+' || r == '-' || r == 'Z' {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

// humanSince renders an age the way a person would say it.
func humanSince(d time.Duration) string {
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func padRight(s string, width int) string {
	if w := lipgloss.Width(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}

func padLeft(s string, width int) string {
	if w := lipgloss.Width(s); w < width {
		return strings.Repeat(" ", width-w) + s
	}
	return s
}

func itoa(n int) string { return strconv.Itoa(n) }
