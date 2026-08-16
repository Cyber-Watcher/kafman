// Package table renders a scrollable, sortable, filterable table.
//
// bubbles/table exists but fixes column widths up front and offers no filtering
// or sorting, both of which kafman needs on every screen. This implementation
// also exposes the visible window, so callers can fetch details only for the
// rows actually on screen — the trick that keeps kafe responsive with hundreds
// of topics.
package table

import (
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Alignment of a column's content.
type Alignment int

const (
	Left Alignment = iota
	Right
)

// Column describes one column.
type Column struct {
	Title string

	// Width is the fixed width in cells. Zero means the column takes whatever
	// is left; at most one column should do that.
	Width int

	Align Alignment

	// TruncateFront cuts long values from the left instead of the right, for
	// values whose tail is the informative part (Kafka topic names).
	TruncateFront bool
}

// Row is one line of cells, matching the column order.
type Row struct {
	Cells []string

	// Style, when set, overrides the row style — used to colour an unhealthy
	// row without the table knowing what "unhealthy" means.
	Style *lipgloss.Style

	// CellStyles, when set, style individual cells by index; nil entries keep
	// the row's style. Ignored while the row is selected — the selection bar
	// owns the whole line, exactly so a coloured cell cannot poke a hole in it.
	CellStyles []*lipgloss.Style

	// Data carries the caller's own object, so the selected row can be mapped
	// back without a parallel lookup.
	Data any
}

// Model is a table.
type Model struct {
	columns []Column
	rows    []Row

	// filtered indexes into rows; nil when no filter is active.
	filtered []int

	cursor  int
	offset  int
	focused bool

	filter    string
	sortCol   int
	sortDesc  bool
	sortable  bool
	pageSize  int
	lastWidth int
}

// New creates a table with the given columns.
func New(columns ...Column) *Model {
	return &Model{columns: columns, sortCol: -1, sortable: true}
}

// SetRows replaces the contents, keeping the cursor on the same row where
// possible so a periodic refresh does not move the selection under the user.
func (m *Model) SetRows(rows []Row) {
	previous := m.SelectedRow()
	m.rows = rows
	m.applyFilter()
	m.applySort()

	if previous != nil && len(previous.Cells) > 0 {
		if idx := m.indexOfFirstCell(previous.Cells[0]); idx >= 0 {
			m.cursor = idx
		}
	}
	m.clampCursor()
}

// Rows returns the unfiltered rows.
func (m *Model) Rows() []Row { return m.rows }

// Len is the number of rows currently visible through the filter.
func (m *Model) Len() int {
	if m.filtered != nil {
		return len(m.filtered)
	}
	return len(m.rows)
}

// TotalLen is the number of rows before filtering.
func (m *Model) TotalLen() int { return len(m.rows) }

// SelectedRow returns the row under the cursor, or nil when the table is empty.
func (m *Model) SelectedRow() *Row {
	if m.Len() == 0 || m.cursor < 0 || m.cursor >= m.Len() {
		return nil
	}
	row := m.rowAt(m.cursor)
	return &row
}

// Cursor is the index of the selection within the filtered rows.
func (m *Model) Cursor() int { return m.cursor }

// VisibleRange returns the half-open interval of rows currently on screen.
// Callers use it to fetch details only for what the user can actually see.
func (m *Model) VisibleRange() (start, end int) {
	end = min(m.offset+m.pageSize, m.Len())
	return m.offset, end
}

// Filter returns the active filter string.
func (m *Model) Filter() string { return m.filter }

// SetFilter narrows the table to rows containing s in any cell.
func (m *Model) SetFilter(s string) {
	m.filter = s
	m.applyFilter()
	m.applySort()
	m.clampCursor()
}

// SetSortable turns the F3 sort cycle on or off.
func (m *Model) SetSortable(v bool) { m.sortable = v }

// Focus and Blur implement ui.Focusable.
func (m *Model) Focus()        { m.focused = true }
func (m *Model) Blur()         { m.focused = false }
func (m *Model) Focused() bool { return m.focused }

// Update handles navigation keys. It ignores everything else, so the parent can
// pass every key through without filtering first.
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	if !m.focused {
		return nil
	}
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return nil
	}

	switch key.String() {
	case "up", "k":
		m.MoveCursor(-1)
	case "down", "j":
		m.MoveCursor(1)
	case "pgup", "ctrl+u":
		m.MoveCursor(-m.pageSize)
	case "pgdown", "ctrl+d":
		m.MoveCursor(m.pageSize)
	case "home", "g":
		m.cursor = 0
		m.scrollToCursor()
	case "end", "G":
		m.cursor = m.Len() - 1
		m.scrollToCursor()
	case "f3":
		m.cycleSort()
	}
	return nil
}

// MoveCursor moves the selection by delta rows, clamped to the ends.
func (m *Model) MoveCursor(delta int) {
	m.cursor += delta
	m.clampCursor()
	m.scrollToCursor()
}

func (m *Model) clampCursor() {
	if m.cursor >= m.Len() {
		m.cursor = m.Len() - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
}

func (m *Model) scrollToCursor() {
	if m.pageSize <= 0 {
		return
	}
	switch {
	case m.cursor < m.offset:
		m.offset = m.cursor
	case m.cursor >= m.offset+m.pageSize:
		m.offset = m.cursor - m.pageSize + 1
	}
	if m.offset < 0 {
		m.offset = 0
	}
}

func (m *Model) cycleSort() {
	if !m.sortable || len(m.columns) == 0 {
		return
	}
	switch {
	case m.sortCol < 0:
		m.sortCol, m.sortDesc = 0, false
	case !m.sortDesc:
		m.sortDesc = true
	default:
		m.sortCol++
		m.sortDesc = false
		if m.sortCol >= len(m.columns) {
			m.sortCol = -1 // back to the original order
		}
	}
	m.applySort()
	m.clampCursor()
}

func (m *Model) applyFilter() {
	if m.filter == "" {
		m.filtered = nil
		return
	}
	needle := strings.ToLower(m.filter)
	m.filtered = m.filtered[:0]
	if m.filtered == nil {
		m.filtered = make([]int, 0, len(m.rows))
	}
	for i, row := range m.rows {
		for _, cell := range row.Cells {
			if strings.Contains(strings.ToLower(cell), needle) {
				m.filtered = append(m.filtered, i)
				break
			}
		}
	}
}

func (m *Model) applySort() {
	if m.sortCol < 0 || m.sortCol >= len(m.columns) {
		return
	}
	col := m.sortCol

	less := func(a, b Row) bool {
		if col >= len(a.Cells) || col >= len(b.Cells) {
			return false
		}
		return naturalLess(a.Cells[col], b.Cells[col])
	}

	if m.filtered == nil {
		sort.SliceStable(m.rows, func(i, j int) bool {
			if m.sortDesc {
				return less(m.rows[j], m.rows[i])
			}
			return less(m.rows[i], m.rows[j])
		})
		return
	}
	sort.SliceStable(m.filtered, func(i, j int) bool {
		a, b := m.rows[m.filtered[i]], m.rows[m.filtered[j]]
		if m.sortDesc {
			return less(b, a)
		}
		return less(a, b)
	})
}

func (m *Model) rowAt(i int) Row {
	if m.filtered != nil {
		return m.rows[m.filtered[i]]
	}
	return m.rows[i]
}

func (m *Model) indexOfFirstCell(value string) int {
	for i := range m.Len() {
		row := m.rowAt(i)
		if len(row.Cells) > 0 && row.Cells[0] == value {
			return i
		}
	}
	return -1
}

// View renders the table into width columns and height rows, the header
// included.
func (m *Model) View(th *theme.Theme, width, height int) string {
	if height < 2 || width < 10 {
		return ""
	}
	m.pageSize = height - 1 // one line goes to the header
	m.lastWidth = width
	m.scrollToCursor()

	widths := m.columnWidths(width)

	var b strings.Builder
	b.WriteString(m.header(th, widths))

	start, end := m.VisibleRange()
	for i := start; i < end; i++ {
		b.WriteString("\n")
		b.WriteString(m.renderRow(th, m.rowAt(i), widths, i == m.cursor))
	}

	// Pad so the panel below does not shift as rows come and go.
	for i := end - start; i < m.pageSize; i++ {
		b.WriteString("\n")
	}
	return b.String()
}

// columnWidths distributes the available width: fixed columns keep their size,
// and one flexible column absorbs the rest.
func (m *Model) columnWidths(total int) []int {
	const gap = 1

	widths := make([]int, len(m.columns))
	fixed, flexible := 0, -1
	for i, c := range m.columns {
		widths[i] = c.Width
		if c.Width == 0 {
			flexible = i
			continue
		}
		fixed += c.Width
	}

	gaps := gap * (len(m.columns) - 1)
	if flexible >= 0 {
		widths[flexible] = max(total-fixed-gaps, 3)
		return widths
	}

	// No flexible column: shrink everything proportionally if it overflows.
	if fixed+gaps > total && fixed > 0 {
		scale := float64(total-gaps) / float64(fixed)
		for i := range widths {
			widths[i] = max(int(float64(widths[i])*scale), 3)
		}
	}
	return widths
}

func (m *Model) header(th *theme.Theme, widths []int) string {
	cells := make([]string, len(m.columns))
	for i, c := range m.columns {
		title := c.Title
		if i == m.sortCol {
			// The arrow points the way the rows are ordered.
			if m.sortDesc {
				title += " ▼"
			} else {
				title += " ▲"
			}
		}
		cells[i] = align(title, widths[i], c.Align, false)
	}
	return th.TableHeader.Render(strings.Join(cells, " "))
}

func (m *Model) renderRow(th *theme.Theme, row Row, widths []int, selected bool) string {
	cells := make([]string, len(m.columns))
	for i, c := range m.columns {
		value := ""
		if i < len(row.Cells) {
			value = row.Cells[i]
		}
		cells[i] = align(value, widths[i], c.Align, c.TruncateFront)
	}
	if selected {
		line := strings.Join(cells, " ")
		if m.focused {
			return th.TableRowSelected.Render(line)
		}
		return th.TableRowUnfocused.Render(line)
	}

	base := th.TableRow
	if row.Style != nil {
		base = *row.Style
	}
	if len(row.CellStyles) == 0 {
		return base.Render(strings.Join(cells, " "))
	}

	// Cells are styled one by one: a nested style's reset sequence would cut a
	// whole-line style short, so there is no whole-line style here.
	for i := range cells {
		style := base
		if i < len(row.CellStyles) && row.CellStyles[i] != nil {
			style = *row.CellStyles[i]
		}
		cells[i] = style.Render(cells[i])
	}
	return strings.Join(cells, " ")
}

func align(s string, width int, a Alignment, truncateFront bool) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) > width {
		if truncateFront {
			s = panel.TruncateLeft(s, width)
		} else {
			s = panel.Truncate(s, width)
		}
	}
	pad := width - lipgloss.Width(s)
	if pad <= 0 {
		return s
	}
	if a == Right {
		return strings.Repeat(" ", pad) + s
	}
	return s + strings.Repeat(" ", pad)
}

// naturalLess compares two cells, ordering numerically when both look like
// numbers so "9" sorts before "10" rather than after it.
func naturalLess(a, b string) bool {
	an, aok := parseNumber(a)
	bn, bok := parseNumber(b)
	if aok && bok {
		return an < bn
	}
	return strings.ToLower(a) < strings.ToLower(b)
}

// parseNumber reads an integer, tolerating the "_" thousands separators kafman
// renders and a trailing unit.
func parseNumber(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	var (
		value  int64
		digits int
	)
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			value = value*10 + int64(r-'0')
			digits++
		case r == '_' || r == ' ':
			// separator, keep going
		default:
			// A unit suffix ("38.1 GB") still sorts by its leading number.
			if digits > 0 {
				return value, true
			}
			return 0, false
		}
	}
	return value, digits > 0
}
