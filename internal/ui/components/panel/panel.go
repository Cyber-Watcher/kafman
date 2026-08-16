// Package panel draws a bordered box with a title and optional counters
// embedded in the border line.
//
// Putting counters like "[ 132 / 8_412 ]" into the border rather than a content
// row saves a line per panel and reads as deliberate — the idea comes from
// ktea's embedded border text and yozefu's record counter.
package panel

import (
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Border characters for the two focus states.
type borderSet struct {
	topLeft, topRight, bottomLeft, bottomRight string
	horizontal, vertical                       string
}

var (
	roundedBorder = borderSet{"╭", "╮", "╰", "╯", "─", "│"}
	thickBorder   = borderSet{"┏", "┓", "┗", "┛", "━", "┃"}
)

// Panel describes one bordered box.
type Panel struct {
	// Title appears at the top left, inside the border.
	Title string

	// TopRight is an optional counter drawn into the top border, right-aligned.
	TopRight string

	// Focused switches to the thick accent border.
	Focused bool

	// Width and Height are the outer dimensions, borders included.
	Width  int
	Height int
}

// Render draws the panel around content, which is clipped or padded to fit.
func (p Panel) Render(th *theme.Theme, content string) string {
	if p.Width < 4 || p.Height < 2 {
		return ""
	}

	border := roundedBorder
	borderStyle := lipgloss.NewStyle().Foreground(th.Muted)
	titleStyle := th.Dim
	if p.Focused {
		border = thickBorder
		borderStyle = lipgloss.NewStyle().Foreground(th.Accent)
		titleStyle = th.PanelTitle
	}

	inner := p.Width - 2 // space between the vertical borders

	var b strings.Builder
	b.WriteString(p.topBorder(th, border, borderStyle, titleStyle, inner))
	b.WriteString("\n")

	lines := fitContent(content, inner, p.Height-2)
	vertical := borderStyle.Render(border.vertical)
	for _, line := range lines {
		b.WriteString(vertical + line + vertical + "\n")
	}

	b.WriteString(borderStyle.Render(
		border.bottomLeft + strings.Repeat(border.horizontal, inner) + border.bottomRight))
	return b.String()
}

// topBorder builds "╭─ Title ────────[ counter ]─╮", dropping pieces that do
// not fit rather than overflowing the width.
func (p Panel) topBorder(_ *theme.Theme, border borderSet, borderStyle, titleStyle lipgloss.Style, inner int) string {
	const (
		leadIn      = 1 // the dash before the title
		titlePad    = 2 // the spaces around the title
		counterPad  = 1 // the dash after the counter
		minFillDash = 1
	)

	title, counter := p.Title, p.TopRight

	// The title is more important than the counter: drop the counter first.
	titleWidth := 0
	if title != "" {
		titleWidth = leadIn + titlePad + lipgloss.Width(title)
	}
	counterWidth := 0
	if counter != "" {
		counterWidth = lipgloss.Width(counter) + titlePad + counterPad
	}
	if titleWidth+counterWidth+minFillDash > inner {
		counter, counterWidth = "", 0
	}
	if titleWidth+minFillDash > inner {
		title, titleWidth = "", 0
	}

	fill := inner - titleWidth - counterWidth
	if fill < 0 {
		fill = 0
	}

	var b strings.Builder
	b.WriteString(borderStyle.Render(border.topLeft))
	if title != "" {
		b.WriteString(borderStyle.Render(border.horizontal))
		b.WriteString(" " + titleStyle.Render(title) + " ")
	}
	b.WriteString(borderStyle.Render(strings.Repeat(border.horizontal, fill)))
	if counter != "" {
		b.WriteString(" " + titleStyle.Render(counter) + " ")
		b.WriteString(borderStyle.Render(border.horizontal))
	}
	b.WriteString(borderStyle.Render(border.topRight))
	return b.String()
}

// fitContent clips or pads content to exactly height lines of exactly width
// columns, measuring in display cells so ANSI codes do not count.
func fitContent(content string, width, height int) []string {
	if height < 0 {
		height = 0
	}
	raw := strings.Split(content, "\n")
	out := make([]string, 0, height)

	for i := range height {
		line := ""
		if i < len(raw) {
			line = raw[i]
		}
		out = append(out, fitLine(line, width))
	}
	return out
}

// fitLine pads a line with spaces, or truncates it with an ellipsis.
func fitLine(line string, width int) string {
	if width <= 0 {
		return ""
	}
	w := lipgloss.Width(line)
	switch {
	case w == width:
		return line
	case w < width:
		return line + strings.Repeat(" ", width-w)
	default:
		return Truncate(line, width)
	}
}

// Truncate shortens a string to width display cells, ending with "…" when it
// had to cut. ANSI escape sequences are copied through without being counted.
func Truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}

	var (
		b        strings.Builder
		consumed int
		inEscape bool
	)
	for _, r := range s {
		if r == '\x1b' {
			inEscape = true
		}
		if inEscape {
			b.WriteRune(r)
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEscape = false
			}
			continue
		}
		runeWidth := lipgloss.Width(string(r))
		if consumed+runeWidth > width-1 {
			break
		}
		b.WriteRune(r)
		consumed += runeWidth
	}
	// A style opened inside the part that was cut would bleed into whatever
	// follows, so it is closed — but only when the string carried one. An
	// unconditional reset would end the style of the row this value is embedded
	// in, leaving the rest of the line unstyled and the escape itself visible in
	// some terminals.
	if strings.Contains(b.String(), "\x1b") {
		return b.String() + "\x1b[0m" + "…"
	}
	return b.String() + "…"
}

// TruncateLeft shortens from the front: "…rders.events[3]".
//
// Kafka topic names are usually distinguished by their tail (env.domain.event),
// so cutting the head keeps the informative part. Idea from yozefu.
func TruncateLeft(s string, width int) string {
	if width <= 0 {
		return ""
	}
	w := lipgloss.Width(s)
	if w <= width {
		return s
	}

	runes := []rune(s)
	// Walk back from the end until the remainder plus the ellipsis fits.
	consumed := 0
	cut := len(runes)
	for i := len(runes) - 1; i >= 0; i-- {
		rw := lipgloss.Width(string(runes[i]))
		if consumed+rw > width-1 {
			break
		}
		consumed += rw
		cut = i
	}
	return "…" + string(runes[cut:])
}
