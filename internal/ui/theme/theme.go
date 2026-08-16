// Package theme holds the colour palette and the lipgloss styles built from it.
//
// Every colour in kafman is declared here. Nothing else in the UI may call
// lipgloss.Color directly: a palette scattered across call sites cannot be
// switched between light and dark, and drifts within a single screen.
package theme

import "github.com/charmbracelet/lipgloss"

// Palette is the set of semantic colours a theme provides. Names describe the
// meaning, never the colour, so a light theme can pick different values without
// the call sites becoming lies.
type Palette struct {
	Name string

	Accent lipgloss.Color // active tab, focused border, selection
	OK     lipgloss.Color // healthy state, keys, success
	Warn   lipgloss.Color // warnings, live indicator, parse errors
	Crit   lipgloss.Color // offline, deny rules, destructive actions
	Info   lipgloss.Color // timestamps, numbers
	Muted  lipgloss.Color // footer, unfocused borders, placeholders
	FG     lipgloss.Color // body text

	// SelectionFG is the text colour on a selected row; the background is Accent.
	SelectionFG lipgloss.Color

	// Series are the chart colours, one per plotted series, picked to stay
	// apart from each other rather than to mean anything. Charts cycle
	// through them by index.
	Series []lipgloss.Color
}

// Dark is the default palette. Backgrounds stay unset so the user's own
// terminal background shows through.
var Dark = Palette{
	Name:        "dark",
	Accent:      "#7571F9",
	OK:          "#00D787",
	Warn:        "#FFAF00",
	Crit:        "#FF5F5F",
	Info:        "#5FAFFF",
	Muted:       "#6C6C6C",
	FG:          "#D0D0D0",
	SelectionFG: "#FFFFFF",
	Series: []lipgloss.Color{
		"#00D787", "#5FAFFF", "#FFD75F", "#FF5F87",
		"#AF87FF", "#5FFFD7", "#FF875F", "#D787D7",
	},
}

// Light keeps the same meanings at darker luminance, so text stays legible on a
// pale background.
var Light = Palette{
	Name:        "light",
	Accent:      "#5F5FD7",
	OK:          "#00875F",
	Warn:        "#AF5F00",
	Crit:        "#D70000",
	Info:        "#0057AF",
	Muted:       "#8A8A8A",
	FG:          "#1C1C1C",
	SelectionFG: "#FFFFFF",
	Series: []lipgloss.Color{
		"#00875F", "#005FD7", "#AF8700", "#D70057",
		"#8700D7", "#00868B", "#D75F00", "#AF00AF",
	},
}

// Theme is a palette plus the styles derived from it.
type Theme struct {
	Palette

	// PlainFonts replaces Nerd Font glyphs with ASCII throughout.
	PlainFonts bool

	// Text styles.
	Base      lipgloss.Style
	Title     lipgloss.Style
	Label     lipgloss.Style
	Value     lipgloss.Style
	Dim       lipgloss.Style
	Success   lipgloss.Style
	Warning   lipgloss.Style
	Critical  lipgloss.Style
	Highlight lipgloss.Style

	// Tab bar.
	TabActive   lipgloss.Style
	TabInactive lipgloss.Style

	// Panels.
	PanelFocused   lipgloss.Style
	PanelUnfocused lipgloss.Style
	PanelTitle     lipgloss.Style

	// Tables.
	TableHeader       lipgloss.Style
	TableRow          lipgloss.Style
	TableRowSelected  lipgloss.Style
	TableRowUnfocused lipgloss.Style

	// Status bar.
	ShortcutKey   lipgloss.Style
	ShortcutLabel lipgloss.Style

	// Modals.
	ModalBorder lipgloss.Style
}

// New builds a theme from a palette.
func New(p Palette, plainFonts bool) *Theme {
	t := &Theme{Palette: p, PlainFonts: plainFonts}

	t.Base = lipgloss.NewStyle().Foreground(p.FG)
	t.Title = lipgloss.NewStyle().Foreground(p.Accent).Bold(true)
	t.Label = lipgloss.NewStyle().Foreground(p.Info)
	t.Value = lipgloss.NewStyle().Foreground(p.FG)
	t.Dim = lipgloss.NewStyle().Foreground(p.Muted)
	t.Success = lipgloss.NewStyle().Foreground(p.OK)
	t.Warning = lipgloss.NewStyle().Foreground(p.Warn)
	t.Critical = lipgloss.NewStyle().Foreground(p.Crit)
	t.Highlight = lipgloss.NewStyle().Foreground(p.Accent).Bold(true)

	t.TabActive = lipgloss.NewStyle().Foreground(p.Accent).Bold(true).Padding(0, 2)
	t.TabInactive = lipgloss.NewStyle().Foreground(p.Muted).Padding(0, 2)

	// A focused panel gets a thick border in the accent colour: the single most
	// important cue in the whole interface is where the keyboard is pointing.
	t.PanelFocused = lipgloss.NewStyle().
		Border(lipgloss.ThickBorder()).
		BorderForeground(p.Accent)
	t.PanelUnfocused = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(p.Muted)
	t.PanelTitle = lipgloss.NewStyle().Foreground(p.Accent).Bold(true)

	t.TableHeader = lipgloss.NewStyle().Foreground(p.Muted).Bold(true)
	t.TableRow = lipgloss.NewStyle().Foreground(p.FG)
	t.TableRowSelected = lipgloss.NewStyle().
		Foreground(p.SelectionFG).Background(p.Accent).Bold(true)
	// Selection in an unfocused panel stays visible but recedes.
	t.TableRowUnfocused = lipgloss.NewStyle().Foreground(p.FG).Bold(true)

	t.ShortcutKey = lipgloss.NewStyle().Foreground(p.FG).Bold(true)
	t.ShortcutLabel = lipgloss.NewStyle().Foreground(p.Muted)

	t.ModalBorder = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(p.Accent).
		Padding(1, 2)

	return t
}

// ByName returns the named theme, falling back to Dark for anything unknown.
func ByName(name string, plainFonts bool) *Theme {
	switch name {
	case "light":
		return New(Light, plainFonts)
	default:
		return New(Dark, plainFonts)
	}
}

// ClusterChip renders a cluster name on its configured colour. The text colour
// is picked for contrast, so a pale user-chosen background stays readable.
// SeriesStyle colours the i-th plotted series; indexes wrap around the
// palette, so any number of series can ask.
func (t *Theme) SeriesStyle(i int) lipgloss.Style {
	if len(t.Series) == 0 {
		return t.Value
	}
	return lipgloss.NewStyle().Foreground(t.Series[((i%len(t.Series))+len(t.Series))%len(t.Series)])
}

func (t *Theme) ClusterChip(name, colour string) string {
	if colour == "" {
		colour = string(t.Accent)
	}
	style := lipgloss.NewStyle().
		Background(lipgloss.Color(colour)).
		Foreground(lipgloss.Color(contrastingText(colour))).
		Bold(true).
		Padding(0, 1)
	return style.Render(name)
}

// contrastingText picks black or white for a background, using the perceptual
// luminance of the colour rather than a naive average.
func contrastingText(hex string) string {
	r, g, b, ok := parseHex(hex)
	if !ok {
		return "#FFFFFF"
	}
	// ITU-R BT.601 luma coefficients: the eye is far more sensitive to green.
	luma := 0.299*float64(r) + 0.587*float64(g) + 0.114*float64(b)
	if luma > 140 {
		return "#000000"
	}
	return "#FFFFFF"
}

func parseHex(s string) (r, g, b int, ok bool) {
	if len(s) != 7 || s[0] != '#' {
		return 0, 0, 0, false
	}
	var values [3]int
	for i := range 3 {
		hi, ok1 := hexDigit(s[1+i*2])
		lo, ok2 := hexDigit(s[2+i*2])
		if !ok1 || !ok2 {
			return 0, 0, 0, false
		}
		values[i] = hi<<4 | lo
	}
	return values[0], values[1], values[2], true
}

func hexDigit(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10, true
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10, true
	}
	return 0, false
}

// Glyph returns the Nerd Font symbol, or its ASCII stand-in when plain fonts
// are in force. Both strings must render to the same width.
func (t *Theme) Glyph(nerd, plain string) string {
	if t.PlainFonts {
		return plain
	}
	return nerd
}
