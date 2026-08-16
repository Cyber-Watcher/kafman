// Package modal draws centred dialogs on top of the current frame and provides
// the confirmation dialog used before every destructive action.
package modal

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Overlay composites foreground over background, centred.
//
// ansi.Cut is what makes this correct: slicing a styled line with plain string
// operations would cut through an escape sequence and corrupt every colour
// after it.
func Overlay(background, foreground string) string {
	bgLines := strings.Split(background, "\n")
	fgLines := strings.Split(foreground, "\n")
	if len(fgLines) == 0 {
		return background
	}

	bgWidth := 0
	for _, line := range bgLines {
		bgWidth = max(bgWidth, ansi.StringWidth(line))
	}
	fgWidth := 0
	for _, line := range fgLines {
		fgWidth = max(fgWidth, ansi.StringWidth(line))
	}

	x := max((bgWidth-fgWidth)/2, 0)
	y := max((len(bgLines)-len(fgLines))/2, 0)

	out := make([]string, len(bgLines))
	copy(out, bgLines)

	for i, fg := range fgLines {
		row := y + i
		if row >= len(out) {
			break
		}
		bg := out[row]
		left := ansi.Cut(bg, 0, x)
		if pad := x - ansi.StringWidth(left); pad > 0 {
			left += strings.Repeat(" ", pad)
		}
		right := ansi.Cut(bg, x+ansi.StringWidth(fg), bgWidth)
		out[row] = left + fg + right
	}
	return strings.Join(out, "\n")
}

// Render draws a titled dialog box.
func Render(th *theme.Theme, title, content string, width int) string {
	inner := lipgloss.JoinVertical(lipgloss.Left,
		th.Title.Render(title),
		"",
		content,
	)
	return th.ModalBorder.Width(width).Render(inner)
}

// --------------------------------------------------------------- confirm ---

// Confirm is a yes/no dialog. When Phrase is set the user must type it exactly,
// which is the standard guard against deleting a production topic by muscle
// memory.
type Confirm struct {
	Title   string
	Message string
	Details string

	// Phrase, when non-empty, must be typed to enable confirmation.
	Phrase string

	// Danger renders the action in the critical colour.
	Danger bool

	typed     string
	confirmed bool
	cancelled bool
}

// NewConfirm builds a simple yes/no dialog.
func NewConfirm(title, message string) *Confirm {
	return &Confirm{Title: title, Message: message}
}

// NewDangerConfirm builds a dialog that requires typing phrase to proceed.
func NewDangerConfirm(title, message, details, phrase string) *Confirm {
	return &Confirm{Title: title, Message: message, Details: details, Phrase: phrase, Danger: true}
}

// Confirmed reports whether the user accepted.
func (c *Confirm) Confirmed() bool { return c.confirmed }

// Cancelled reports whether the user dismissed the dialog.
func (c *Confirm) Cancelled() bool { return c.cancelled }

// Ready reports whether confirmation is currently allowed.
func (c *Confirm) Ready() bool { return c.Phrase == "" || c.typed == c.Phrase }

// Update handles typing and the accept/cancel keys.
func (c *Confirm) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return nil
	}

	switch key.Type {
	case tea.KeyEsc:
		c.cancelled = true
		return nil
	case tea.KeyEnter:
		if c.Ready() {
			c.confirmed = true
		}
		return nil
	case tea.KeyBackspace:
		if c.Phrase != "" && c.typed != "" {
			c.typed = c.typed[:len(c.typed)-1]
		}
		return nil
	case tea.KeyRunes:
		if c.Phrase != "" {
			c.typed += string(key.Runes)
			return nil
		}
		// Without a phrase to type, y/n are shortcuts.
		switch strings.ToLower(string(key.Runes)) {
		case "y":
			c.confirmed = true
		case "n":
			c.cancelled = true
		}
	}
	return nil
}

// View renders the dialog.
func (c *Confirm) View(th *theme.Theme, width int) string {
	accent := th.Warning
	if c.Danger {
		accent = th.Critical
	}

	lines := []string{accent.Render(c.Message)}
	if c.Details != "" {
		lines = append(lines, th.Dim.Render(c.Details))
	}

	if c.Phrase != "" {
		cursor := th.Highlight.Render("▏")
		box := th.Dim.Render("[ ") + th.Value.Render(c.typed) + cursor +
			strings.Repeat(" ", max(len(c.Phrase)-len(c.typed), 0)) + th.Dim.Render(" ]")
		lines = append(lines,
			"",
			th.Dim.Render("This cannot be undone. Type the name to confirm:"),
			box,
		)
	}

	action := "Confirm"
	if c.Danger {
		action = "Delete"
	}
	buttons := th.Dim.Render("[Esc] Cancel")
	if c.Ready() {
		buttons = accent.Bold(true).Render("[Enter] "+action) + "   " + buttons
	} else {
		buttons = th.Dim.Render("[Enter] "+action) + "   " + buttons
	}
	lines = append(lines, "", buttons)

	return Render(th, c.Title, lipgloss.JoinVertical(lipgloss.Left, lines...), width)
}
