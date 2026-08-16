// Package form builds keyboard-driven forms out of labelled fields.
//
// Text entry is delegated to bubbles/textinput, which already handles cursor
// movement, editing and password masking. What this package adds is the layout,
// the focus cycle, sections, checkboxes, radio groups and per-field validation
// that runs as you type.
package form

import (
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Validator checks a value as it is typed. Returning an error marks the field,
// it does not block typing: interrupting someone mid-word is worse than showing
// a transient complaint.
type Validator func(string) error

// Field is one row of a form.
type Field interface {
	// Focusable reports whether the field takes part in the focus cycle;
	// section headers do not.
	Focusable() bool

	Focus()
	Blur()

	Update(msg tea.Msg) tea.Cmd
	View(th *theme.Theme, labelWidth, width int, focused bool) string

	// Err is the current validation error, or nil.
	Err() error
}

// ---------------------------------------------------------------- section ---

// Section is a non-focusable heading that groups the fields below it.
type Section struct{ Title string }

// NewSection creates a heading.
func NewSection(title string) *Section { return &Section{Title: title} }

func (s *Section) Focusable() bool        { return false }
func (s *Section) Focus()                 {}
func (s *Section) Blur()                  {}
func (s *Section) Update(tea.Msg) tea.Cmd { return nil }
func (s *Section) Err() error             { return nil }

func (s *Section) View(th *theme.Theme, _, width int, _ bool) string {
	rule := ""
	if pad := width - lipgloss.Width(s.Title) - 5; pad > 0 {
		rule = " " + strings.Repeat("─", pad)
	}
	return th.Dim.Render("── " + s.Title + rule)
}

// ------------------------------------------------------------------- text ---

// Text is a single-line text field.
type Text struct {
	Label    string
	Hint     string
	input    textinput.Model
	validate Validator
	err      error
}

// NewText creates a text field.
func NewText(label, value string) *Text {
	in := textinput.New()
	in.SetValue(value)
	in.Prompt = ""
	return &Text{Label: label, input: in}
}

// NewPassword creates a masked field. The value is never rendered in clear.
func NewPassword(label, value string) *Text {
	t := NewText(label, value)
	t.input.EchoMode = textinput.EchoPassword
	t.input.EchoCharacter = '•'
	return t
}

// WithHint attaches a hint shown to the right of the field.
func (t *Text) WithHint(hint string) *Text { t.Hint = hint; return t }

// WithPlaceholder sets the text shown while the field is empty.
func (t *Text) WithPlaceholder(s string) *Text { t.input.Placeholder = s; return t }

// WithValidator attaches a check that runs as the field is edited.
//
// It deliberately does not run now: an untouched empty field is not a mistake
// yet, and opening a form with every required field already marked in red
// teaches people to ignore the marks. The form validates everything again when
// a button is pressed, which is the moment an empty field really is wrong.
func (t *Text) WithValidator(v Validator) *Text {
	t.validate = v
	return t
}

// Value returns the current text.
func (t *Text) Value() string { return t.input.Value() }

// SetValue replaces the text.
func (t *Text) SetValue(s string) {
	t.input.SetValue(s)
	t.runValidator()
}

func (t *Text) Focusable() bool { return true }
func (t *Text) Focus()          { t.input.Focus() }
func (t *Text) Blur()           { t.input.Blur() }
func (t *Text) Err() error      { return t.err }

func (t *Text) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	t.input, cmd = t.input.Update(msg)
	t.runValidator()
	return cmd
}

func (t *Text) runValidator() {
	if t.validate == nil {
		return
	}
	t.err = t.validate(t.input.Value())
}

func (t *Text) View(th *theme.Theme, labelWidth, width int, focused bool) string {
	// The hint or the error shares the line with the input, so the input cannot
	// have all of the width: it would push the explanation past the panel, where
	// it is cut off — and a hint nobody sees is the same as no hint.
	trailing, trailingStyle := "", th.Dim
	switch {
	case t.err != nil:
		trailing, trailingStyle = "✖ "+t.err.Error(), th.Critical
	case t.Hint != "":
		trailing = t.Hint
	}

	const brackets = 6 // "[ " + " ]" plus the space after the label
	reserved := brackets
	if trailing != "" {
		reserved += lipgloss.Width(trailing) + 2
	}
	t.input.Width = max(width-labelWidth-reserved, 10)

	label := th.Label.Render(padRight(t.Label, labelWidth))
	if focused {
		label = th.Highlight.Render(padRight(t.Label, labelWidth))
	}

	line := label + " " + "[ " + t.input.View() + " ]"
	if trailing == "" {
		return line
	}

	// A hint longer than what is left is trimmed rather than allowed to
	// overflow; the field itself keeps its minimum width.
	room := width - lipgloss.Width(line) - 2
	if room < 4 {
		return line
	}
	return line + "  " + trailingStyle.Render(truncate(trailing, room))
}

// truncate shortens s to width, marking the cut.
func truncate(s string, width int) string {
	if lipgloss.Width(s) <= width {
		return s
	}
	runes := []rune(s)
	if width <= 1 {
		return "…"
	}
	return string(runes[:width-1]) + "…"
}

// --------------------------------------------------------------- checkbox ---

// Checkbox is a boolean field toggled with space.
type Checkbox struct {
	Label   string
	Hint    string
	checked bool
}

// NewCheckbox creates a checkbox.
func NewCheckbox(label string, checked bool) *Checkbox {
	return &Checkbox{Label: label, checked: checked}
}

// WithHint attaches a hint.
func (c *Checkbox) WithHint(hint string) *Checkbox { c.Hint = hint; return c }

// Checked reports the state.
func (c *Checkbox) Checked() bool { return c.checked }

// SetChecked sets the state.
func (c *Checkbox) SetChecked(v bool) { c.checked = v }

func (c *Checkbox) Focusable() bool { return true }
func (c *Checkbox) Focus()          {}
func (c *Checkbox) Blur()           {}
func (c *Checkbox) Err() error      { return nil }

func (c *Checkbox) Update(msg tea.Msg) tea.Cmd {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case " ", "enter", "x":
			c.checked = !c.checked
		}
	}
	return nil
}

func (c *Checkbox) View(th *theme.Theme, labelWidth, _ int, focused bool) string {
	label := th.Label.Render(padRight(c.Label, labelWidth))
	if focused {
		label = th.Highlight.Render(padRight(c.Label, labelWidth))
	}

	mark := "[ ]"
	if c.checked {
		mark = th.Success.Render("[x]")
	}
	line := label + " " + mark
	if c.Hint != "" {
		line += "  " + th.Dim.Render(c.Hint)
	}
	return line
}

// ------------------------------------------------------------------ radio ---

// Radio is a one-of-many choice, changed with the left and right arrows.
type Radio struct {
	Label    string
	Options  []string
	selected int
}

// NewRadio creates a radio group.
func NewRadio(label string, options []string, selected int) *Radio {
	return &Radio{Label: label, Options: options, selected: max(min(selected, len(options)-1), 0)}
}

// Selected returns the chosen option.
func (r *Radio) Selected() string {
	if len(r.Options) == 0 {
		return ""
	}
	return r.Options[r.selected]
}

// SelectValue picks the option equal to value, if present.
func (r *Radio) SelectValue(value string) {
	for i, o := range r.Options {
		if o == value {
			r.selected = i
			return
		}
	}
}

func (r *Radio) Focusable() bool { return true }
func (r *Radio) Focus()          {}
func (r *Radio) Blur()           {}
func (r *Radio) Err() error      { return nil }

func (r *Radio) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok || len(r.Options) == 0 {
		return nil
	}
	switch key.String() {
	case "left", "h":
		r.selected = (r.selected - 1 + len(r.Options)) % len(r.Options)
	case "right", "l", " ":
		r.selected = (r.selected + 1) % len(r.Options)
	}
	return nil
}

func (r *Radio) View(th *theme.Theme, labelWidth, width int, focused bool) string {
	label := th.Label.Render(padRight(r.Label, labelWidth))
	if focused {
		label = th.Highlight.Render(padRight(r.Label, labelWidth))
	}

	// Options wrap onto further lines instead of running off the panel: an ACL
	// operation is one of eleven, and eleven of them do not fit a terminal.
	const gap = 2
	available := max(width-labelWidth-1, 20)

	var lines []string
	var line strings.Builder
	lineWidth := 0

	for i, option := range r.Options {
		text := "( ) " + option
		style := th.Dim
		if i == r.selected {
			text, style = "(•) "+option, th.Success
		}

		if lineWidth > 0 && lineWidth+gap+len(text) > available {
			lines = append(lines, line.String())
			line.Reset()
			lineWidth = 0
		}
		if lineWidth > 0 {
			line.WriteString(strings.Repeat(" ", gap))
			lineWidth += gap
		}
		line.WriteString(style.Render(text))
		lineWidth += len(text)
	}
	lines = append(lines, line.String())

	indent := strings.Repeat(" ", labelWidth+1)
	for i := 1; i < len(lines); i++ {
		lines[i] = indent + lines[i]
	}
	return label + " " + strings.Join(lines, "\n")
}

// ------------------------------------------------------------------- form ---

// Model is a form: a list of fields with a focus cycle and buttons.
type Model struct {
	fields  []Field
	buttons []string

	focus      int // index into fields; len(fields)+i selects button i
	labelWidth int

	submitted string
	cancelled bool
}

// New builds a form. Buttons are rendered in order at the bottom.
func New(fields []Field, buttons ...string) *Model {
	m := &Model{fields: fields, buttons: buttons, focus: -1}
	// Labels line up in one column, sized to the longest of them.
	for _, f := range fields {
		switch field := f.(type) {
		case *Text:
			m.labelWidth = max(m.labelWidth, len(field.Label))
		case *Checkbox:
			m.labelWidth = max(m.labelWidth, len(field.Label))
		case *Radio:
			m.labelWidth = max(m.labelWidth, len(field.Label))
		}
	}
	m.labelWidth = max(m.labelWidth, 14)
	m.focusFirst()
	return m
}

// Fields returns the fields, for callers that keep typed references.
func (m *Model) Fields() []Field { return m.fields }

// Submitted returns the label of the pressed button, or "" if none was.
func (m *Model) Submitted() string { return m.submitted }

// Cancelled reports whether the form was dismissed.
func (m *Model) Cancelled() bool { return m.cancelled }

// FocusedButton returns the label of the button that has focus, or "" when a
// field does.
func (m *Model) FocusedButton() string {
	if m.focus < len(m.fields) {
		return ""
	}
	return m.buttons[m.focus-len(m.fields)]
}

// Reset clears the submitted and cancelled flags.
func (m *Model) Reset() { m.submitted, m.cancelled = "", false }

// Valid reports whether every field passes its validator.
func (m *Model) Valid() bool {
	for _, f := range m.fields {
		if f.Err() != nil {
			return false
		}
	}
	return true
}

// Errors lists the current validation failures.
func (m *Model) Errors() []error {
	var errs []error
	for _, f := range m.fields {
		if err := f.Err(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (m *Model) focusFirst() {
	for i, f := range m.fields {
		if f.Focusable() {
			m.setFocus(i)
			return
		}
	}
}

func (m *Model) setFocus(index int) {
	if m.focus >= 0 && m.focus < len(m.fields) {
		m.fields[m.focus].Blur()
	}
	m.focus = index
	if index >= 0 && index < len(m.fields) {
		m.fields[index].Focus()
	}
}

// moveFocus advances through fields and then buttons, skipping headings.
func (m *Model) moveFocus(delta int) {
	total := len(m.fields) + len(m.buttons)
	if total == 0 {
		return
	}
	next := m.focus
	for range total {
		next = (next + delta + total) % total
		if next >= len(m.fields) || m.fields[next].Focusable() {
			m.setFocus(next)
			return
		}
	}
}

// Update routes keys: navigation is handled here, everything else goes to the
// focused field.
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		if m.focus >= 0 && m.focus < len(m.fields) {
			return m.fields[m.focus].Update(msg)
		}
		return nil
	}

	switch key.String() {
	case "esc":
		m.cancelled = true
		return nil

	case "tab", "down":
		m.moveFocus(1)
		return nil

	case "shift+tab", "up":
		m.moveFocus(-1)
		return nil

	case "enter":
		// On a button, press it. In a field, move on — Enter as "next" is what
		// people expect in a form.
		if m.focus >= len(m.fields) {
			// Pressing a button is the moment to judge fields nobody touched.
			m.validateAll()
			m.submitted = m.buttons[m.focus-len(m.fields)]
			return nil
		}
		if _, isCheckbox := m.currentField().(*Checkbox); !isCheckbox {
			m.moveFocus(1)
			return nil
		}
	}

	if f := m.currentField(); f != nil {
		return f.Update(msg)
	}
	return nil
}

// validateAll runs every field's validator, so a form submitted without being
// filled in marks what is missing instead of failing silently.
func (m *Model) validateAll() {
	for _, f := range m.fields {
		if text, ok := f.(*Text); ok {
			text.runValidator()
		}
	}
}

func (m *Model) currentField() Field {
	if m.focus < 0 || m.focus >= len(m.fields) {
		return nil
	}
	return m.fields[m.focus]
}

// View renders the form.
func (m *Model) View(th *theme.Theme, width int) string {
	lines := make([]string, 0, len(m.fields)+2)
	for i, f := range m.fields {
		lines = append(lines, f.View(th, m.labelWidth, width, i == m.focus))
	}

	if len(m.buttons) > 0 {
		rendered := make([]string, len(m.buttons))
		for i, b := range m.buttons {
			style := th.Dim
			if m.focus == len(m.fields)+i {
				style = th.Highlight
			}
			rendered[i] = style.Render("[ " + b + " ]")
		}
		lines = append(lines, "", lipgloss.PlaceHorizontal(width, lipgloss.Center,
			strings.Join(rendered, "   ")))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// Height is how many lines the form will occupy.
func (m *Model) Height() int {
	h := len(m.fields)
	if len(m.buttons) > 0 {
		h += 2
	}
	return h
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
