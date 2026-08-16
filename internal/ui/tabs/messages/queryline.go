package messages

import (
	"errors"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/query"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// validationDelay is how long typing pauses before the query is checked.
//
// Validating on every keystroke means the error flickers while a word is still
// half-typed, which trains people to ignore it. 700 ms is long enough to finish
// a token and short enough to feel immediate. Borrowed from yozefu.
const validationDelay = 700 * time.Millisecond

// queryValidatedMsg carries the result of a debounced validation.
type queryValidatedMsg struct {
	// Text is the input the result belongs to; a stale result is discarded.
	Text string
	Err  *query.Error
}

// queryLine is the input at the bottom of the Messages tab.
type queryLine struct {
	input   textinput.Model
	focused bool

	// err is the parse failure for the current text, or nil.
	err *query.Error

	// history is the list of queries run before, oldest first.
	history []string
	// browsing is the index into history while walking it with the arrows, or
	// -1 when not browsing.
	browsing int
	// draft holds what was typed before browsing started.
	draft string

	// generation increases on every keystroke so a late validation for older
	// text can be recognised and dropped.
	generation int
}

func newQueryLine(history []string) *queryLine {
	input := textinput.New()
	input.Prompt = ""
	input.Placeholder = `key contains "order-" and value.status == "FAILED" from end - 500`

	return &queryLine{input: input, history: history, browsing: -1}
}

func (q *queryLine) Focus() tea.Cmd {
	q.focused = true
	return q.input.Focus()
}

func (q *queryLine) Blur() {
	q.focused = false
	q.input.Blur()
}

func (q *queryLine) Focused() bool { return q.focused }

// Text is the current input.
func (q *queryLine) Text() string { return q.input.Value() }

// SetText replaces the input.
func (q *queryLine) SetText(s string) {
	q.input.SetValue(s)
	q.input.CursorEnd()
}

// Err is the current parse error, or nil.
func (q *queryLine) Err() *query.Error { return q.err }

// Parse returns the parsed query, or the error.
func (q *queryLine) Parse() (*query.Query, error) {
	return query.Parse(q.input.Value())
}

// Remember adds a query to the history, most recent last, without duplicates.
func (q *queryLine) Remember(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	for i, existing := range q.history {
		if existing == text {
			q.history = append(q.history[:i], q.history[i+1:]...)
			break
		}
	}
	q.history = append(q.history, text)
	q.browsing = -1
}

// History returns the remembered queries, oldest first.
func (q *queryLine) History() []string { return q.history }

// Update handles typing and history navigation.
func (q *queryLine) Update(msg tea.Msg) tea.Cmd {
	if !q.focused {
		return nil
	}

	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.Type {
		case tea.KeyUp:
			q.browse(-1)
			return q.scheduleValidation()
		case tea.KeyDown:
			q.browse(1)
			return q.scheduleValidation()
		case tea.KeyRight:
			// At the end of the line, the right arrow accepts the suggestion.
			if q.input.Position() >= len(q.input.Value()) {
				if suggestion := q.suggestion(); suggestion != "" {
					q.SetText(suggestion)
					return q.scheduleValidation()
				}
			}
		}
	}

	var cmd tea.Cmd
	q.input, cmd = q.input.Update(msg)
	return tea.Batch(cmd, q.scheduleValidation())
}

// scheduleValidation starts a debounce timer. Each keystroke bumps the
// generation, so only the last one produces a result that is kept.
func (q *queryLine) scheduleValidation() tea.Cmd {
	q.generation++
	text := q.input.Value()

	return tea.Tick(validationDelay, func(time.Time) tea.Msg {
		var parseErr *query.Error
		if _, err := query.Parse(text); err != nil {
			if !errors.As(err, &parseErr) {
				parseErr = &query.Error{Message: err.Error()}
			}
		}
		return queryValidatedMsg{Text: text, Err: parseErr}
	})
}

// Validated applies a validation result, ignoring one for text that has since
// changed.
func (q *queryLine) Validated(msg queryValidatedMsg) {
	if msg.Text != q.input.Value() {
		return
	}
	q.err = msg.Err
}

// browse walks the history. Going past the newest entry restores the draft.
func (q *queryLine) browse(delta int) {
	if len(q.history) == 0 {
		return
	}

	if q.browsing < 0 {
		q.draft = q.input.Value()
		q.browsing = len(q.history)
	}

	q.browsing += delta
	switch {
	case q.browsing < 0:
		q.browsing = 0
	case q.browsing >= len(q.history):
		q.browsing = -1
		q.SetText(q.draft)
		return
	}
	q.SetText(q.history[q.browsing])
}

// suggestion returns the most recent history entry starting with what has been
// typed, or "" when there is none.
func (q *queryLine) suggestion() string {
	typed := q.input.Value()
	if typed == "" {
		return ""
	}
	for i := len(q.history) - 1; i >= 0; i-- {
		if strings.HasPrefix(q.history[i], typed) && q.history[i] != typed {
			return q.history[i]
		}
	}
	return ""
}

// View renders the input with the unparsable tail underlined in place.
//
// Highlighting the error where it happened, rather than in a modal or a status
// line, is what makes the language learnable: the input itself says which word
// it did not understand.
func (q *queryLine) View(th *theme.Theme, width int) string {
	q.input.Width = max(width-4, 10)

	if !q.focused {
		return q.renderStatic(th, width)
	}

	line := q.input.View()

	// A suggestion is drawn after the cursor, dimmed.
	if suggestion := q.suggestion(); suggestion != "" {
		if rest := strings.TrimPrefix(suggestion, q.input.Value()); rest != "" {
			line += th.Dim.Render(rest)
		}
	}
	return " " + line
}

// renderStatic draws the query when the input does not have focus, marking the
// offending tail so an error stays visible after the user tabs away.
func (q *queryLine) renderStatic(th *theme.Theme, width int) string {
	text := q.input.Value()
	if text == "" {
		return " " + th.Dim.Render("press / to write a query")
	}

	if q.err == nil || q.err.Position >= len(text) {
		return " " + th.Value.Render(truncateTo(text, width-2))
	}

	head := text[:q.err.Position]
	tail := text[q.err.Position:]
	bad := lipgloss.NewStyle().Foreground(th.Warn).Underline(true)
	return " " + th.Value.Render(head) + bad.Render(tail)
}

// ErrorLine is the message shown under the input, or "" when the query is fine.
func (q *queryLine) ErrorLine(th *theme.Theme, width int) string {
	if q.err == nil {
		return ""
	}
	return " " + th.Warning.Render(truncateTo(q.err.Message, width-2))
}

func truncateTo(s string, width int) string {
	if width <= 1 || lipgloss.Width(s) <= width {
		return s
	}
	runes := []rune(s)
	for len(runes) > 0 && lipgloss.Width(string(runes))+1 > width {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "…"
}
