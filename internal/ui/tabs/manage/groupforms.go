package manage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/query"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/form"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
)

// buttonPreview computes the reset without applying it.
const buttonPreview = "Preview"

// resetModes are the offsets a reset can move to, in the order the radio shows
// them. The labels are the ones kafka-consumer-groups uses, so a person who
// knows that tool recognises them.
var resetModes = []string{"to-earliest", "to-latest", "to-offset", "to-datetime", "shift-by"}

// resetForm collects what to reset. It never touches the cluster: pressing
// Preview turns the form into a ResetSpec, and the cluster answers with the
// changes the user then approves.
type resetForm struct {
	form  *form.Model
	group string

	topic    *form.Text
	mode     *form.Radio
	offset   *form.Text
	datetime *form.Text
	shift    *form.Text

	err error
}

func newResetForm(group string, topics []string) *resetForm {
	f := &resetForm{group: group}

	hint := "blank for every topic the group reads"
	if len(topics) > 0 {
		hint = "blank for all: " + strings.Join(topics, ", ")
	}
	f.topic = form.NewText("Topic", "").
		WithPlaceholder("all topics").
		WithHint(hint)

	f.mode = form.NewRadio("Reset to", resetModes, 0)

	f.offset = form.NewText("Offset", "").
		WithPlaceholder("0").
		WithHint("used by to-offset; clamped into each partition").
		WithValidator(optionalNumber())

	f.datetime = form.NewText("Datetime", "").
		WithPlaceholder("2 hours ago").
		WithHint("used by to-datetime; also 2026-08-04T12:00 or 12:30").
		WithValidator(func(s string) error {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			_, err := query.ParseTime(s)
			return err
		})

	f.shift = form.NewText("Shift by", "").
		WithPlaceholder("-1000").
		WithHint("used by shift-by; negative moves back").
		WithValidator(optionalSignedNumber())

	f.form = form.New([]form.Field{
		f.topic, f.mode,
		form.NewSection("Position"),
		f.offset, f.datetime, f.shift,
	}, buttonPreview, buttonCancel)

	return f
}

func (f *resetForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

func (f *resetForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

func (f *resetForm) Submitted() bool {
	if f.form.Submitted() == buttonPreview {
		f.form.Reset()
		return true
	}
	return false
}

func (f *resetForm) setError(err error) { f.err = err }

// Spec assembles the reset, checking the one field the chosen mode needs.
func (f *resetForm) Spec() (kafka.ResetSpec, error) {
	if !f.form.Valid() {
		return kafka.ResetSpec{}, errors.New("fix the marked fields first")
	}

	spec := kafka.ResetSpec{Group: f.group}
	if topic := strings.TrimSpace(f.topic.Value()); topic != "" {
		spec.Topics = []string{topic}
	}

	switch f.mode.Selected() {
	case "to-earliest":
		spec.Kind = kafka.ResetToEarliest

	case "to-latest":
		spec.Kind = kafka.ResetToLatest

	case "to-offset":
		value, err := strconv.ParseInt(strings.TrimSpace(f.offset.Value()), 10, 64)
		if err != nil {
			return kafka.ResetSpec{}, errors.New("to-offset needs an offset")
		}
		if value < 0 {
			return kafka.ResetSpec{}, errors.New("an offset cannot be negative")
		}
		spec.Kind, spec.Offset = kafka.ResetToOffset, value

	case "to-datetime":
		at, err := query.ParseTime(strings.TrimSpace(f.datetime.Value()))
		if err != nil {
			return kafka.ResetSpec{}, fmt.Errorf("to-datetime needs a time: %w", err)
		}
		spec.Kind, spec.At = kafka.ResetToTime, at

	case "shift-by":
		value, err := strconv.ParseInt(strings.TrimSpace(f.shift.Value()), 10, 64)
		if err != nil {
			return kafka.ResetSpec{}, errors.New("shift-by needs a number of records")
		}
		if value == 0 {
			return kafka.ResetSpec{}, errors.New("shifting by 0 changes nothing")
		}
		spec.Kind, spec.Shift = kafka.ResetShiftBy, value
	}

	return spec, nil
}

func (f *resetForm) View(k *ktx.Context, width int) string {
	th := k.Theme

	lines := []string{
		th.Value.Render("Resetting the offsets of " + f.group + "."),
		// This is the constraint that turns into a protocol error later, so it is
		// said before the form is filled in rather than after.
		th.Warning.Render("Kafka refuses while the group has live members: stop them first."),
		"",
		f.form.View(th, width),
	}
	if f.err != nil {
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// ---------------------------------------------------------------- preview ---

// previewView shows where every partition would land, and is the only place a
// reset can be applied from. Nothing is committed until this screen is
// confirmed.
type previewView struct {
	group   string
	spec    kafka.ResetSpec
	changes []kafka.OffsetChange
	table   *table.Model
}

func newPreviewView(group string, spec kafka.ResetSpec, changes []kafka.OffsetChange) *previewView {
	v := &previewView{group: group, spec: spec, changes: changes}

	v.table = table.New(
		table.Column{Title: "TOPIC", Width: 0, TruncateFront: true},
		table.Column{Title: "P", Width: 4, Align: table.Right},
		table.Column{Title: "CURRENT", Width: 11, Align: table.Right},
		table.Column{Title: "NEW", Width: 11, Align: table.Right},
		table.Column{Title: "Δ", Width: 10, Align: table.Right},
		table.Column{Title: "LAG AFTER", Width: 10, Align: table.Right},
	)
	v.table.Focus()

	rows := make([]table.Row, 0, len(changes))
	for _, change := range changes {
		current, delta := "—", "—"
		if change.Current >= 0 {
			current = humanCount(change.Current)
			delta = signed(change.Delta())
		}

		row := table.Row{
			Cells: []string{
				change.Topic, itoa(int(change.Partition)), current,
				humanCount(change.New), delta, humanCount(change.LagAfter()),
			},
			Data: change,
		}
		if change.Clamped || change.Note != "" {
			style := warningStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	v.table.SetRows(rows)

	return v
}

func (v *previewView) Update(msg tea.Msg) tea.Cmd { return v.table.Update(msg) }

// movingChanges are the partitions that actually change position; committing
// the rest would only rewrite them with what they already hold.
func (v *previewView) movingChanges() []kafka.OffsetChange {
	moving := make([]kafka.OffsetChange, 0, len(v.changes))
	for _, change := range v.changes {
		if change.Moves() {
			moving = append(moving, change)
		}
	}
	return moving
}

func (v *previewView) summary() string {
	return fmt.Sprintf("reset %s %s: %d partitions moved",
		v.group, v.spec.Kind, len(v.movingChanges()))
}

// counts describes the preview in the terms that decide whether to apply it.
func (v *previewView) counts() (moving, clamped, unread int) {
	for _, change := range v.changes {
		if change.Moves() {
			moving++
		}
		if change.Clamped {
			clamped++
		}
		if change.Current < 0 {
			unread++
		}
	}
	return moving, clamped, unread
}

func (v *previewView) View(k *ktx.Context, width, height int) string {
	th := k.Theme

	moving, clamped, unread := v.counts()

	notes := []string{fmt.Sprintf("%d of %d partitions move", moving, len(v.changes))}
	if clamped > 0 {
		notes = append(notes, fmt.Sprintf("%d clamped to the partition bounds", clamped))
	}
	if unread > 0 {
		notes = append(notes, fmt.Sprintf("%d never committed", unread))
	}

	header := th.Value.Render(fmt.Sprintf("%s → %s", v.group, v.spec.Kind))
	footer := th.Dim.Render(strings.Join(notes, " · "))
	if moving == 0 {
		footer = th.Warning.Render("nothing to apply: every partition is already there")
	}

	// Two lines of context, one blank, and the table gets the rest.
	// The table is capped rather than stretched: on a wide terminal a flexible
	// topic column would push the numbers to the far right, away from the topic
	// they belong to.
	body := lipgloss.JoinVertical(lipgloss.Left,
		header,
		v.table.View(th, min(width-4, 88), height-5),
		footer,
	)

	return panel.Panel{
		Title:    "Preview · nothing has been changed yet",
		TopRight: "[Enter] apply  [Esc] back",
		Focused:  true,
		Width:    width,
		Height:   height,
	}.Render(th, body)
}

// ---------------------------------------------------------------- helpers ---

func signed(n int64) string {
	if n > 0 {
		return "+" + humanCount(n)
	}
	return humanCount(n)
}

func optionalSignedNumber() form.Validator {
	return func(s string) error {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			return errors.New("must be a number, negative to move back")
		}
		return nil
	}
}
