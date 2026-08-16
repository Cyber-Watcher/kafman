package manage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/form"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// Button labels.
const (
	buttonCreate = "Create"
	buttonApply  = "Apply"
	buttonCancel = "Cancel"
)

// topicNamePattern is what Kafka accepts: letters, digits, dot, underscore and
// hyphen.
const topicNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

// ------------------------------------------------------------------ create ---

// createForm creates a topic.
type createForm struct {
	form     *form.Model
	existing map[string]bool

	name       *form.Text
	partitions *form.Text
	replicas   *form.Text
	cleanup    *form.Radio
	retention  *form.Text
	minISR     *form.Text

	err error
}

func newCreateForm(existing []string) *createForm {
	f := &createForm{existing: make(map[string]bool, len(existing))}
	for _, name := range existing {
		f.existing[name] = true
	}

	f.name = form.NewText("Name", "").
		WithPlaceholder("orders.events").
		WithValidator(func(s string) error {
			s = strings.TrimSpace(s)
			switch {
			case s == "":
				return errors.New("required")
			case f.existing[s]:
				return errors.New("already exists")
			case s == "." || s == "..":
				return errors.New("this name is reserved by Kafka")
			case len(s) > 249:
				return errors.New("too long, 249 characters maximum")
			}
			for _, r := range s {
				if !strings.ContainsRune(topicNameChars, r) {
					return fmt.Errorf("%q is not allowed; use letters, digits, . _ -", r)
				}
			}
			return nil
		})

	f.partitions = form.NewText("Partitions", "3").
		WithHint("cannot be reduced later").
		WithValidator(positiveNumber(1, 200_000))

	f.replicas = form.NewText("Replication factor", "3").
		WithHint("must not exceed the broker count").
		WithValidator(positiveNumber(1, 32))

	f.cleanup = form.NewRadio("Cleanup", []string{"delete", "compact", "compact,delete"}, 0)

	f.retention = form.NewText("Retention", "").
		WithPlaceholder("604800000").
		WithHint("milliseconds, blank for the broker default").
		WithValidator(optionalNumber())

	f.minISR = form.NewText("Min in-sync replicas", "").
		WithPlaceholder("2").
		WithHint("blank for the broker default").
		WithValidator(optionalNumber())

	f.form = form.New([]form.Field{
		f.name, f.partitions, f.replicas,
		form.NewSection("Configuration"),
		f.cleanup, f.retention, f.minISR,
	}, buttonCreate, buttonCancel)

	return f
}

func (f *createForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

func (f *createForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

func (f *createForm) Submitted() bool {
	if f.form.Submitted() == buttonCreate {
		f.form.Reset()
		return true
	}
	return false
}

func (f *createForm) setError(err error) { f.err = err }

// Spec assembles the topic to create.
func (f *createForm) Spec() (kafka.TopicSpec, error) {
	if !f.form.Valid() {
		return kafka.TopicSpec{}, errors.New("fix the marked fields first")
	}

	partitions, err := strconv.Atoi(f.partitions.Value())
	if err != nil {
		return kafka.TopicSpec{}, errors.New("partitions must be a number")
	}
	replicas, err := strconv.Atoi(f.replicas.Value())
	if err != nil {
		return kafka.TopicSpec{}, errors.New("replication factor must be a number")
	}

	spec := kafka.TopicSpec{
		Name:              strings.TrimSpace(f.name.Value()),
		Partitions:        int32(partitions),
		ReplicationFactor: int16(replicas),
		Configs:           map[string]string{"cleanup.policy": f.cleanup.Selected()},
	}
	if value := strings.TrimSpace(f.retention.Value()); value != "" {
		spec.Configs["retention.ms"] = value
	}
	if value := strings.TrimSpace(f.minISR.Value()); value != "" {
		spec.Configs["min.insync.replicas"] = value
	}
	return spec, nil
}

func (f *createForm) View(k *ktx.Context, width int) string {
	th := k.Theme

	lines := []string{f.form.View(th, width)}
	if f.err != nil {
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// -------------------------------------------------------------- partitions ---

// partitionForm raises a topic's partition count.
type partitionForm struct {
	form    *form.Model
	topic   string
	current int

	total *form.Text
	err   error
}

func newPartitionForm(topic string, current int) *partitionForm {
	f := &partitionForm{topic: topic, current: current}

	f.total = form.NewText("New total", itoa(current+1)).
		WithValidator(func(s string) error {
			value, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil {
				return errors.New("must be a number")
			}
			if value <= current {
				return fmt.Errorf("must be more than the current %d", current)
			}
			return nil
		})

	f.form = form.New([]form.Field{f.total}, buttonApply, buttonCancel)
	return f
}

func (f *partitionForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

func (f *partitionForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

func (f *partitionForm) Submitted() bool {
	if f.form.Submitted() == buttonApply {
		f.form.Reset()
		return true
	}
	return false
}

func (f *partitionForm) setError(err error) { f.err = err }

func (f *partitionForm) Total() (int32, error) {
	if !f.form.Valid() {
		return 0, errors.New("fix the marked field first")
	}
	value, err := strconv.Atoi(strings.TrimSpace(f.total.Value()))
	if err != nil {
		return 0, errors.New("must be a number")
	}
	return int32(value), nil
}

func (f *partitionForm) View(k *ktx.Context, width int) string {
	th := k.Theme

	lines := []string{
		th.Value.Render(fmt.Sprintf("%s currently has %d partitions.", f.topic, f.current)),
		"",
		// This is the warning that matters: adding partitions silently changes
		// where existing keys route, and nothing later will remind them.
		th.Warning.Render("Adding partitions changes which partition a key routes to."),
		th.Warning.Render("Records already written stay where they are, so per-key"),
		th.Warning.Render("ordering is broken from this point on. Kafka cannot undo it."),
		"",
		f.form.View(th, width),
	}
	if f.err != nil {
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// ----------------------------------------------------------------- configs ---

// configsLoadedMsg carries a topic's settings.
type configsLoadedMsg struct {
	Topic   string
	Entries []kafka.ConfigEntry
	Err     error
}

// configView lists and edits a topic's configuration.
type configView struct {
	topic   string
	entries []kafka.ConfigEntry
	table   *table.Model

	showDefaults bool
	loading      bool
	err          error

	// edit is the value field open over a setting, or nil.
	edit    *form.Text
	editKey string
}

func newConfigView(topic string) *configView {
	v := &configView{topic: topic}
	v.table = table.New(
		table.Column{Title: "SETTING", Width: 38},
		table.Column{Title: "VALUE", Width: 0},
		table.Column{Title: "SOURCE", Width: 16},
	)
	v.table.Focus()
	return v
}

func (v *configView) editing() bool { return v.edit != nil }

// Load fetches the configuration.
func (v *configView) Load(client kafka.Facade) tea.Cmd {
	if client == nil {
		return nil
	}
	v.loading, v.err = true, nil

	topic, defaults := v.topic, v.showDefaults
	return func() tea.Msg {
		entries, err := client.TopicConfig(context.Background(), topic, defaults)
		return configsLoadedMsg{Topic: topic, Entries: entries, Err: err}
	}
}

// Loaded applies a fetched configuration.
func (v *configView) Loaded(message configsLoadedMsg) {
	if message.Topic != v.topic {
		return
	}
	v.loading = false
	if message.Err != nil {
		v.err = message.Err
		return
	}
	v.entries = message.Entries
	v.rebuild()
}

func (v *configView) rebuild() {
	rows := make([]table.Row, 0, len(v.entries))
	for _, entry := range v.entries {
		source := entry.Source
		if entry.ReadOnly {
			source += " · read-only"
		}
		rows = append(rows, table.Row{
			Cells: []string{entry.Name, entry.Value, source},
			Data:  entry,
		})
	}
	v.table.SetRows(rows)
}

// Shortcuts implements ui.ShortcutProvider.
func (v *configView) Shortcuts() []chrome.Shortcut {
	if v.editing() {
		return []chrome.Shortcut{{Key: "Enter", Label: "Apply"}, {Key: "Esc", Label: "Cancel"}}
	}
	return []chrome.Shortcut{
		{Key: "Enter", Label: "Edit"},
		{Key: "F2", Label: "Reset"},
		{Key: "F4", Label: defaultsLabel(v.showDefaults)},
		{Key: "Esc", Label: "Back"},
	}
}

func defaultsLabel(showing bool) string {
	if showing {
		return "Hide defaults"
	}
	return "Show defaults"
}

// Update handles keys.
func (v *configView) Update(key tea.KeyMsg, client kafka.Facade) tea.Cmd {
	if v.editing() {
		return v.updateEdit(key, client)
	}

	switch key.String() {
	case "f4":
		v.showDefaults = !v.showDefaults
		return v.Load(client)

	case "enter":
		entry := v.selected()
		if entry == nil {
			return nil
		}
		if entry.ReadOnly {
			return notify(entry.Name+" cannot be changed at runtime", msg.NotifyWarning)
		}
		v.editKey = entry.Name
		v.edit = form.NewText(entry.Name, entry.Value)
		return nil

	case "f2":
		entry := v.selected()
		if entry == nil || entry.Default {
			return nil
		}
		topic, key := v.topic, entry.Name
		return func() tea.Msg {
			err := client.ResetTopicConfig(context.Background(), topic, key)
			return topicChangedMsg{Summary: fmt.Sprintf("%s reset to its default on %s", key, topic), Err: err}
		}
	}
	return v.table.Update(key)
}

func (v *configView) updateEdit(key tea.KeyMsg, client kafka.Facade) tea.Cmd {
	switch key.String() {
	case "esc":
		v.edit, v.editKey = nil, ""
		return nil

	case "enter":
		topic, name, value := v.topic, v.editKey, v.edit.Value()
		v.edit, v.editKey = nil, ""
		return func() tea.Msg {
			err := client.SetTopicConfig(context.Background(), topic, name, value)
			return topicChangedMsg{
				Summary: fmt.Sprintf("%s set to %s on %s", name, value, topic),
				Err:     err,
			}
		}
	}
	return v.edit.Update(key)
}

func (v *configView) selected() *kafka.ConfigEntry {
	row := v.table.SelectedRow()
	if row == nil {
		return nil
	}
	entry, ok := row.Data.(kafka.ConfigEntry)
	if !ok {
		return nil
	}
	return &entry
}

// View renders the configuration list.
func (v *configView) View(k *ktx.Context, width, height int) string {
	th := k.Theme

	content := v.table.View(th, width-2, height-2)
	switch {
	case v.loading:
		content = th.Dim.Render("  loading…")
	case v.err != nil:
		content = th.Critical.Render("  " + kafka.Explain(v.err))
	case len(v.entries) == 0:
		content = th.Dim.Render("  no explicit settings; press F4 to include the defaults")
	}

	title := v.topic + " · configuration"
	counter := itoa(len(v.entries))
	if v.showDefaults {
		counter += " with defaults"
	}

	body := panel.Panel{
		Title: title, TopRight: counter, Focused: true,
		Width: width, Height: height,
	}.Render(th, content)

	if v.editing() {
		box := th.ModalBorder.Width(min(width-8, 70)).Render(
			lipgloss.JoinVertical(lipgloss.Left,
				th.Title.Render("Set "+v.editKey),
				"",
				v.edit.View(th, 22, min(width-16, 60), true),
				"",
				th.Dim.Render("[Enter] apply   [Esc] cancel"),
			))
		return modal.Overlay(body, box)
	}
	return body
}

// ---------------------------------------------------------------- helpers ---

func positiveNumber(low, high int) form.Validator {
	return func(s string) error {
		value, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return errors.New("must be a number")
		}
		if value < low || value > high {
			return fmt.Errorf("must be between %d and %d", low, high)
		}
		return nil
	}
}

func optionalNumber() form.Validator {
	return func(s string) error {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			return errors.New("must be a number")
		}
		return nil
	}
}
