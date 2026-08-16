package manage

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

func newTab(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	mock := kafka.NewMock(nil)
	m := New()
	m.SetClient(mock)

	topics, _ := mock.ListTopics(context.Background(), true)
	sizes, _ := mock.TopicSizes(context.Background())
	m.Update(msg.TopicsLoaded{Topics: topics})
	m.Update(msg.SizesLoaded{Topics: sizes})
	return m, mock
}

func render(t *testing.T, m *Model) string {
	t.Helper()
	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k := ktx.New(cfg, theme.New(theme.Dark, false), 130, 30)
	return ansi.Strip(m.View(k))
}

func key(s string) tea.KeyMsg {
	switch s {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "ctrl+n":
		return tea.KeyMsg{Type: tea.KeyCtrlN}
	case "ctrl+o":
		return tea.KeyMsg{Type: tea.KeyCtrlO}
	case "ctrl+p":
		return tea.KeyMsg{Type: tea.KeyCtrlP}
	case "ctrl+t":
		return tea.KeyMsg{Type: tea.KeyCtrlT}
	case "f2":
		return tea.KeyMsg{Type: tea.KeyF2}
	case "f4":
		return tea.KeyMsg{Type: tea.KeyF4}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

func typeText(m *Model, s string) {
	for _, r := range s {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
}

// run executes a returned command and feeds the result back, the way the event
// loop would.
func run(m *Model, cmd tea.Cmd) {
	for range 4 {
		if cmd == nil {
			return
		}
		message := cmd()
		if message == nil {
			return
		}
		if batch, ok := message.(tea.BatchMsg); ok {
			for _, c := range batch {
				run(m, c)
			}
			return
		}
		cmd = m.Update(message)
	}
}

// ---------------------------------------------------------------- sections ---

func TestSectionsAreListed(t *testing.T) {
	m, _ := newTab(t)
	view := render(t, m)

	// Sections that are not built yet are still shown, so the shape of the tab
	// is visible.
	for _, want := range []string{"Topics", "Consumer groups", "Users (SCRAM)", "ACLs"} {
		if !strings.Contains(view, want) {
			t.Errorf("the section list should offer %q:\n%s", want, view)
		}
	}
}

// Every section is built now, and walking the list has to land on each of them
// without the tab losing the keyboard on the way.
func TestEverySectionOpens(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("tab")) // focus the section list

	titles := []string{"Consumer groups", "SCRAM users", "ACLs", "Topics"}
	for _, want := range titles {
		run(m, m.Update(key("down")))
		view := render(t, m)
		if !strings.Contains(view, want) {
			t.Errorf("moving down should open a section showing %q:\n%s", want, view)
		}
	}
}

func TestTabMovesBetweenSectionsAndContent(t *testing.T) {
	m, _ := newTab(t)

	if m.sectionFocused {
		t.Error("the content should start focused")
	}
	m.Update(key("tab"))
	if !m.sectionFocused {
		t.Error("Tab should move focus to the section list")
	}
	m.Update(key("tab"))
	if m.sectionFocused {
		t.Error("Tab should move focus back to the content")
	}
}

// ------------------------------------------------------------------ topics ---

func TestTopicListShowsReplicationState(t *testing.T) {
	m, _ := newTab(t)
	view := render(t, m)

	for _, want := range []string{"orders.events", "36/36", "audit.log", "71/72"} {
		if !strings.Contains(view, want) {
			t.Errorf("the list should show %q:\n%s", want, view)
		}
	}
}

func TestInternalTopicsAreMarked(t *testing.T) {
	m, _ := newTab(t)
	view := render(t, m)

	if !strings.Contains(view, "__consumer_offsets *") {
		t.Errorf("internal topics should be marked:\n%s", view)
	}
}

func TestInternalTopicsCannotBeDeleted(t *testing.T) {
	m, mock := newTab(t)

	// Walk to the internal topic.
	for range 10 {
		if topic := m.topics.selected(); topic != nil && topic.Internal {
			break
		}
		m.Update(key("down"))
	}

	cmd := m.Update(key("f2"))
	if m.topics.mode == modeConfirmDelete {
		t.Error("deleting an internal topic should not even be offered")
	}
	if cmd == nil {
		t.Fatal("the refusal should be explained")
	}
	if len(mock.Deleted) != 0 {
		t.Error("nothing should have been deleted")
	}
}

func TestDeleteRequiresTypingTheName(t *testing.T) {
	m, mock := newTab(t)

	// Move off the internal topic.
	for range 10 {
		if topic := m.topics.selected(); topic != nil && !topic.Internal {
			break
		}
		m.Update(key("down"))
	}
	name := m.topics.selected().Name

	m.Update(key("f2"))
	if m.topics.mode != modeConfirmDelete {
		t.Fatal("F2 should open a confirmation")
	}

	// Enter alone does nothing: the name has to be typed first.
	m.Update(key("enter"))
	if len(mock.Deleted) != 0 {
		t.Fatal("confirming without typing the name must not delete anything")
	}

	view := render(t, m)
	if !strings.Contains(view, "cannot be undone") {
		t.Errorf("the dialog should say the deletion is permanent:\n%s", view)
	}

	typeText(m, name)
	run(m, m.Update(key("enter")))

	if len(mock.Deleted) != 1 || mock.Deleted[0] != name {
		t.Errorf("deleted %v, want [%s]", mock.Deleted, name)
	}
}

func TestDeleteCanBeCancelled(t *testing.T) {
	m, mock := newTab(t)
	m.Update(key("down"))
	m.Update(key("f2"))
	m.Update(key("esc"))

	if m.topics.mode != modeList {
		t.Error("Esc should dismiss the confirmation")
	}
	if len(mock.Deleted) != 0 {
		t.Error("cancelling must not delete anything")
	}
}

func TestCreateTopicThroughTheForm(t *testing.T) {
	m, mock := newTab(t)

	m.Update(key("ctrl+n"))
	if m.topics.mode != modeCreate {
		t.Fatal("Ctrl+N should open the create form")
	}
	if !m.AcceptsText() {
		t.Error("with the form open, keys belong to it")
	}

	typeText(m, "new.topic")
	m.topics.create.partitions.SetValue("6")
	m.topics.create.replicas.SetValue("2")
	m.topics.create.retention.SetValue("3600000")

	spec, err := m.topics.create.Spec()
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}
	if spec.Name != "new.topic" || spec.Partitions != 6 || spec.ReplicationFactor != 2 {
		t.Errorf("spec = %+v, want the typed values", spec)
	}
	if spec.Configs["retention.ms"] != "3600000" {
		t.Errorf("configs = %v, want the retention carried through", spec.Configs)
	}
	if spec.Configs["cleanup.policy"] != "delete" {
		t.Errorf("cleanup.policy = %q, want the default", spec.Configs["cleanup.policy"])
	}

	run(m, m.CreateTopic(spec))
	if len(mock.Created) != 1 {
		t.Fatalf("created %d topics, want 1", len(mock.Created))
	}
}

func TestCreateFormRejectsBadNames(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("ctrl+n"))

	cases := map[string]string{
		"orders.events": "already exists",
		"":              "required",
		"has spaces":    "not allowed",
		"has/slash":     "not allowed",
		".":             "reserved",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			m.topics.create.name.SetValue(name)
			err := m.topics.create.name.Err()
			if err == nil {
				t.Fatalf("name %q should be rejected", name)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error = %q, want it to mention %q", err, want)
			}
		})
	}

	m.topics.create.name.SetValue("perfectly.fine_name-1")
	if err := m.topics.create.name.Err(); err != nil {
		t.Errorf("a valid name was rejected: %v", err)
	}
}

func TestPartitionFormWarnsAboutKeyRouting(t *testing.T) {
	m, _ := newTab(t)
	m.Update(key("ctrl+p"))

	if m.topics.mode != modeAddPartitions {
		t.Fatal("Ctrl+P should open the partition form")
	}

	view := render(t, m)
	// This warning is the whole point: nothing later will remind the user that
	// adding partitions rehomes their keys.
	for _, want := range []string{"changes which partition a key routes to", "cannot undo"} {
		if !strings.Contains(view, want) {
			t.Errorf("the form should warn that %q:\n%s", want, view)
		}
	}
}

func TestPartitionFormRefusesToShrink(t *testing.T) {
	m, _ := newTab(t)
	current := m.topics.selected().Partitions
	m.Update(key("ctrl+p"))

	m.topics.partition.total.SetValue(itoa(current - 1))
	if m.topics.partition.total.Err() == nil {
		t.Error("reducing the partition count should be refused in the form")
	}

	m.topics.partition.total.SetValue(itoa(current + 3))
	if err := m.topics.partition.total.Err(); err != nil {
		t.Errorf("growing should be allowed: %v", err)
	}
}

// ----------------------------------------------------------------- configs ---

func TestConfigViewListsOverrides(t *testing.T) {
	m, _ := newTab(t)
	run(m, m.Update(key("ctrl+o")))

	if m.topics.mode != modeConfig {
		t.Fatal("Ctrl+O should open the configuration")
	}

	view := render(t, m)
	for _, want := range []string{"cleanup.policy", "retention.ms", "min.insync.replicas"} {
		if !strings.Contains(view, want) {
			t.Errorf("the configuration should list %q:\n%s", want, view)
		}
	}
	// Defaults are hidden until asked for: an audit wants the overrides.
	if strings.Contains(view, "compression.type") {
		t.Errorf("defaults should be hidden by default:\n%s", view)
	}
}

func TestConfigViewCanShowDefaults(t *testing.T) {
	m, _ := newTab(t)
	run(m, m.Update(key("ctrl+o")))
	run(m, m.Update(key("f4")))

	view := render(t, m)
	if !strings.Contains(view, "compression.type") {
		t.Errorf("F4 should reveal the defaults:\n%s", view)
	}
}

func TestReadOnlyConfigCannotBeEdited(t *testing.T) {
	m, _ := newTab(t)
	run(m, m.Update(key("ctrl+o")))
	run(m, m.Update(key("f4"))) // defaults include a read-only entry

	// Walk to the read-only setting.
	var found bool
	for range 20 {
		if entry := m.topics.configs.selected(); entry != nil && entry.ReadOnly {
			found = true
			break
		}
		m.Update(key("down"))
	}
	if !found {
		t.Skip("no read-only entry in the mock")
	}

	cmd := m.Update(key("enter"))
	if m.topics.configs.editing() {
		t.Error("a read-only setting should not open an editor")
	}
	if cmd == nil {
		t.Error("the refusal should be explained")
	}
}

func TestEditingAConfigSendsIt(t *testing.T) {
	m, mock := newTab(t)
	run(m, m.Update(key("ctrl+o")))

	var applied [3]string
	mock.SetTopicConfigFunc = func(_ context.Context, topic, key, value string) error {
		applied = [3]string{topic, key, value}
		return nil
	}

	m.Update(key("enter")) // open the editor on the first setting
	if !m.topics.configs.editing() {
		t.Fatal("Enter should open an editor")
	}
	name := m.topics.configs.editKey

	m.topics.configs.edit.SetValue("compact")
	run(m, m.Update(key("enter")))

	if applied[1] != name || applied[2] != "compact" {
		t.Errorf("applied %v, want the edited setting", applied)
	}
}

func TestConfigEditCanBeCancelled(t *testing.T) {
	m, mock := newTab(t)
	run(m, m.Update(key("ctrl+o")))

	m.Update(key("enter"))
	m.Update(key("esc"))

	if m.topics.configs.editing() {
		t.Error("Esc should close the editor")
	}
	if mock.Called("SetTopicConfig") {
		t.Error("cancelling must not change anything")
	}
}

func TestFailureIsReported(t *testing.T) {
	m, mock := newTab(t)
	mock.CreateTopicFunc = func(context.Context, kafka.TopicSpec) error {
		return errors.New("cluster authorization failed")
	}

	cmd := m.CreateTopic(kafka.TopicSpec{Name: "x", Partitions: 1, ReplicationFactor: 1})
	message := cmd()

	changed, ok := message.(topicChangedMsg)
	if !ok {
		t.Fatalf("got %T, want topicChangedMsg", message)
	}
	if changed.Err == nil {
		t.Fatal("the failure should be carried")
	}

	notification := m.Update(changed)
	if notification == nil {
		t.Fatal("a failure should be shown to the user")
	}
	if typed, ok := notification().(msg.Notify); !ok || typed.Level != msg.NotifyError {
		t.Errorf("got %#v, want an error notification", notification())
	}
}

func TestDisconnectClearsTheList(t *testing.T) {
	m, _ := newTab(t)
	if !strings.Contains(render(t, m), "orders.events") {
		t.Fatal("setup: topics should be listed")
	}

	m.SetClient(nil)

	view := render(t, m)
	if strings.Contains(view, "orders.events") {
		t.Errorf("data from a closed connection must not stay on screen:\n%s", view)
	}
	if !strings.Contains(view, "not connected") {
		t.Errorf("the tab should say it is not connected:\n%s", view)
	}
}
