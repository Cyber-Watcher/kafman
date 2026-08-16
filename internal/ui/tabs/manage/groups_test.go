package manage

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// onGroups opens the Consumer groups section with its list and the detail of
// the first group loaded, which is where every test below starts.
func onGroups(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	m, mock := newTab(t)
	m.section = sectionGroups
	run(m, m.groups.Init())
	return m, mock
}

func TestGroupsAreListedWithLag(t *testing.T) {
	m, _ := onGroups(t)
	view := render(t, m)

	for _, want := range []string{"billing-worker", "Stable", "38_104", "search-indexer", "Rebalancing"} {
		if !strings.Contains(view, want) {
			t.Errorf("the group list should show %q:\n%s", want, view)
		}
	}
}

func TestSelectedGroupOffsetsAreShown(t *testing.T) {
	m, mock := onGroups(t)
	view := render(t, m)

	if !mock.Called("DescribeConsumerGroup") {
		t.Error("the detail of the selected group should be loaded")
	}
	for _, want := range []string{"orders.events", "391_755", "399_012", "10.0.0.2", "coordinator 2"} {
		if !strings.Contains(view, want) {
			t.Errorf("the offsets panel should show %q:\n%s", want, view)
		}
	}
}

// The offsets panel names every host the group's consumers run on: "who is
// consuming this right now" is what the panel is opened for.
func TestOffsetsPanelShowsConsumerHosts(t *testing.T) {
	m, _ := onGroups(t)
	view := render(t, m)

	if !strings.Contains(view, "hosts (1): 10.0.0.2") {
		t.Errorf("the offsets panel should list the members' hosts:\n%s", view)
	}
}

// A partition the group has never committed to must not be rendered as offset
// zero: never-committed and committed-at-zero mean very different things.
func TestNeverCommittedPartitionIsNotZero(t *testing.T) {
	m, _ := onGroups(t)
	view := render(t, m)

	line := lineContaining(t, view, "395_881")
	if !strings.Contains(line, "—") {
		t.Errorf("a never-committed partition should show a dash, not a number:\n%s", line)
	}
}

func TestMovingTheCursorLoadsTheOtherGroup(t *testing.T) {
	m, _ := onGroups(t)

	run(m, m.Update(key("down")))
	if m.groups.detailName != "search-indexer" {
		t.Fatalf("the detail should follow the cursor, got %q", m.groups.detailName)
	}
}

// A detail that arrives after the cursor has moved on belongs to a group that
// is no longer on screen, and must not overwrite the current one.
func TestStaleDetailIsDropped(t *testing.T) {
	m, _ := onGroups(t)

	stale := groupDetailMsg{
		Name:    "old-consumer",
		Group:   kafka.ConsumerGroup{Name: "old-consumer", TotalLag: 999},
		Offsets: []kafka.GroupOffset{{Topic: "elsewhere", Partition: 7}},
	}
	m.Update(stale)

	if strings.Contains(render(t, m), "elsewhere") {
		t.Error("a detail for another group should be dropped")
	}
}

func TestEnterOpensMembers(t *testing.T) {
	m, _ := onGroups(t)
	run(m, m.Update(key("enter")))

	view := render(t, m)
	for _, want := range []string{"members", "worker-1", "assignment", "orders.events p0"} {
		if !strings.Contains(view, want) {
			t.Errorf("the members screen should show %q:\n%s", want, view)
		}
	}

	m.Update(key("esc"))
	if m.groups.mode != groupModeList {
		t.Error("Esc should close the members screen")
	}
}

// ------------------------------------------------------------------ delete ---

func TestDeleteGroupAsksFirst(t *testing.T) {
	m, mock := onGroups(t)

	run(m, m.Update(key("f2")))
	if len(mock.DeletedGroups) != 0 {
		t.Fatal("nothing should be deleted before the confirmation")
	}
	if !strings.Contains(render(t, m), "Delete consumer group") {
		t.Fatal("F2 should open a confirmation")
	}

	run(m, m.Update(key("enter")))
	if len(mock.DeletedGroups) != 1 || mock.DeletedGroups[0] != "billing-worker" {
		t.Errorf("the confirmed group should be deleted, got %v", mock.DeletedGroups)
	}
}

func TestDeleteGroupCanBeCancelled(t *testing.T) {
	m, mock := onGroups(t)

	run(m, m.Update(key("f2")))
	run(m, m.Update(key("esc")))

	if len(mock.DeletedGroups) != 0 {
		t.Errorf("a cancelled delete should not happen, got %v", mock.DeletedGroups)
	}
	if m.groups.mode != groupModeList {
		t.Error("Esc should close the confirmation")
	}
}

func TestClearOffsetsNeedsATopic(t *testing.T) {
	m, mock := onGroups(t)

	// The mock group reads one topic, so Ctrl+D can name it without help.
	run(m, m.Update(key("ctrl+d")))
	if !strings.Contains(render(t, m), "orders.events") {
		t.Fatal("the confirmation should name the topic")
	}

	run(m, m.Update(key("enter")))
	if len(mock.DeletedOffsets) != 1 || mock.DeletedOffsets[0] != "billing-worker/orders.events" {
		t.Errorf("the offsets of the selected group and topic should be cleared, got %v",
			mock.DeletedOffsets)
	}
}

// ------------------------------------------------------------------- reset ---

func TestResetPreviewsBeforeApplying(t *testing.T) {
	m, mock := onGroups(t)

	run(m, m.Update(key("ctrl+r")))
	if m.groups.mode != groupModeReset {
		t.Fatal("Ctrl+R should open the reset form")
	}

	// Default mode is to-earliest, so the form is complete as it stands.
	run(m, m.Update(key("enter"))) // Topic → Reset to
	run(m, m.Update(key("enter"))) // → Position section
	submitForm(m)

	if !mock.Called("PreviewOffsetReset") {
		t.Fatal("the reset should be previewed")
	}
	if len(mock.Applied) != 0 {
		t.Fatal("a preview must not commit anything")
	}
	if m.groups.mode != groupModePreview {
		t.Fatalf("the preview should be on screen, mode is %v", m.groups.mode)
	}

	view := render(t, m)
	for _, want := range []string{"nothing has been changed yet", "391_755", "-391_655", "clamped"} {
		if !strings.Contains(view, want) {
			t.Errorf("the preview should show %q:\n%s", want, view)
		}
	}

	// The preview shows what would happen; applying it is a second, explicit act.
	run(m, m.Update(key("enter")))
	if len(mock.Applied) != 0 {
		t.Fatal("the preview should ask before committing")
	}
	if !strings.Contains(render(t, m), "Apply the reset") {
		t.Fatalf("a confirmation should be on screen:\n%s", render(t, m))
	}

	run(m, m.Update(key("enter")))
	if len(mock.Applied) != 1 {
		t.Fatalf("the confirmed reset should be applied, got %v", mock.Applied)
	}
	if got := mock.Applied[0].Group; got != "billing-worker" {
		t.Errorf("the reset should apply to the selected group, got %q", got)
	}
	if got := len(mock.Applied[0].Changes); got != 2 {
		t.Errorf("both moving partitions should be committed, got %d", got)
	}
}

func TestDecliningTheResetKeepsThePreview(t *testing.T) {
	m, mock := onGroups(t)

	run(m, m.Update(key("ctrl+r")))
	submitForm(m)
	run(m, m.Update(key("enter"))) // ask
	run(m, m.Update(key("esc")))   // decline

	if m.groups.mode != groupModePreview {
		t.Errorf("declining should go back to the preview, mode is %v", m.groups.mode)
	}
	if len(mock.Applied) != 0 {
		t.Errorf("a declined reset must not be committed, got %v", mock.Applied)
	}
}

func TestPreviewEscGoesBackToTheForm(t *testing.T) {
	m, mock := onGroups(t)

	run(m, m.Update(key("ctrl+r")))
	submitForm(m)
	if m.groups.mode != groupModePreview {
		t.Fatal("the preview should be open")
	}

	run(m, m.Update(key("esc")))
	if m.groups.mode != groupModeReset {
		t.Error("Esc on the preview should go back to the form, not out of the reset")
	}
	if len(mock.Applied) != 0 {
		t.Error("leaving the preview must not commit anything")
	}
}

func TestPreviewFailureKeepsTheForm(t *testing.T) {
	m, mock := onGroups(t)
	mock.PreviewOffsetResetFunc = func(context.Context, kafka.ResetSpec) ([]kafka.OffsetChange, error) {
		return nil, errors.New("group billing-worker has active members; stop its consumers first")
	}

	run(m, m.Update(key("ctrl+r")))
	submitForm(m)

	if m.groups.mode != groupModeReset {
		t.Fatal("a failed preview should leave the form open")
	}
	if !strings.Contains(render(t, m), "active members") {
		t.Error("the reason should be on screen")
	}
}

func TestResetFormBuildsTheSpec(t *testing.T) {
	tests := []struct {
		name   string
		mode   string
		field  *string
		value  string
		verify func(t *testing.T, spec kafka.ResetSpec)
	}{
		{
			name: "to-latest",
			mode: "to-latest",
			verify: func(t *testing.T, spec kafka.ResetSpec) {
				if spec.Kind != kafka.ResetToLatest {
					t.Errorf("kind is %v", spec.Kind)
				}
			},
		},
		{
			name:  "to-offset",
			mode:  "to-offset",
			value: "1200",
			verify: func(t *testing.T, spec kafka.ResetSpec) {
				if spec.Kind != kafka.ResetToOffset || spec.Offset != 1200 {
					t.Errorf("kind %v offset %d", spec.Kind, spec.Offset)
				}
			},
		},
		{
			name:  "shift-by accepts a negative number",
			mode:  "shift-by",
			value: "-500",
			verify: func(t *testing.T, spec kafka.ResetSpec) {
				if spec.Kind != kafka.ResetShiftBy || spec.Shift != -500 {
					t.Errorf("kind %v shift %d", spec.Kind, spec.Shift)
				}
			},
		},
		{
			name:  "to-datetime speaks the query language",
			mode:  "to-datetime",
			value: "2 hours ago",
			verify: func(t *testing.T, spec kafka.ResetSpec) {
				if spec.Kind != kafka.ResetToTime || spec.At.IsZero() {
					t.Errorf("kind %v at %v", spec.Kind, spec.At)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newResetForm("billing-worker", []string{"orders.events"})
			f.mode.SelectValue(test.mode)
			switch test.mode {
			case "to-offset":
				f.offset.SetValue(test.value)
			case "shift-by":
				f.shift.SetValue(test.value)
			case "to-datetime":
				f.datetime.SetValue(test.value)
			}

			spec, err := f.Spec()
			if err != nil {
				t.Fatalf("Spec: %v", err)
			}
			if spec.Group != "billing-worker" {
				t.Errorf("group is %q", spec.Group)
			}
			test.verify(t, spec)
		})
	}
}

func TestResetFormRejectsIncompleteInput(t *testing.T) {
	tests := []struct {
		mode string
		want string
	}{
		{mode: "to-offset", want: "needs an offset"},
		{mode: "to-datetime", want: "needs a time"},
		{mode: "shift-by", want: "needs a number"},
	}

	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			f := newResetForm("g", nil)
			f.mode.SelectValue(test.mode)

			_, err := f.Spec()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Errorf("%s without its field should say %q, got %v", test.mode, test.want, err)
			}
		})
	}
}

func TestResetFormRefusesAPointlessShift(t *testing.T) {
	f := newResetForm("g", nil)
	f.mode.SelectValue("shift-by")
	f.shift.SetValue("0")

	if _, err := f.Spec(); err == nil {
		t.Error("shifting by zero should be refused rather than sent to the cluster")
	}
}

func TestResetTargetsOneTopic(t *testing.T) {
	f := newResetForm("g", []string{"orders.events", "payments.tx"})
	f.topic.SetValue("payments.tx")

	spec, err := f.Spec()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Topics) != 1 || spec.Topics[0] != "payments.tx" {
		t.Errorf("the named topic should limit the reset, got %v", spec.Topics)
	}
}

// ------------------------------------------------------------------- jumps ---

func TestFocusGroupSelectsTheGroup(t *testing.T) {
	m, _ := newTab(t)

	run(m, m.FocusGroup(msg.FocusGroup{Group: "audit-archiver"}))

	if m.section != sectionGroups {
		t.Fatal("a jump should open the Consumer groups section")
	}
	if got := m.groups.selectedName(); got != "audit-archiver" {
		t.Errorf("the named group should be selected, got %q", got)
	}
}

// ------------------------------------------------------------------ errors ---

func TestGroupListFailureIsShown(t *testing.T) {
	m, mock := newTab(t)
	mock.ConsumerGroupsWithLagFunc = func(context.Context) ([]kafka.ConsumerGroup, error) {
		return nil, errors.New("kafka: broker not available")
	}

	m.section = sectionGroups
	run(m, m.groups.Init())

	if !strings.Contains(render(t, m), "broker not available") {
		t.Errorf("a failed list should say why:\n%s", render(t, m))
	}
}

func TestDisconnectClearsTheGroups(t *testing.T) {
	m, _ := onGroups(t)
	m.SetClient(nil)

	if m.groups.groups != nil || m.groups.detailName != "" {
		t.Error("losing the connection should clear what was on screen")
	}
}

// ---------------------------------------------------------------- helpers ---

// submitForm walks to the Preview button and presses it, which is what the user
// does with Tab and Enter.
func submitForm(m *Model) {
	for range 12 {
		if m.groups.mode != groupModeReset {
			return
		}
		run(m, m.Update(tea.KeyMsg{Type: tea.KeyTab}))
		if m.groups.reset != nil && m.groups.reset.form.FocusedButton() == buttonPreview {
			run(m, m.Update(key("enter")))
			return
		}
	}
}

func lineContaining(t *testing.T, view, needle string) string {
	t.Helper()
	for _, line := range strings.Split(view, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	t.Fatalf("no line contains %q:\n%s", needle, view)
	return ""
}
