package manage

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// onUsers opens the Users section with its list loaded.
func onUsers(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	m, mock := newTab(t)
	m.section = sectionUsers
	run(m, m.users.Init())
	return m, mock
}

func TestUsersAreListed(t *testing.T) {
	m, _ := onUsers(t)
	view := render(t, m)

	for _, want := range []string{"admin", "SCRAM-SHA-256, SCRAM-SHA-512", "8192", "readonly-audit", "4096"} {
		if !strings.Contains(view, want) {
			t.Errorf("the user list should show %q:\n%s", want, view)
		}
	}
}

func TestUserListFailureIsShown(t *testing.T) {
	m, mock := newTab(t)
	mock.ListScramUsersFunc = func(context.Context) ([]kafka.ScramUser, error) {
		return nil, errors.New("kafka: insufficient permissions to describe credentials")
	}

	m.section = sectionUsers
	run(m, m.users.Init())

	if !strings.Contains(render(t, m), "insufficient permissions") {
		t.Errorf("a failed list should say why:\n%s", render(t, m))
	}
}

func TestCreateUserThroughTheForm(t *testing.T) {
	m, mock := onUsers(t)

	run(m, m.Update(key("ctrl+n")))
	if m.users.mode != userModeCreate {
		t.Fatal("Ctrl+N should open the create form")
	}

	form := m.users.form
	form.nameField.SetValue("new-worker")
	form.password.SetValue("s3cret")
	form.repeat.SetValue("s3cret")
	submitUserForm(m)

	if len(mock.Upserted) != 1 {
		t.Fatalf("the user should be written, got %v", mock.Upserted)
	}
	spec := mock.Upserted[0]
	if spec.Name != "new-worker" || spec.Password != "s3cret" {
		t.Errorf("spec = %+v, want the typed name and password", spec)
	}
	if spec.Mechanism != kafka.ScramSHA512 {
		t.Errorf("mechanism = %q, want the default %s", spec.Mechanism, kafka.ScramSHA512)
	}
	if spec.Iterations != 0 {
		t.Errorf("iterations = %d, want 0 so the domain applies the Kafka default", spec.Iterations)
	}
}

// The password is the one value on this screen that must never be rendered.
func TestPasswordIsNeverEchoed(t *testing.T) {
	m, _ := onUsers(t)
	run(m, m.Update(key("ctrl+n")))

	m.users.form.nameField.SetValue("new-worker")

	// Username → Mechanism → Password, then type into it.
	run(m, m.Update(tea.KeyMsg{Type: tea.KeyTab}))
	run(m, m.Update(tea.KeyMsg{Type: tea.KeyTab}))
	typeText(m, "hunter2")

	if got := m.users.form.password.Value(); got != "hunter2" {
		t.Fatalf("the password field holds %q; the test typed into the wrong field", got)
	}

	view := render(t, m)
	if strings.Contains(view, "hunter2") {
		t.Errorf("a typed password must not appear on screen:\n%s", view)
	}
}

func TestPasswordsMustMatch(t *testing.T) {
	f := newUserForm(kafka.ScramUser{}, nil)
	f.nameField.SetValue("new-worker")
	f.password.SetValue("one")
	f.repeat.SetValue("other")

	if _, err := f.Spec(); err == nil {
		t.Error("mismatched passwords should be refused before reaching the cluster")
	}
}

func TestUserFormRefusesAnExistingName(t *testing.T) {
	f := newUserForm(kafka.ScramUser{}, []string{"admin"})
	f.nameField.SetValue("admin")

	if f.nameField.Err() == nil {
		t.Error("an existing user should be marked, with the way to change its password")
	}
}

func TestUserFormRefusesTooFewIterations(t *testing.T) {
	f := newUserForm(kafka.ScramUser{}, nil)
	f.nameField.SetValue("new-worker")
	f.password.SetValue("s3cret")
	f.repeat.SetValue("s3cret")
	f.iterations.SetValue("1024")

	if _, err := f.Spec(); err == nil {
		t.Errorf("Kafka's minimum of %d should be enforced locally", kafka.DefaultScramIterations)
	}
}

// Changing a password edits one named user, so the form has no name field and
// starts on the mechanism that user already holds.
func TestChangePasswordKeepsTheMechanism(t *testing.T) {
	m, mock := onUsers(t)

	run(m, m.Update(key("down"))) // billing-worker, SCRAM-SHA-512 only
	run(m, m.Update(key("ctrl+e")))

	if m.users.mode != userModePassword {
		t.Fatal("Ctrl+E should open the password form")
	}
	if m.users.form.nameField != nil {
		t.Error("a password change has no name field: the user is already chosen")
	}
	if got := m.users.form.mechanism.Selected(); got != kafka.ScramSHA512 {
		t.Errorf("mechanism = %q, want the one the user holds", got)
	}

	m.users.form.password.SetValue("new-one")
	m.users.form.repeat.SetValue("new-one")
	submitUserForm(m)

	if len(mock.Upserted) != 1 || mock.Upserted[0].Name != "billing-worker" {
		t.Fatalf("the selected user should be updated, got %v", mock.Upserted)
	}
	if mock.Upserted[0].Mechanism != kafka.ScramSHA512 {
		t.Errorf("mechanism = %q, want no second credential to appear", mock.Upserted[0].Mechanism)
	}
}

func TestDeleteUserAsksFirst(t *testing.T) {
	m, mock := onUsers(t)

	run(m, m.Update(key("f2")))
	if len(mock.DeletedUsers) != 0 {
		t.Fatal("nothing should be deleted before the confirmation")
	}
	if !strings.Contains(render(t, m), "Delete SCRAM user") {
		t.Fatal("F2 should open a confirmation")
	}

	run(m, m.Update(key("enter")))
	if len(mock.DeletedUsers) != 1 || mock.DeletedUsers[0] != "admin" {
		t.Errorf("the confirmed user should be deleted, got %v", mock.DeletedUsers)
	}
}

func TestDeleteUserCanBeCancelled(t *testing.T) {
	m, mock := onUsers(t)

	run(m, m.Update(key("f2")))
	run(m, m.Update(key("esc")))

	if len(mock.DeletedUsers) != 0 {
		t.Errorf("a cancelled delete should not happen, got %v", mock.DeletedUsers)
	}
	if m.users.mode != userModeList {
		t.Error("Esc should close the confirmation")
	}
}

func TestDisconnectClearsTheUsers(t *testing.T) {
	m, _ := onUsers(t)
	m.SetClient(nil)

	if m.users.users != nil {
		t.Error("losing the connection should clear what was on screen")
	}
}

// submitUserForm walks to the submit button and presses it.
func submitUserForm(m *Model) {
	for range 12 {
		if m.users.mode != userModeCreate && m.users.mode != userModePassword {
			return
		}
		run(m, m.Update(tea.KeyMsg{Type: tea.KeyTab}))
		if m.users.form == nil {
			return
		}
		if button := m.users.form.form.FocusedButton(); button == buttonCreate || button == buttonApply {
			run(m, m.Update(key("enter")))
			return
		}
	}
}
