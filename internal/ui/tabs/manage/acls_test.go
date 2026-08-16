package manage

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// onACLs opens the ACLs section with its list loaded.
func onACLs(t *testing.T) (*Model, *kafka.Mock) {
	t.Helper()

	m, mock := newTab(t)
	m.section = sectionACLs
	run(m, m.acls.Init())
	return m, mock
}

func TestACLsAreListed(t *testing.T) {
	m, _ := onACLs(t)
	view := render(t, m)

	for _, want := range []string{
		"User:billing-worker", "orders.events", "Prefixed", "Describe", "Deny", "Allow",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("the ACL list should show %q:\n%s", want, view)
		}
	}
}

// A prefixed rule covers everything under it, and reading the name alone would
// suggest it covers one group.
func TestPrefixedResourceIsMarked(t *testing.T) {
	m, _ := onACLs(t)

	if !strings.Contains(render(t, m), "billing-*") {
		t.Error("a prefixed resource should be rendered with its star")
	}
}

// A denial overrides every allowance, so it must not look like just another row.
func TestDenyRowsAreMarked(t *testing.T) {
	m, _ := onACLs(t)

	var deny *kafka.ACL
	for _, row := range m.acls.table.Rows() {
		acl, ok := row.Data.(kafka.ACL)
		if !ok || !acl.Denies() {
			continue
		}
		deny = &acl
		if row.Style == nil {
			t.Errorf("the Deny rule for %s should be styled apart from the rest", acl.Principal)
		}
	}
	if deny == nil {
		t.Fatal("the mock cluster should carry a denial to render")
	}
}

func TestACLListFailureIsShown(t *testing.T) {
	m, mock := newTab(t)
	mock.ListACLsFunc = func(context.Context, kafka.ACLFilter) ([]kafka.ACL, error) {
		return nil, errors.New("kafka: security features are disabled — the cluster has no authorizer configured")
	}

	m.section = sectionACLs
	run(m, m.acls.Init())

	if !strings.Contains(render(t, m), "no authorizer") {
		t.Errorf("a cluster without an authorizer should say so:\n%s", render(t, m))
	}
}

func TestCreateACLThroughTheForm(t *testing.T) {
	m, mock := onACLs(t)

	run(m, m.Update(key("ctrl+n")))
	if m.acls.mode != aclModeCreate {
		t.Fatal("Ctrl+N should open the create form")
	}

	f := m.acls.form
	f.principal.SetValue("User:new-worker")
	f.resource.SelectValue("Topic")
	f.name.SetValue("orders.events")
	f.pattern.SelectValue("Literal")
	f.operation.SelectValue("Read")
	f.permission.SelectValue("Allow")
	submitACLForm(m)

	if len(mock.CreatedACLs) != 1 {
		t.Fatalf("the rule should be created, got %v", mock.CreatedACLs)
	}
	acl := mock.CreatedACLs[0]
	if acl.Principal != "User:new-worker" || acl.Operation != "Read" || acl.ResourceName != "orders.events" {
		t.Errorf("acl = %+v, want the values from the form", acl)
	}
	if acl.Host != "*" {
		t.Errorf("host = %q, want the default *", acl.Host)
	}
}

// A rule is seven short fields; the form spells out what they add up to.
func TestFormShowsTheRuleAsASentence(t *testing.T) {
	m, _ := onACLs(t)
	run(m, m.Update(key("ctrl+n")))

	f := m.acls.form
	f.principal.SetValue("User:new-worker")
	f.name.SetValue("orders.events")
	f.operation.SelectValue("Write")
	f.permission.SelectValue("Deny")

	view := render(t, m)
	if !strings.Contains(view, "User:new-worker may not write topic orders.events") {
		t.Errorf("the form should read the rule back as a sentence:\n%s", view)
	}
}

// A principal without its type never matches anything, and Kafka accepts the
// entry regardless.
func TestPrincipalNeedsItsType(t *testing.T) {
	f := newACLForm(nil)
	f.principal.SetValue("billing-worker")

	if f.principal.Err() == nil {
		t.Error("a principal without Type: should be marked")
	}
}

// Copying the selected rule saves retyping six fields — but not the permission:
// a Deny carried over by accident is the mistake that quietly breaks a consumer.
func TestCreateSeedsFromTheSelectedRuleExceptThePermission(t *testing.T) {
	m, _ := onACLs(t)

	selected := m.acls.selected()
	if selected == nil || !selected.Denies() {
		t.Fatal("the first row of the mock cluster should be the denial")
	}

	run(m, m.Update(key("ctrl+n")))
	f := m.acls.form

	if got := f.principal.Value(); got != selected.Principal {
		t.Errorf("principal = %q, want it seeded from the selected rule", got)
	}
	if got := f.name.Value(); got != selected.ResourceName {
		t.Errorf("resource name = %q, want it seeded", got)
	}
	if got := f.permission.Selected(); got != "Allow" {
		t.Errorf("permission = %q, want Allow: a denial must be chosen deliberately", got)
	}
}

func TestDeleteACLAsksFirstAndNamesTheRule(t *testing.T) {
	m, mock := onACLs(t)

	run(m, m.Update(key("f2")))
	if len(mock.DeletedACLs) != 0 {
		t.Fatal("nothing should be deleted before the confirmation")
	}

	view := render(t, m)
	for _, want := range []string{"Delete ACL", "denied", "Removing a denial grants"} {
		if !strings.Contains(view, want) {
			t.Errorf("the confirmation should say %q:\n%s", want, view)
		}
	}

	run(m, m.Update(key("enter")))
	if len(mock.DeletedACLs) != 1 {
		t.Fatalf("the confirmed rule should be deleted, got %v", mock.DeletedACLs)
	}
	if got := mock.DeletedACLs[0].PatternType; got != "Literal" {
		t.Errorf("pattern type = %q, want the exact rule so a prefixed one survives", got)
	}
}

func TestDeleteACLCanBeCancelled(t *testing.T) {
	m, mock := onACLs(t)

	run(m, m.Update(key("f2")))
	run(m, m.Update(key("esc")))

	if len(mock.DeletedACLs) != 0 {
		t.Errorf("a cancelled delete should not happen, got %v", mock.DeletedACLs)
	}
	if m.acls.mode != aclModeList {
		t.Error("Esc should close the confirmation")
	}
}

func TestDisconnectClearsTheACLs(t *testing.T) {
	m, _ := onACLs(t)
	m.SetClient(nil)

	if m.acls.acls != nil {
		t.Error("losing the connection should clear what was on screen")
	}
}

// submitACLForm walks to the Create button and presses it.
func submitACLForm(m *Model) {
	for range 16 {
		if m.acls.mode != aclModeCreate {
			return
		}
		run(m, m.Update(tea.KeyMsg{Type: tea.KeyTab}))
		if m.acls.form != nil && m.acls.form.form.FocusedButton() == buttonCreate {
			run(m, m.Update(key("enter")))
			return
		}
	}
}
