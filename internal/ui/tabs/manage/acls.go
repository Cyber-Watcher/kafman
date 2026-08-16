package manage

import (
	"context"
	"errors"
	"fmt"
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

// aclMode is what the ACLs section is showing.
type aclMode int

const (
	aclModeList aclMode = iota
	aclModeCreate
	aclModeConfirmDelete
)

// aclsSection lists and edits the cluster's access control entries.
type aclsSection struct {
	client kafka.Facade

	table   *table.Model
	acls    []kafka.ACL
	loading bool
	err     error

	mode    aclMode
	form    *aclForm
	confirm *modal.Confirm

	// pending is the entry a confirmation applies to.
	pending kafka.ACL
}

func newACLsSection() *aclsSection {
	s := &aclsSection{}
	s.table = table.New(
		table.Column{Title: "PRINCIPAL", Width: 22},
		table.Column{Title: "RESOURCE", Width: 0, TruncateFront: true},
		table.Column{Title: "TYPE", Width: 8},
		table.Column{Title: "PATTERN", Width: 8},
		table.Column{Title: "OPERATION", Width: 15},
		table.Column{Title: "PERM", Width: 5},
		table.Column{Title: "HOST", Width: 10},
	)
	s.table.Focus()
	return s
}

// SetClient hands over the live client.
func (s *aclsSection) SetClient(client kafka.Facade) {
	s.client = client
	if client == nil {
		s.acls = nil
		s.table.SetRows(nil)
	}
}

// Init loads every entry; the table's own filter narrows them.
func (s *aclsSection) Init() tea.Cmd {
	if s.client == nil {
		return nil
	}
	s.loading, s.err = true, nil

	client := s.client
	return func() tea.Msg {
		acls, err := client.ListACLs(context.Background(), kafka.ACLFilter{})
		return aclsLoadedMsg{ACLs: acls, Err: err}
	}
}

// Capturing reports whether the section owns the keyboard, including Tab.
func (s *aclsSection) Capturing() bool { return s.mode != aclModeList }

// AcceptsText reports whether typed characters go to a field.
func (s *aclsSection) AcceptsText() bool {
	return s.mode == aclModeCreate || s.mode == aclModeConfirmDelete
}

// Breadcrumb names what is open inside the section.
func (s *aclsSection) Breadcrumb() []string {
	if s.mode == aclModeCreate {
		return []string{"Create"}
	}
	return nil
}

// Shortcuts implements ui.ShortcutProvider.
func (s *aclsSection) Shortcuts() []chrome.Shortcut {
	switch s.mode {
	case aclModeCreate:
		return []chrome.Shortcut{
			{Key: "Tab", Label: "Next field"},
			{Key: "←/→", Label: "Choose"},
			{Key: "Enter", Label: "Create"},
			{Key: "Esc", Label: "Cancel"},
		}
	case aclModeConfirmDelete:
		return []chrome.Shortcut{{Key: "Enter", Label: "Confirm"}, {Key: "Esc", Label: "Cancel"}}
	}
	return []chrome.Shortcut{
		{Key: "Ctrl+N", Label: "Create"},
		{Key: "F2", Label: "Delete"},
		{Key: "/", Label: "Filter"},
	}
}

// Update handles messages and keys.
func (s *aclsSection) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case aclsLoadedMsg:
		s.loading = false
		if typed.Err != nil {
			s.err = typed.Err
			return nil
		}
		s.acls = typed.ACLs
		s.rebuild()
		return nil

	case aclChangedMsg:
		if typed.Err != nil {
			return notify(kafka.Explain(typed.Err), msg.NotifyError)
		}
		return tea.Batch(notify(typed.Summary, msg.NotifySuccess), s.Init())

	case tea.KeyMsg:
		return s.handleKey(typed)
	}
	return nil
}

func (s *aclsSection) handleKey(key tea.KeyMsg) tea.Cmd {
	switch s.mode {
	case aclModeCreate:
		return s.updateForm(key)
	case aclModeConfirmDelete:
		return s.updateConfirm(key)
	}
	return s.updateList(key)
}

func (s *aclsSection) updateList(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "ctrl+n":
		// The selected entry seeds the form: rules come in families, and the one
		// on screen is usually the closest thing to the one being added.
		s.form = newACLForm(s.selected())
		s.mode = aclModeCreate
		return nil

	case "f2":
		acl := s.selected()
		if acl == nil {
			return nil
		}
		s.pending = *acl
		s.confirm = modal.NewConfirm("Delete ACL", deleteACLMessage(*acl))
		s.mode = aclModeConfirmDelete
		return nil
	}
	return s.table.Update(key)
}

// deleteACLMessage spells the rule out, because a row of seven short columns is
// not something to confirm from memory — and removing a Deny grants access.
func deleteACLMessage(acl kafka.ACL) string {
	verb := "allowed to"
	if acl.Denies() {
		verb = "denied"
	}

	message := fmt.Sprintf("Remove the rule that %s is %s %s\non %s %s from %s?",
		acl.Principal, verb, acl.Operation, strings.ToLower(acl.ResourceType), acl.Resource(), acl.Host)
	if acl.Denies() {
		message += "\n\nRemoving a denial grants what it was blocking."
	}
	return message
}

func (s *aclsSection) updateForm(key tea.KeyMsg) tea.Cmd {
	cmd := s.form.Update(key)

	switch {
	case s.form.Cancelled():
		s.mode, s.form = aclModeList, nil
		return nil

	case s.form.Submitted():
		acl, err := s.form.ACL()
		if err != nil {
			s.form.setError(err)
			return nil
		}
		s.mode, s.form = aclModeList, nil
		return s.create(acl)
	}
	return cmd
}

// create adds the rule and reloads the list.
func (s *aclsSection) create(acl kafka.ACL) tea.Cmd {
	client := s.client
	return func() tea.Msg {
		err := client.CreateACL(context.Background(), acl)
		return aclChangedMsg{
			Summary: fmt.Sprintf("%s %s %s on %s %s",
				acl.Permission, acl.Principal, acl.Operation,
				strings.ToLower(acl.ResourceType), acl.Resource()),
			Err: err,
		}
	}
}

func (s *aclsSection) updateConfirm(key tea.KeyMsg) tea.Cmd {
	s.confirm.Update(key)

	switch {
	case s.confirm.Cancelled():
		s.mode, s.confirm = aclModeList, nil
		return nil

	case s.confirm.Confirmed():
		acl := s.pending
		s.mode, s.confirm, s.pending = aclModeList, nil, kafka.ACL{}

		client := s.client
		return func() tea.Msg {
			removed, err := client.DeleteACL(context.Background(), acl)
			return aclChangedMsg{
				Summary: fmt.Sprintf("removed %d rule(s) for %s", removed, acl.Principal),
				Err:     err,
			}
		}
	}
	return nil
}

// ------------------------------------------------------------------- rows ---

func (s *aclsSection) rebuild() {
	rows := make([]table.Row, 0, len(s.acls))
	for _, acl := range s.acls {
		row := table.Row{
			Cells: []string{
				acl.Principal, acl.Resource(), acl.ResourceType, acl.PatternType,
				acl.Operation, acl.Permission, acl.Host,
			},
			Data: acl,
		}
		// A denial overrides every allowance, so it is the row to see first.
		if acl.Denies() {
			style := criticalStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	s.table.SetRows(rows)
}

func (s *aclsSection) selected() *kafka.ACL {
	row := s.table.SelectedRow()
	if row == nil {
		return nil
	}
	acl, ok := row.Data.(kafka.ACL)
	if !ok {
		return nil
	}
	return &acl
}

// ------------------------------------------------------------------- view ---

// View renders the section.
func (s *aclsSection) View(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	if s.mode == aclModeCreate {
		return panel.Panel{Title: "Create ACL", Focused: true, Width: width, Height: height}.
			Render(th, s.form.View(k, width-4))
	}

	content := s.table.View(th, width-2, height-2)
	switch {
	case s.loading && len(s.acls) == 0:
		content = th.Dim.Render("  loading ACLs…")
	case s.err != nil:
		content = th.Critical.Render("  " + kafka.Explain(s.err))
	case len(s.acls) == 0:
		content = th.Dim.Render("  no ACLs; every principal is allowed what the broker's default permits")
	}

	counter := itoa(len(s.acls))
	if s.table.Filter() != "" {
		counter = fmt.Sprintf("%d / %d", s.table.Len(), s.table.TotalLen())
	}

	body := panel.Panel{
		Title: "ACLs", TopRight: counter, Focused: focused,
		Width: width, Height: height,
	}.Render(th, content)

	if s.mode == aclModeConfirmDelete {
		return modal.Overlay(body, s.confirm.View(th, min(width-6, 70)))
	}
	return body
}

// ---------------------------------------------------------------- messages ---

// aclsLoadedMsg carries the access control entries.
type aclsLoadedMsg struct {
	ACLs []kafka.ACL
	Err  error
}

// aclChangedMsg reports the outcome of an operation that alters an entry.
type aclChangedMsg struct {
	Summary string
	Err     error
}

// ------------------------------------------------------------------- form ---

// aclForm builds one access control entry.
type aclForm struct {
	form *form.Model

	principal  *form.Text
	resource   *form.Radio
	name       *form.Text
	pattern    *form.Radio
	operation  *form.Radio
	permission *form.Radio
	host       *form.Text

	err error
}

func newACLForm(seed *kafka.ACL) *aclForm {
	f := &aclForm{}

	f.principal = form.NewText("Principal", "User:").
		WithPlaceholder("User:billing-worker").
		WithHint("Type:name, as Kafka writes it").
		WithValidator(func(s string) error {
			s = strings.TrimSpace(s)
			switch {
			case s == "":
				return errors.New("required")
			case !strings.Contains(s, ":"):
				return errors.New("write it as User:name")
			case strings.HasSuffix(s, ":"):
				return errors.New("the name after the colon is missing")
			}
			return nil
		})

	f.resource = form.NewRadio("Resource", kafka.ACLResourceTypes, 0)

	f.name = form.NewText("Name", "").
		WithPlaceholder("orders.events").
		WithHint("* for every resource; a prefix when Pattern is Prefixed").
		WithValidator(func(s string) error {
			if strings.TrimSpace(s) == "" {
				return errors.New("required")
			}
			return nil
		})

	f.pattern = form.NewRadio("Pattern", kafka.ACLPatternTypes, 0)
	f.operation = form.NewRadio("Operation", kafka.ACLOperations, 1)
	f.permission = form.NewRadio("Permission", kafka.ACLPermissions, 0)

	f.host = form.NewText("Host", "*").
		WithHint("* for any host")

	if seed != nil {
		f.principal.SetValue(seed.Principal)
		f.resource.SelectValue(seed.ResourceType)
		f.name.SetValue(seed.ResourceName)
		f.pattern.SelectValue(seed.PatternType)
		f.operation.SelectValue(seed.Operation)
		// The permission is deliberately not seeded from a denial: a Deny copied
		// by accident is the one mistake here that quietly breaks a consumer.
		f.host.SetValue(seed.Host)
	}

	f.form = form.New([]form.Field{
		f.principal,
		form.NewSection("Resource"),
		f.resource, f.name, f.pattern,
		form.NewSection("Rule"),
		f.operation, f.permission, f.host,
	}, buttonCreate, buttonCancel)

	return f
}

func (f *aclForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

func (f *aclForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

func (f *aclForm) Submitted() bool {
	if f.form.Submitted() == buttonCreate {
		f.form.Reset()
		return true
	}
	return false
}

func (f *aclForm) setError(err error) { f.err = err }

// ACL assembles the entry.
func (f *aclForm) ACL() (kafka.ACL, error) {
	if !f.form.Valid() {
		return kafka.ACL{}, errors.New("fix the marked fields first")
	}

	host := strings.TrimSpace(f.host.Value())
	if host == "" {
		host = "*"
	}

	return kafka.ACL{
		Principal:    strings.TrimSpace(f.principal.Value()),
		Host:         host,
		Operation:    f.operation.Selected(),
		Permission:   f.permission.Selected(),
		ResourceType: f.resource.Selected(),
		ResourceName: strings.TrimSpace(f.name.Value()),
		PatternType:  f.pattern.Selected(),
	}, nil
}

func (f *aclForm) View(k *ktx.Context, width int) string {
	th := k.Theme

	lines := []string{f.form.View(th, width), ""}

	// The sentence the rule amounts to, so it is read as a sentence before it is
	// created — seven fields are easy to fill in and hard to reread.
	if acl, err := f.ACL(); err == nil {
		style := th.Success
		if acl.Denies() {
			style = th.Critical
		}
		lines = append(lines, style.Render("→ "+aclSentence(acl)))
	}
	if f.err != nil {
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// aclSentence renders an entry as the rule it is.
func aclSentence(acl kafka.ACL) string {
	verb := "may"
	if acl.Denies() {
		verb = "may not"
	}
	from := ""
	if acl.Host != "*" && acl.Host != "" {
		from = " from " + acl.Host
	}
	return fmt.Sprintf("%s %s %s %s %s%s",
		acl.Principal, verb, strings.ToLower(acl.Operation),
		strings.ToLower(acl.ResourceType), acl.Resource(), from)
}
