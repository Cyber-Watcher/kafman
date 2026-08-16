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

// userMode is what the Users section is showing.
type userMode int

const (
	userModeList userMode = iota
	userModeCreate
	userModePassword
	userModeConfirmDelete
)

// usersSection lists and edits the cluster's SASL/SCRAM principals.
type usersSection struct {
	client kafka.Facade

	table   *table.Model
	users   []kafka.ScramUser
	loading bool
	err     error

	mode    userMode
	form    *userForm
	confirm *modal.Confirm

	// pending is the user a confirmation applies to.
	pending kafka.ScramUser
}

func newUsersSection() *usersSection {
	s := &usersSection{}
	s.table = table.New(
		table.Column{Title: "USERNAME", Width: 0},
		table.Column{Title: "MECHANISMS", Width: 32},
		table.Column{Title: "ITERATIONS", Width: 10, Align: table.Right},
	)
	s.table.Focus()
	return s
}

// SetClient hands over the live client.
func (s *usersSection) SetClient(client kafka.Facade) {
	s.client = client
	if client == nil {
		s.users = nil
		s.table.SetRows(nil)
	}
}

// Init loads the user list.
func (s *usersSection) Init() tea.Cmd {
	if s.client == nil {
		return nil
	}
	s.loading, s.err = true, nil

	client := s.client
	return func() tea.Msg {
		users, err := client.ListScramUsers(context.Background())
		return usersLoadedMsg{Users: users, Err: err}
	}
}

// Capturing reports whether the section owns the keyboard, including Tab.
func (s *usersSection) Capturing() bool { return s.mode != userModeList }

// AcceptsText reports whether typed characters go to a field.
func (s *usersSection) AcceptsText() bool {
	return s.mode == userModeCreate || s.mode == userModePassword ||
		s.mode == userModeConfirmDelete
}

// Breadcrumb names what is open inside the section.
func (s *usersSection) Breadcrumb() []string {
	switch s.mode {
	case userModeCreate:
		return []string{"Create"}
	case userModePassword:
		return []string{s.form.name, "Password"}
	default:
		return nil
	}
}

// Shortcuts implements ui.ShortcutProvider.
func (s *usersSection) Shortcuts() []chrome.Shortcut {
	switch s.mode {
	case userModeCreate, userModePassword:
		return []chrome.Shortcut{
			{Key: "Tab", Label: "Next field"},
			{Key: "Enter", Label: "Confirm"},
			{Key: "Esc", Label: "Cancel"},
		}
	case userModeConfirmDelete:
		return []chrome.Shortcut{{Key: "Enter", Label: "Confirm"}, {Key: "Esc", Label: "Cancel"}}
	}
	return []chrome.Shortcut{
		{Key: "Ctrl+N", Label: "Create"},
		{Key: "Ctrl+E", Label: "Password"},
		{Key: "F2", Label: "Delete"},
		{Key: "/", Label: "Filter"},
	}
}

// Update handles messages and keys.
func (s *usersSection) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case usersLoadedMsg:
		s.loading = false
		if typed.Err != nil {
			s.err = typed.Err
			return nil
		}
		s.users = typed.Users
		s.rebuild()
		return nil

	case userChangedMsg:
		if typed.Err != nil {
			return notify(kafka.Explain(typed.Err), msg.NotifyError)
		}
		return tea.Batch(notify(typed.Summary, msg.NotifySuccess), s.Init())

	case tea.KeyMsg:
		return s.handleKey(typed)
	}
	return nil
}

func (s *usersSection) handleKey(key tea.KeyMsg) tea.Cmd {
	switch s.mode {
	case userModeCreate, userModePassword:
		return s.updateForm(key)
	case userModeConfirmDelete:
		return s.updateConfirm(key)
	}
	return s.updateList(key)
}

func (s *usersSection) updateList(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "ctrl+n":
		s.form = newUserForm(kafka.ScramUser{}, s.userNames())
		s.mode = userModeCreate
		return nil

	case "ctrl+e":
		user := s.selected()
		if user == nil {
			return nil
		}
		s.form = newUserForm(*user, nil)
		s.mode = userModePassword
		return nil

	case "f2":
		user := s.selected()
		if user == nil {
			return nil
		}
		s.pending = *user
		s.confirm = modal.NewConfirm("Delete SCRAM user",
			fmt.Sprintf("Delete %s?\n\n%s goes with it, and anything authenticating\nas that user stops connecting.",
				user.Name, strings.Join(user.Mechanisms(), " and ")))
		s.mode = userModeConfirmDelete
		return nil
	}
	return s.table.Update(key)
}

func (s *usersSection) updateForm(key tea.KeyMsg) tea.Cmd {
	cmd := s.form.Update(key)

	switch {
	case s.form.Cancelled():
		s.mode, s.form = userModeList, nil
		return nil

	case s.form.Submitted():
		spec, err := s.form.Spec()
		if err != nil {
			s.form.setError(err)
			return nil
		}
		creating := s.mode == userModeCreate
		s.mode, s.form = userModeList, nil

		return s.upsert(spec, creating)
	}
	return cmd
}

// upsert writes the credential and reloads the list.
func (s *usersSection) upsert(spec kafka.ScramUserSpec, creating bool) tea.Cmd {
	summary := fmt.Sprintf("changed the password of %s (%s)", spec.Name, spec.Mechanism)
	if creating {
		summary = fmt.Sprintf("created %s with %s", spec.Name, spec.Mechanism)
	}

	client := s.client
	return func() tea.Msg {
		err := client.UpsertScramUser(context.Background(), spec)
		return userChangedMsg{Summary: summary, Err: err}
	}
}

func (s *usersSection) updateConfirm(key tea.KeyMsg) tea.Cmd {
	s.confirm.Update(key)

	switch {
	case s.confirm.Cancelled():
		s.mode, s.confirm = userModeList, nil
		return nil

	case s.confirm.Confirmed():
		user := s.pending
		s.mode, s.confirm, s.pending = userModeList, nil, kafka.ScramUser{}

		client := s.client
		return func() tea.Msg {
			err := client.DeleteScramUser(context.Background(), user.Name, user.Mechanisms())
			return userChangedMsg{Summary: "deleted SCRAM user " + user.Name, Err: err}
		}
	}
	return nil
}

// ------------------------------------------------------------------- rows ---

func (s *usersSection) rebuild() {
	rows := make([]table.Row, 0, len(s.users))
	for _, user := range s.users {
		iterations := make([]string, 0, len(user.Credentials))
		for _, credential := range user.Credentials {
			iterations = append(iterations, itoa(int(credential.Iterations)))
		}

		rows = append(rows, table.Row{
			Cells: []string{
				user.Name,
				strings.Join(user.Mechanisms(), ", "),
				strings.Join(uniqueStrings(iterations), ", "),
			},
			Data: user,
		})
	}
	s.table.SetRows(rows)
}

func (s *usersSection) selected() *kafka.ScramUser {
	row := s.table.SelectedRow()
	if row == nil {
		return nil
	}
	user, ok := row.Data.(kafka.ScramUser)
	if !ok {
		return nil
	}
	return &user
}

func (s *usersSection) userNames() []string {
	names := make([]string, len(s.users))
	for i, user := range s.users {
		names[i] = user.Name
	}
	return names
}

// ------------------------------------------------------------------- view ---

// View renders the section.
func (s *usersSection) View(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	switch s.mode {
	case userModeCreate:
		return panel.Panel{Title: "Create SCRAM user", Focused: true, Width: width, Height: height}.
			Render(th, s.form.View(k, width-4))
	case userModePassword:
		return panel.Panel{Title: "Change password · " + s.form.name, Focused: true,
			Width: width, Height: height}.Render(th, s.form.View(k, width-4))
	}

	content := s.table.View(th, width-2, height-2)
	switch {
	case s.loading && len(s.users) == 0:
		content = th.Dim.Render("  loading users…")
	case s.err != nil:
		content = th.Critical.Render("  " + kafka.Explain(s.err))
	case len(s.users) == 0:
		content = th.Dim.Render("  no SCRAM users; press Ctrl+N to create one")
	}

	counter := itoa(len(s.users))
	if s.table.Filter() != "" {
		counter = fmt.Sprintf("%d / %d", s.table.Len(), s.table.TotalLen())
	}

	body := panel.Panel{
		Title: "SCRAM users", TopRight: counter, Focused: focused,
		Width: width, Height: height,
	}.Render(th, content)

	if s.mode == userModeConfirmDelete {
		return modal.Overlay(body, s.confirm.View(th, min(width-6, 70)))
	}
	return body
}

// ---------------------------------------------------------------- messages ---

// usersLoadedMsg carries the SCRAM principals.
type usersLoadedMsg struct {
	Users []kafka.ScramUser
	Err   error
}

// userChangedMsg reports the outcome of an operation that alters a user.
type userChangedMsg struct {
	Summary string
	Err     error
}

// ------------------------------------------------------------------- form ---

// userForm creates a user or changes its password. Kafka does not distinguish
// the two — both are an upsert of a credential — so neither does this form; only
// the name field differs, because a password change is for one named user.
type userForm struct {
	form *form.Model
	name string

	// existing is true when the user is already there and only the password is
	// being replaced.
	existing bool
	taken    map[string]bool

	nameField  *form.Text
	mechanism  *form.Radio
	password   *form.Text
	repeat     *form.Text
	iterations *form.Text

	err error
}

func newUserForm(user kafka.ScramUser, taken []string) *userForm {
	f := &userForm{
		name:     user.Name,
		existing: user.Name != "",
		taken:    make(map[string]bool, len(taken)),
	}
	for _, name := range taken {
		f.taken[name] = true
	}

	fields := make([]form.Field, 0, 5)

	if !f.existing {
		f.nameField = form.NewText("Username", "").
			WithPlaceholder("billing-worker").
			WithValidator(func(s string) error {
				s = strings.TrimSpace(s)
				switch {
				case s == "":
					return errors.New("required")
				case f.taken[s]:
					return errors.New("already exists; use Ctrl+E to change the password")
				case strings.ContainsAny(s, " \t"):
					return errors.New("no spaces")
				}
				return nil
			})
		fields = append(fields, f.nameField)
	}

	// A user can hold both mechanisms; the form writes one at a time, defaulting
	// to the one it already has so a password change does not quietly add a
	// second credential.
	selected := 1
	if user.Has(kafka.ScramSHA256) && !user.Has(kafka.ScramSHA512) {
		selected = 0
	}
	f.mechanism = form.NewRadio("Mechanism", []string{kafka.ScramSHA256, kafka.ScramSHA512}, selected)

	f.password = form.NewPassword("Password", "").
		WithHint("never shown, never logged").
		WithValidator(func(s string) error {
			if s == "" {
				return errors.New("required")
			}
			return nil
		})

	f.repeat = form.NewPassword("Repeat", "").
		WithValidator(func(s string) error {
			if s != f.password.Value() {
				return errors.New("does not match")
			}
			return nil
		})

	f.iterations = form.NewText("Iterations", "").
		WithPlaceholder(itoa(int(kafka.DefaultScramIterations))).
		WithHint(fmt.Sprintf("blank for %d, the Kafka default and minimum", kafka.DefaultScramIterations)).
		WithValidator(func(s string) error {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			value, err := strconv.Atoi(s)
			if err != nil {
				return errors.New("must be a number")
			}
			if value < int(kafka.DefaultScramIterations) {
				return fmt.Errorf("at least %d", kafka.DefaultScramIterations)
			}
			return nil
		})

	fields = append(fields, f.mechanism, f.password, f.repeat, f.iterations)

	button := buttonCreate
	if f.existing {
		button = buttonApply
	}
	f.form = form.New(fields, button, buttonCancel)
	return f
}

func (f *userForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

func (f *userForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

func (f *userForm) Submitted() bool {
	button := buttonCreate
	if f.existing {
		button = buttonApply
	}
	if f.form.Submitted() == button {
		f.form.Reset()
		return true
	}
	return false
}

func (f *userForm) setError(err error) { f.err = err }

// Spec assembles the credential to write.
func (f *userForm) Spec() (kafka.ScramUserSpec, error) {
	if !f.form.Valid() {
		return kafka.ScramUserSpec{}, errors.New("fix the marked fields first")
	}

	name := f.name
	if f.nameField != nil {
		name = strings.TrimSpace(f.nameField.Value())
	}
	if name == "" {
		return kafka.ScramUserSpec{}, errors.New("a user needs a name")
	}
	if f.password.Value() == "" {
		return kafka.ScramUserSpec{}, errors.New("a user needs a password")
	}
	if f.password.Value() != f.repeat.Value() {
		return kafka.ScramUserSpec{}, errors.New("the passwords do not match")
	}

	spec := kafka.ScramUserSpec{
		Name:      name,
		Mechanism: f.mechanism.Selected(),
		Password:  f.password.Value(),
	}
	if value := strings.TrimSpace(f.iterations.Value()); value != "" {
		iterations, err := strconv.Atoi(value)
		if err != nil {
			return kafka.ScramUserSpec{}, errors.New("iterations must be a number")
		}
		spec.Iterations = int32(iterations)
	}
	return spec, nil
}

func (f *userForm) View(k *ktx.Context, width int) string {
	th := k.Theme

	lines := make([]string, 0, 4)
	if f.existing {
		lines = append(lines,
			th.Value.Render("Replacing the password of "+f.name+"."),
			th.Dim.Render("The old password stops working once the change reaches the brokers."),
			"")
	}
	lines = append(lines, f.form.View(th, width))

	if f.err != nil {
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// ---------------------------------------------------------------- helpers ---

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
