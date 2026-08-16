package clusters

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

const threeClusters = `
current-cluster: local
clusters:
  - name: local
    brokers: [localhost:9092]
  - name: staging
    brokers: [stg-1:9092, stg-2:9092]
    sasl:
      enabled: true
      mechanism: scram-sha-512
      username: u
      password: p
  - name: prod
    brokers: [prod:9094]
    tls:
      enabled: true
      insecure: true
`

func newTab(t *testing.T, body string) (*Model, *config.Config, string) {
	t.Helper()
	dir := t.TempDir()

	path := filepath.Join(dir, config.FileName)
	if err := os.WriteFile(path, []byte(body), config.FileMode); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return New(cfg, cfg.CurrentCluster), cfg, dir
}

func render(t *testing.T, m *Model, cfg *config.Config) string {
	t.Helper()
	k := ktx.New(cfg, theme.New(theme.Dark, false), 110, 30)
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
	case "ctrl+n":
		return tea.KeyMsg{Type: tea.KeyCtrlN}
	case "ctrl+e":
		return tea.KeyMsg{Type: tea.KeyCtrlE}
	case "f2":
		return tea.KeyMsg{Type: tea.KeyF2}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

// typeText feeds a string one rune at a time, as a keyboard would.
func typeText(m *Model, s string) {
	for _, r := range s {
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
}

func TestListShowsEveryCluster(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	view := render(t, m, cfg)

	for _, want := range []string{"local", "staging", "prod"} {
		if !strings.Contains(view, want) {
			t.Errorf("the list should contain %q:\n%s", want, view)
		}
	}
	// The security profile is what tells a user why a connection behaves as it
	// does, so it belongs in the list rather than only in the details.
	for _, want := range []string{"PLAINTEXT", "SASL_PLAINTEXT/SCRAM-SHA-512", "SSL"} {
		if !strings.Contains(view, want) {
			t.Errorf("the list should show the security profile %q:\n%s", want, view)
		}
	}
}

func TestCurrentClusterIsMarked(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.SetCurrent("local", chrome.Connected)

	view := render(t, m, cfg)
	if !strings.Contains(view, "▸ local") {
		t.Errorf("the connected cluster should be marked:\n%s", view)
	}
	if !strings.Contains(view, "● connected") {
		t.Errorf("the connected cluster should show its live state:\n%s", view)
	}
	if !strings.Contains(view, "○ not tested") {
		t.Errorf("untested clusters should say so:\n%s", view)
	}
}

func TestEnterRequestsASwitch(t *testing.T) {
	m, _, _ := newTab(t, threeClusters)
	m.SetCurrent("local", chrome.Connected)

	m.Update(key("j")) // move to staging
	cmd := m.Update(key("enter"))
	if cmd == nil {
		t.Fatal("Enter on another cluster should ask for a switch")
	}
	switch typed := cmd().(type) {
	case msg.SwitchCluster:
		if typed.Name != "staging" {
			t.Errorf("switch requested for %q, want staging", typed.Name)
		}
	default:
		t.Errorf("got %T, want msg.SwitchCluster", typed)
	}
}

func TestEnterOnTheCurrentClusterDoesNothing(t *testing.T) {
	m, _, _ := newTab(t, threeClusters)
	m.SetCurrent("local", chrome.Connected)

	// The cursor starts on local, which is already current.
	if cmd := m.Update(key("enter")); cmd != nil {
		t.Errorf("switching to the current cluster should be a no-op, got %T", cmd())
	}
}

func TestTestKeyRequestsATest(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("j"))

	cmd := m.Update(key("t"))
	if cmd == nil {
		t.Fatal("t should request a connection test")
	}
	if typed, ok := cmd().(msg.TestCluster); !ok || typed.Name != "staging" {
		t.Errorf("got %#v, want a test of staging", cmd())
	}

	// While the test runs the row must say so, rather than looking untested.
	if view := render(t, m, cfg); !strings.Contains(view, "◌ testing") {
		t.Errorf("a running test should be visible:\n%s", view)
	}
}

func TestFailedTestShowsTheFullError(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("j"))
	m.Update(key("t"))

	// A truncated TLS error is useless, so the whole thing is rendered.
	longError := errors.New("connect to cluster \"staging\": kafka: client has run out of " +
		"available brokers to talk to: tls: failed to verify certificate: x509: certificate " +
		"signed by unknown authority")
	m.Update(msg.ClusterTested{
		Name: "staging",
		Result: kafka.ConnCheck{
			Err:       longError,
			Diagnosis: "the broker's certificate is signed by a CA that is not trusted",
		},
	})

	view := render(t, m, cfg)
	if !strings.Contains(view, "✖ failed") {
		t.Errorf("the row should show the failure:\n%s", view)
	}
	if !strings.Contains(view, "not trusted") {
		t.Errorf("the diagnosis should be shown:\n%s", view)
	}
	// Wrapped across lines, but every word present.
	flat := strings.Join(strings.Fields(view), " ")
	if !strings.Contains(flat, "certificate signed by unknown authority") {
		t.Errorf("the full error text should survive wrapping:\n%s", view)
	}
}

func TestDetailsHideSecrets(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("j")) // staging, which has a password

	view := render(t, m, cfg)
	if strings.Contains(view, "password: p") || strings.Contains(view, " p ") {
		t.Errorf("the password must never be rendered:\n%s", view)
	}
	if !strings.Contains(view, "(hidden)") {
		t.Errorf("the details should say the password is hidden:\n%s", view)
	}
	if !strings.Contains(view, "scram-sha-512") {
		t.Errorf("the mechanism should be shown:\n%s", view)
	}
}

func TestInsecureTLSIsFlagged(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("j"))
	m.Update(key("j")) // prod, with insecure: true

	if view := render(t, m, cfg); !strings.Contains(view, "verification disabled") {
		t.Errorf("disabled certificate verification should be called out:\n%s", view)
	}
}

func TestRemoveRequiresConfirmation(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("j")) // staging

	m.Update(key("f2"))
	if m.mode != modeConfirmDelete {
		t.Fatal("F2 should open a confirmation")
	}
	if len(cfg.Clusters) != 3 {
		t.Error("nothing should be removed before confirming")
	}

	m.Update(key("esc"))
	if m.mode != modeList {
		t.Error("Esc should dismiss the confirmation")
	}
	if len(cfg.Clusters) != 3 {
		t.Error("cancelling must not remove anything")
	}

	m.Update(key("f2"))
	m.Update(key("y"))
	if len(cfg.Clusters) != 2 {
		t.Errorf("after confirming, %d clusters remain, want 2", len(cfg.Clusters))
	}
	if _, err := cfg.Cluster("staging"); err == nil {
		t.Error("staging should be gone")
	}
}

func TestTheLastClusterCannotBeRemoved(t *testing.T) {
	m, cfg, _ := newTab(t, "clusters:\n  - name: only\n    brokers: [h:9092]\n")

	cmd := m.Update(key("f2"))
	if m.mode == modeConfirmDelete {
		t.Error("removing the only cluster should not even be offered")
	}
	if cmd == nil {
		t.Fatal("the refusal should be explained")
	}
	if typed, ok := cmd().(msg.Notify); !ok || !strings.Contains(typed.Text, "last cluster") {
		t.Errorf("got %#v, want an explanation", cmd())
	}
	if len(cfg.Clusters) != 1 {
		t.Error("the cluster must still be there")
	}
}

func TestAddClusterThroughTheForm(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)

	m.Update(key("ctrl+n"))
	if m.mode != modeForm {
		t.Fatal("Ctrl+N should open the form")
	}
	if !m.AcceptsText() {
		t.Error("with a form open, letters must reach the form rather than trigger shortcuts")
	}

	typeText(m, "new-cluster")
	m.Update(key("tab")) // colour
	m.Update(key("tab")) // brokers
	typeText(m, "kafka.new:9092")

	pressButton(t, m, buttonSave)

	if m.mode != modeList {
		t.Errorf("after saving, the form should close")
	}
	added, err := cfg.Cluster("new-cluster")
	if err != nil {
		t.Fatalf("the cluster was not added: %v", err)
	}
	if len(added.Brokers) != 1 || added.Brokers[0] != "kafka.new:9092" {
		t.Errorf("brokers = %v, want [kafka.new:9092]", added.Brokers)
	}
}

// pressButton walks the focus to the named button and presses it.
func pressButton(t *testing.T, m *Model, label string) {
	t.Helper()
	for range 40 {
		m.Update(key("tab"))
		if m.form == nil {
			return // the form closed, so the button was reached
		}
		if focusedButton(m) == label {
			m.Update(key("enter"))
			return
		}
	}
	t.Fatalf("never reached the %q button", label)
}

func focusedButton(m *Model) string { return m.form.form.FocusedButton() }

func TestFormRejectsInvalidInput(t *testing.T) {
	m, cfg, _ := newTab(t, threeClusters)
	m.Update(key("ctrl+n"))

	// A name that already exists, and no brokers at all.
	typeText(m, "local")
	if _, err := m.form.Cluster(); err == nil {
		t.Error("a duplicate name should be refused")
	}

	if len(cfg.Clusters) != 3 {
		t.Error("nothing should have been saved")
	}
}

func TestFormValidatesBrokerFormat(t *testing.T) {
	m, _, _ := newTab(t, threeClusters)
	m.Update(key("ctrl+n"))

	typeText(m, "valid-name")
	m.Update(key("tab"))
	m.Update(key("tab"))
	typeText(m, "no-port-here")

	if _, err := m.form.Cluster(); err == nil {
		t.Error("a broker without a port should be refused")
	}
}

func TestFormValidatesCertificatePaths(t *testing.T) {
	m, _, dir := newTab(t, threeClusters)

	// A real file next to the config must be accepted by its relative name.
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	m.Update(key("ctrl+e"))
	m.form.tlsCA.SetValue("ca.crt")
	if err := m.form.tlsCA.Err(); err != nil {
		t.Errorf("an existing file should validate: %v", err)
	}

	m.form.tlsCA.SetValue("missing.crt")
	if m.form.tlsCA.Err() == nil {
		t.Error("a missing file should be flagged while typing")
	}
}

func TestEditFormPreloadsValues(t *testing.T) {
	m, _, _ := newTab(t, threeClusters)
	m.Update(key("j")) // staging
	m.Update(key("ctrl+e"))

	if m.form == nil {
		t.Fatal("Ctrl+E should open the form")
	}
	if got := m.form.name.Value(); got != "staging" {
		t.Errorf("name = %q, want staging", got)
	}
	if got := m.form.brokers.Value(); !strings.Contains(got, "stg-1:9092") {
		t.Errorf("brokers = %q, want the stored list", got)
	}
	if !m.form.saslEnabled.Checked() {
		t.Error("SASL should be pre-ticked for staging")
	}
	if got := m.form.saslMechanism.Selected(); got != config.MechanismSCRAMSHA512 {
		t.Errorf("mechanism = %q, want scram-sha-512", got)
	}
}

func TestEscapeClosesTheForm(t *testing.T) {
	m, _, _ := newTab(t, threeClusters)
	m.Update(key("ctrl+n"))
	m.Update(key("esc"))

	if m.mode != modeList {
		t.Error("Esc should close the form")
	}
	if m.AcceptsText() {
		t.Error("with the form closed, keys are shortcuts again")
	}
}
