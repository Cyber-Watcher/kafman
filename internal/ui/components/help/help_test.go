package help

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// secretConfig is a cluster with every kind of credential filled in, so a test
// can check that none of them reaches the screen.
const secretConfig = `
current-cluster: prod
clusters:
  - name: prod
    brokers: [broker-1:9094, broker-2:9094]
    client-id: kafman
    kafka-version: "3.9.0"
    tls:
      enabled: true
      insecure: true
      cert-key-passphrase: super-secret-passphrase
    sasl:
      enabled: true
      mechanism: scram-sha-512
      username: kafman
      password: hunter2
    jmx:
      enabled: true
      targets:
        - broker-id: 1
          url: http://broker-1:9404/metrics
`

func render(t *testing.T, source string, height int) string {
	t.Helper()

	cfg, err := config.LoadFrom([]byte(source), t.TempDir())
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	m := New("/var/log/kafman.log")
	k := ktx.New(cfg, theme.New(theme.Dark, false), 120, height)
	k.AvailableHeight = height

	// The screen scrolls; the whole of it is what matters here.
	var b strings.Builder
	for page := range 6 {
		m.offset = page * height
		b.WriteString(ansi.Strip(m.View(k)))
		b.WriteString("\n")
	}
	return b.String()
}

// Every tab's keys belong here: the help screen is the only place the whole set
// is written down.
func TestEveryTabIsDocumented(t *testing.T) {
	view := render(t, secretConfig, 40)

	for _, tab := range []string{"Overview", "Messages", "Manage", "Monitoring", "Clusters"} {
		if !strings.Contains(view, tab) {
			t.Errorf("the help screen should document the %s tab:\n%s", tab, view)
		}
	}
	for _, key := range []string{"Ctrl+R", "Ctrl+D", "Ctrl+N", "F2", "j", "+ / -"} {
		if !strings.Contains(view, key) {
			t.Errorf("the help screen should mention %q", key)
		}
	}
}

// This screen ends up in screenshots and bug reports.
func TestNoSecretsOnTheHelpScreen(t *testing.T) {
	view := render(t, secretConfig, 40)

	for _, secret := range []string{"hunter2", "super-secret-passphrase"} {
		if strings.Contains(view, secret) {
			t.Errorf("a credential reached the help screen: %q\n%s", secret, view)
		}
	}
	if !strings.Contains(view, "(hidden)") {
		t.Error("the password should be shown as hidden rather than omitted silently")
	}
}

// The settings that decide how the client behaves are what a question about
// behaviour comes down to.
func TestEffectiveSettingsAreShown(t *testing.T) {
	view := render(t, secretConfig, 40)

	for _, want := range []string{
		"Current cluster · prod",
		"broker-1:9094",
		"SASL_SSL",
		"scram-sha-512",
		"3.9.0",
		"http://broker-1:9404/metrics",
		"/var/log/kafman.log",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("the help screen should show %q:\n%s", want, view)
		}
	}

	// Disabled certificate verification is a security decision, and it says so
	// rather than being one absent line among many.
	if !strings.Contains(view, "DISABLED") {
		t.Error("insecure TLS should be called out")
	}
}

// The query language is only discoverable from this screen.
func TestQueryLanguageIsDocumented(t *testing.T) {
	view := render(t, secretConfig, 40)

	for _, want := range []string{"Query language", "order by", "limit", "from"} {
		if !strings.Contains(view, want) {
			t.Errorf("the help screen should document %q", want)
		}
	}
}

func TestVersionIsShown(t *testing.T) {
	view := render(t, secretConfig, 40)

	for _, want := range []string{"Version", "commit", "built", "go1."} {
		if !strings.Contains(view, want) {
			t.Errorf("the help screen should show %q", want)
		}
	}
}

// The key column is aligned by display width; arrows and box characters are
// multi-byte and a byte count leaves those rows short.
func TestKeyColumnIsAligned(t *testing.T) {
	view := render(t, secretConfig, 40)

	var arrows, plain string
	for _, line := range strings.Split(view, "\n") {
		switch {
		case strings.Contains(line, "previous / next tab") && strings.Contains(line, "←"):
			arrows = line
		case strings.Contains(line, "switch cluster"):
			plain = line
		}
	}
	if arrows == "" || plain == "" {
		t.Fatal("the two reference rows were not rendered")
	}

	// Display columns, not byte offsets: the arrow row is multi-byte and a byte
	// index would differ even when the rows line up on screen.
	column := func(line, word string) int {
		return ansi.StringWidth(line[:strings.Index(line, word)])
	}
	if a, b := column(arrows, "previous"), column(plain, "switch"); a != b {
		t.Errorf("descriptions start at columns %d and %d:\n%q\n%q", a, b, arrows, plain)
	}
}
