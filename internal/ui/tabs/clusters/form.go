package clusters

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/form"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
)

// Button labels.
const (
	buttonTest   = "Test"
	buttonSave   = "Save"
	buttonCancel = "Cancel"
)

// clusterForm edits one cluster definition.
type clusterForm struct {
	form    *form.Model
	editing bool

	// original is the name before editing, so a rename can find the old entry.
	original string

	// configDir anchors relative certificate paths, matching what the config
	// package does when it loads them.
	configDir string

	name     *form.Text
	colour   *form.Text
	brokers  *form.Text
	version  *form.Text
	clientID *form.Text
	timeout  *form.Text

	tlsEnabled  *form.Checkbox
	tlsInsecure *form.Checkbox
	tlsCA       *form.Text
	tlsCert     *form.Text
	tlsKey      *form.Text
	tlsPass     *form.Text

	saslEnabled   *form.Checkbox
	saslMechanism *form.Radio
	saslUser      *form.Text
	saslPassword  *form.Text

	jmxEnabled *form.Checkbox
	jmxURL     *form.Text

	nodeEnabled *form.Checkbox
	nodeURL     *form.Text

	testing bool
}

func newClusterForm(existing *config.Cluster, cfg *config.Config) *clusterForm {
	f := &clusterForm{
		editing:   existing != nil,
		configDir: cfg.Dir(),
	}

	cl := config.Cluster{
		ClientID:       config.DefaultClientID,
		RequestTimeout: config.Duration(config.DefaultRequestTimeout),
	}
	if existing != nil {
		cl = *existing
		f.original = existing.Name
	}

	f.name = form.NewText("Name", cl.Name).
		WithPlaceholder("production-dc1").
		WithValidator(func(s string) error {
			if strings.TrimSpace(s) == "" {
				return errors.New("required")
			}
			// A rename must not collide with another entry.
			if s != f.original {
				if _, err := cfg.Cluster(s); err == nil {
					return errors.New("already exists")
				}
			}
			return nil
		})

	f.colour = form.NewText("Colour", cl.Color).
		WithPlaceholder("#FF5F5F").
		WithHint("marks the cluster in the header").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			if !strings.HasPrefix(s, "#") || len(s) != 7 {
				return errors.New("use #RRGGBB")
			}
			return nil
		})

	f.brokers = form.NewText("Bootstrap", strings.Join(cl.Brokers, ", ")).
		WithPlaceholder("host:9092, host2:9092").
		WithValidator(validateBrokers)

	f.version = form.NewText("Kafka version", cl.KafkaVersion).
		WithPlaceholder("3.9.0").
		WithHint("blank = negotiate").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			if strings.Count(s, ".") < 2 {
				return errors.New("use x.y.z")
			}
			return nil
		})

	f.clientID = form.NewText("Client ID", cl.ClientID)

	f.timeout = form.NewText("Timeout", cl.RequestTimeout.String()).
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			if _, err := time.ParseDuration(s); err != nil {
				return errors.New("use 10s, 1m30s")
			}
			return nil
		})

	f.tlsEnabled = form.NewCheckbox("Enabled", cl.TLS.Enabled)
	f.tlsInsecure = form.NewCheckbox("Skip verification", cl.TLS.Insecure).
		WithHint("development only")
	f.tlsCA = form.NewText("CA certificate", cl.TLS.CA).
		WithValidator(f.fileValidator())
	f.tlsCert = form.NewText("Client cert", cl.TLS.Cert).
		WithHint("mutual TLS").
		WithValidator(f.fileValidator())
	f.tlsKey = form.NewText("Client key", cl.TLS.CertKey).
		WithValidator(f.fileValidator())
	f.tlsPass = form.NewPassword("Key passphrase", cl.TLS.CertKeyPassphrase)

	f.saslEnabled = form.NewCheckbox("Enabled", cl.SASL.Enabled)
	f.saslMechanism = form.NewRadio("Mechanism",
		[]string{config.MechanismPlain, config.MechanismSCRAMSHA256, config.MechanismSCRAMSHA512}, 0)
	if cl.SASL.Mechanism != "" {
		f.saslMechanism.SelectValue(cl.SASL.Mechanism)
	}
	f.saslUser = form.NewText("Username", cl.SASL.Username)
	f.saslPassword = form.NewPassword("Password", cl.SASL.Password)

	f.jmxEnabled = form.NewCheckbox("Enabled", cl.JMX.Enabled)
	jmxURL := ""
	if len(cl.JMX.Targets) > 0 {
		urls := make([]string, len(cl.JMX.Targets))
		for i, t := range cl.JMX.Targets {
			urls[i] = t.URL
		}
		jmxURL = strings.Join(urls, ", ")
	}
	f.jmxURL = form.NewText("Metrics URLs", jmxURL).
		WithPlaceholder("http://broker:9404/metrics").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			for _, u := range splitList(s) {
				if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
					return errors.New("must start with http:// or https://")
				}
			}
			return nil
		})

	f.nodeEnabled = form.NewCheckbox("Enabled", cl.NodeExporter.Enabled)
	nodeURL := ""
	if len(cl.NodeExporter.Targets) > 0 {
		urls := make([]string, len(cl.NodeExporter.Targets))
		for i, t := range cl.NodeExporter.Targets {
			urls[i] = t.URL
		}
		nodeURL = strings.Join(urls, ", ")
	}
	f.nodeURL = form.NewText("Metrics URLs", nodeURL).
		WithPlaceholder("http://node:9100/metrics").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			for _, u := range splitList(s) {
				if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
					return errors.New("must start with http:// or https://")
				}
			}
			return nil
		})

	f.form = form.New([]form.Field{
		f.name, f.colour, f.brokers, f.version, f.clientID, f.timeout,
		form.NewSection("TLS"),
		f.tlsEnabled, f.tlsInsecure, f.tlsCA, f.tlsCert, f.tlsKey, f.tlsPass,
		form.NewSection("SASL"),
		f.saslEnabled, f.saslMechanism, f.saslUser, f.saslPassword,
		form.NewSection("JMX exporter"),
		f.jmxEnabled, f.jmxURL,
		form.NewSection("Node exporter"),
		f.nodeEnabled, f.nodeURL,
	}, buttonTest, buttonSave, buttonCancel)

	return f
}

// fileValidator checks that a certificate path exists and is readable, using
// the same relative-path rules as the config loader, so what the form accepts
// is exactly what will load later.
func (f *clusterForm) fileValidator() form.Validator {
	return func(s string) error {
		if strings.TrimSpace(s) == "" {
			return nil
		}
		path := s
		if strings.HasPrefix(path, "~/") {
			if home, err := os.UserHomeDir(); err == nil {
				path = filepath.Join(home, path[2:])
			}
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(f.configDir, path)
		}

		file, err := os.Open(path) //nolint:gosec // the user typed this path deliberately
		if err != nil {
			if os.IsNotExist(err) {
				return errors.New("no such file")
			}
			return errors.New("not readable")
		}
		defer func() { _ = file.Close() }()
		return nil
	}
}

func validateBrokers(s string) error {
	list := splitList(s)
	if len(list) == 0 {
		return errors.New("required")
	}
	for _, b := range list {
		if _, _, err := net.SplitHostPort(b); err != nil {
			return fmt.Errorf("%q is not host:port", b)
		}
	}
	return nil
}

func splitList(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Update forwards to the form.
func (f *clusterForm) Update(msg tea.Msg) tea.Cmd {
	f.testing = false
	return f.form.Update(msg)
}

// Cancelled reports whether the user backed out.
func (f *clusterForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

// SubmittedTest reports whether Test was pressed.
func (f *clusterForm) SubmittedTest() bool {
	if f.form.Submitted() == buttonTest {
		f.form.Reset()
		return true
	}
	return false
}

// SubmittedSave reports whether Save was pressed.
func (f *clusterForm) SubmittedSave() bool {
	if f.form.Submitted() == buttonSave {
		f.form.Reset()
		return true
	}
	return false
}

func (f *clusterForm) markTesting() { f.testing = true }

// Cluster assembles a config.Cluster from the fields, refusing when validation
// has not passed.
func (f *clusterForm) Cluster() (config.Cluster, error) {
	if !f.form.Valid() {
		errs := f.form.Errors()
		return config.Cluster{}, fmt.Errorf("fix %d field(s) first: %w", len(errs), errs[0])
	}

	timeout := config.Duration(config.DefaultRequestTimeout)
	if d, err := time.ParseDuration(f.timeout.Value()); err == nil && d > 0 {
		timeout = config.Duration(d)
	}

	cl := config.Cluster{
		Name:           strings.TrimSpace(f.name.Value()),
		Color:          strings.TrimSpace(f.colour.Value()),
		Brokers:        splitList(f.brokers.Value()),
		KafkaVersion:   strings.TrimSpace(f.version.Value()),
		ClientID:       strings.TrimSpace(f.clientID.Value()),
		RequestTimeout: timeout,
		TLS: config.TLS{
			Enabled:           f.tlsEnabled.Checked(),
			Insecure:          f.tlsInsecure.Checked(),
			CA:                strings.TrimSpace(f.tlsCA.Value()),
			Cert:              strings.TrimSpace(f.tlsCert.Value()),
			CertKey:           strings.TrimSpace(f.tlsKey.Value()),
			CertKeyPassphrase: f.tlsPass.Value(),
		},
		SASL: config.SASL{
			Enabled:   f.saslEnabled.Checked(),
			Mechanism: f.saslMechanism.Selected(),
			Username:  strings.TrimSpace(f.saslUser.Value()),
			Password:  f.saslPassword.Value(),
		},
	}

	if f.jmxEnabled.Checked() {
		urls := splitList(f.jmxURL.Value())
		cl.JMX = config.JMX{Enabled: true, Targets: make([]config.JMXTarget, len(urls))}
		for i, u := range urls {
			cl.JMX.Targets[i] = config.JMXTarget{BrokerID: int32(i + 1), URL: u}
		}
	}

	if f.nodeEnabled.Checked() {
		urls := splitList(f.nodeURL.Value())
		cl.NodeExporter = config.NodeExporter{Enabled: true, Targets: make([]config.NodeTarget, len(urls))}
		for i, u := range urls {
			cl.NodeExporter.Targets[i] = config.NodeTarget{BrokerID: int32(i + 1), URL: u}
		}
	}

	// The same rules the config file is held to, so a form that saves cannot
	// produce a file that fails to load.
	probe := config.Config{Clusters: []config.Cluster{cl}, UI: config.UI{
		Theme: config.DefaultTheme, TimestampFormat: config.DefaultTimestampFormat,
	}}
	if err := probe.Validate(); err != nil {
		return config.Cluster{}, firstError(err)
	}
	return cl, nil
}

// firstError reduces a joined error to its first line, which is what fits in a
// notification.
func firstError(err error) error {
	line, _, _ := strings.Cut(err.Error(), "\n")
	return errors.New(line)
}

// View renders the form inside a panel.
func (f *clusterForm) View(k *ktx.Context) string {
	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	title := "Add cluster"
	if f.editing {
		title = "Edit cluster: " + f.original
	}

	status := ""
	switch {
	case f.testing:
		status = th.Warning.Render("testing the connection…")
	case !f.form.Valid():
		status = th.Dim.Render(fmt.Sprintf("%d field(s) need attention", len(f.form.Errors())))
	}

	body := f.form.View(th, width-6)
	if status != "" {
		body = lipgloss.JoinVertical(lipgloss.Left, body, "", status)
	}

	return panel.Panel{Title: title, Focused: true, Width: width, Height: height}.
		Render(th, body)
}
