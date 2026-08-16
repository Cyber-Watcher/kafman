// Package help renders the F1 screen: a key reference and a diagnostics report
// in one place.
//
// Modelled on yozefu's help, which doubles as a bug report — a user can send a
// screenshot of it instead of describing their setup. Secrets are never shown.
package help

import (
	"fmt"
	"runtime"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/version"
)

// Binding is one documented key.
type Binding struct {
	Key         string
	Description string
}

// Section groups related bindings.
type Section struct {
	Title    string
	Bindings []Binding
}

// Model is the help overlay.
type Model struct {
	logPath string
	offset  int
	height  int
}

// New builds the help screen.
func New(logPath string) *Model {
	return &Model{logPath: logPath}
}

// Name implements ui.Overlay.
func (m *Model) Name() string { return "Help" }

// Update handles scrolling.
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return nil
	}
	switch key.String() {
	case "up", "k":
		m.offset = max(m.offset-1, 0)
	case "down", "j":
		m.offset++
	case "pgup":
		m.offset = max(m.offset-m.height, 0)
	case "pgdown":
		m.offset += m.height
	case "home", "g":
		m.offset = 0
	}
	return nil
}

// View renders the help text.
func (m *Model) View(k *ktx.Context) string {
	th := k.Theme
	m.height = k.AvailableHeight

	lines := make([]string, 0, 80)
	add := func(s ...string) { lines = append(lines, s...) }

	add(th.Title.Render("kafman — keys and diagnostics"), "")

	for _, section := range GlobalSections() {
		add(th.Highlight.Render(section.Title))
		for _, b := range section.Bindings {
			add(fmt.Sprintf("  %s  %s",
				th.ShortcutKey.Render(pad(b.Key, 16)),
				th.Value.Render(b.Description)))
		}
		add("")
	}

	// The query language is only discoverable from here, so it gets a full
	// reference rather than a pointer to documentation the user does not have.
	add(th.Highlight.Render("Query language"))
	add(th.Dim.Render("  Clauses may appear in any order."))
	add("")
	for _, row := range queryReference() {
		add(fmt.Sprintf("  %s  %s",
			th.ShortcutKey.Render(pad(row[0], 30)),
			th.Value.Render(row[1])))
	}
	add("")
	add(th.Dim.Render("  Examples"))
	for _, example := range queryExamples() {
		add("  " + th.Success.Render(example))
	}
	add("")

	add(th.Highlight.Render("Configuration"))
	for _, row := range m.diagnostics(k.Config) {
		add(fmt.Sprintf("  %s  %s",
			th.Label.Render(pad(row[0], 16)),
			th.Value.Render(row[1])))
	}
	add("")

	add(th.Highlight.Render("Clusters"))
	for i := range k.Config.Clusters {
		cl := &k.Config.Clusters[i]
		marker := "  "
		if cl.Name == k.Config.CurrentCluster {
			marker = th.Success.Render(" " + th.Glyph("●", "*"))
		}
		// Credentials are deliberately absent: this screen gets screenshotted.
		add(fmt.Sprintf("  %s %s  %s  %s",
			marker,
			th.Value.Render(pad(cl.Name, 22)),
			th.Dim.Render(pad(cl.SecurityProfile(), 26)),
			th.Dim.Render(strings.Join(cl.Brokers, ", "))))
	}
	add("")

	// The effective settings of the cluster in use, which is what a question
	// about behaviour usually comes down to. Passwords and key passphrases are
	// never here — this screen gets screenshotted into tickets.
	if current, err := k.Config.Current(); err == nil {
		add(th.Highlight.Render("Current cluster · " + current.Name))
		for _, row := range clusterSettings(current) {
			add(fmt.Sprintf("  %s  %s",
				th.Label.Render(pad(row[0], 16)),
				th.Value.Render(row[1])))
		}
		add("")
	}

	add(th.Highlight.Render("Version"))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("kafman", 16)), th.Value.Render(version.Version)))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("commit", 16)), th.Value.Render(version.Commit)))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("built", 16)), th.Value.Render(version.Date)))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("go", 16)), th.Value.Render(runtime.Version())))
	add("")

	add(th.Highlight.Render("About"))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("author", 16)), th.Value.Render("Cyberwatcher")))
	add(fmt.Sprintf("  %s  %s", th.Label.Render(pad("repository", 16)),
		th.Value.Render("https://github.com/Cyber-Watcher/kafman")))

	// Scroll.
	if m.offset > max(len(lines)-m.height, 0) {
		m.offset = max(len(lines)-m.height, 0)
	}
	visible := lines[min(m.offset, len(lines)):]
	if len(visible) > m.height {
		visible = visible[:m.height]
	}
	return lipgloss.JoinVertical(lipgloss.Left, visible...)
}

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	return []chrome.Shortcut{
		{Key: "↑/↓", Label: "Scroll"},
		{Key: "Esc", Label: "Close"},
	}
}

func (m *Model) diagnostics(cfg *config.Config) [][2]string {
	rows := [][2]string{
		{"config file", cfg.Path()},
		{"log file", m.logPath},
		{"theme", cfg.UI.Theme},
		{"plain fonts", yesNo(cfg.UI.PlainFonts)},
		{"refresh", cfg.UI.RefreshInterval.String()},
		{"record buffer", fmt.Sprint(cfg.UI.RecordBufferSize)},
		{"timestamps", cfg.UI.TimestampFormat},
	}
	if cfg.UI.ExportDir != "" {
		rows = append(rows, [2]string{"exports", cfg.UI.ExportDir})
	}
	return rows
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

// clusterSettings lists what the current cluster resolves to, with every secret
// left out: this screen is meant to be pasted into a bug report.
func clusterSettings(cl *config.Cluster) [][2]string {
	rows := [][2]string{
		{"brokers", strings.Join(cl.Brokers, ", ")},
		{"security", cl.SecurityProfile()},
		{"kafka version", orDash(cl.KafkaVersion)},
		{"client id", orDash(cl.ClientID)},
		{"request timeout", cl.RequestTimeout.String()},
	}

	if cl.TLS.Enabled {
		rows = append(rows,
			[2]string{"tls ca", orDash(cl.TLS.CAFile())},
			[2]string{"tls cert", orDash(cl.TLS.CertFile())})
		if cl.TLS.Insecure {
			rows = append(rows, [2]string{"tls verify", "DISABLED — certificates are not checked"})
		}
		if cl.TLS.CertKeyPassphrase != "" {
			rows = append(rows, [2]string{"tls key", "encrypted, passphrase in the config"})
		}
	}
	if cl.SASL.Enabled {
		rows = append(rows,
			[2]string{"sasl mechanism", cl.SASL.Mechanism},
			[2]string{"sasl username", orDash(cl.SASL.Username)},
			[2]string{"sasl password", "(hidden)"})
	}
	if cl.JMX.Enabled {
		urls := make([]string, 0, len(cl.JMX.Targets))
		for _, target := range cl.JMX.Targets {
			urls = append(urls, fmt.Sprintf("%d:%s", target.BrokerID, target.URL))
		}
		rows = append(rows, [2]string{"jmx targets", strings.Join(urls, "  ")})
	}
	if cl.NodeExporter.Enabled {
		urls := make([]string, 0, len(cl.NodeExporter.Targets))
		for _, target := range cl.NodeExporter.Targets {
			urls = append(urls, fmt.Sprintf("%d:%s", target.BrokerID, target.URL))
		}
		rows = append(rows, [2]string{"node-exporter targets", strings.Join(urls, "  ")})
	}
	return rows
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// GlobalSections returns the key reference. It lives here so the help screen and
// docs/project_layout.md have a single source to stay in step with.
func GlobalSections() []Section {
	return []Section{
		{
			Title: "Global",
			Bindings: []Binding{
				{"Ctrl+←/→", "previous / next tab"},
				{"Ctrl+H / Ctrl+L", "previous / next tab"},
				{"1 … 5", "jump straight to a tab"},
				{"Tab / Shift+Tab", "move focus between panels"},
				{"Ctrl+K", "switch cluster"},
				{"F1", "this screen"},
				{"F5", "refresh the current tab"},
				{"Esc", "close the overlay on top"},
				{"q / Ctrl+C", "quit"},
			},
		},
		{
			Title: "Tables",
			Bindings: []Binding{
				{"↑/↓, k/j", "move the selection"},
				{"PgUp/PgDn", "move a page"},
				{"Home/End, g/G", "first / last row"},
				{"/", "filter"},
				{"F3", "cycle the sort column"},
				{"Enter", "open the selected row"},
			},
		},
		{
			Title: "Overview",
			Bindings: []Binding{
				{"Tab", "topics / consumer groups"},
				{"Enter", "open the topic in Messages, the group in Manage"},
				{"F5", "reload every panel"},
			},
		},
		{
			Title: "Messages",
			Bindings: []Binding{
				{"Space", "select a topic to read"},
				{"Ctrl+U", "clear the selection"},
				{"Ctrl+O", "show or hide the topics panel"},
				{"F4", "show or hide internal topics"},
				{"Enter", "open the record; ↑/↓ then page through them"},
				{"Tab", "in the open record: scroll the value with ↑/↓"},
				{"s", "cycle the start position (or write from … in a query)"},
				{"f", "follow the newest records"},
				{"t", "cycle the timestamp format"},
				{"r", "reverse the display order"},
				{"←/→", "resize the TOPIC and KEY columns"},
				{"P", "publish a record"},
				{"E / Ctrl+E", "export the selected record / the whole buffer"},
				{"C", "copy the record to the clipboard"},
				{"Ctrl+C", "stop a running read (in the query line)"},
			},
		},
		{
			Title: "Manage",
			Bindings: []Binding{
				{"Tab", "section list / content"},
				{"Ctrl+N", "create — topic, user or ACL, by section"},
				{"Ctrl+O", "the selected topic's configuration"},
				{"Ctrl+P", "add partitions to a topic"},
				{"Ctrl+T", "delete every record in a topic"},
				{"Ctrl+E", "change a SCRAM user's password"},
				{"Ctrl+R", "reset a consumer group's offsets"},
				{"Ctrl+D", "clear a group's offsets for one topic"},
				{"→/←", "groups list / its partition offsets"},
				{"F2", "delete, always with a confirmation"},
			},
		},
		{
			Title: "Monitoring",
			Bindings: []Binding{
				{"Tab", "cycle focus: the panels and the section list"},
				{"↑ / ↓", "in the section list: Overview / Groups / JMX / OS"},
				{"Enter", "Overview: topic → Messages, group → Manage; Groups: group → Manage"},
				{"t", "chart mode: topics / retention · by group · bytes in · CPU / net / disk"},
				{"+ / -", "time window: 1m, 5m, 15m, 1h"},
				{"F5", "poll now"},
			},
		},
		{
			Title: "Clusters",
			Bindings: []Binding{
				{"Enter", "connect to the selected cluster"},
				{"t", "test the connection without switching"},
				{"Ctrl+N", "add a cluster"},
				{"Ctrl+E", "edit the selected cluster"},
				{"F2", "delete it"},
			},
		},
	}
}

// queryReference documents the language. It lives beside the parser's tests,
// which check that every example here actually parses.
func queryReference() [][2]string {
	return [][2]string{
		{"topic, t", "the topic name"},
		{"partition, p", "the partition number"},
		{"offset, o", "the record offset"},
		{"key, k", "the record key"},
		{"value, v", "the record value"},
		{"value.a.b[0]", "a field inside a JSON value"},
		{"timestamp, ts", "the record timestamp"},
		{"size, si", "the record size in bytes"},
		{"headers.name, h.name", "a header by name"},
		{"", ""},
		{"== != > >= < <=", "comparison"},
		{"contains, ~=", "substring, case-insensitive"},
		{"starts with", "prefix, case-insensitive"},
		{"and or ! ( )", "combine conditions; && and || also work"},
		{"", ""},
		{"from beginning", "read everything retained"},
		{"from end", "read only what arrives from now on"},
		{"from end - 500", "the last 500 records of each partition"},
		{"from 34895", "start at an offset"},
		{`from "2 hours ago"`, "start at a time, absolute or relative"},
		{"limit 100", "stop after 100 matching records"},
		{"order by offset desc", "sort what is on screen"},
		{`between "..." and "..."`, "a timestamp range"},
	}
}

func queryExamples() []string {
	return []string{
		`key contains "order-" from end - 500 limit 100`,
		`partition == 2 and value.status == "FAILED"`,
		`timestamp >= "2 hours ago" order by offset desc`,
		`(key == "ABC") || (key contains "XYZ") from beginning`,
		`size > 1_000_000 limit 20`,
	}
}

// pad aligns by display width, not by byte length: the key column carries
// arrows and box characters, and len() would leave those rows short.
func pad(s string, width int) string {
	if w := lipgloss.Width(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}
