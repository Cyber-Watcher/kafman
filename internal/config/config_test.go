package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfig puts a config file in a temporary directory and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	if err := os.WriteFile(path, []byte(body), FileMode); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const minimalConfig = `
current-cluster: local
clusters:
  - name: local
    brokers: [localhost:9092]
`

func TestLoadCreatesDefaultWhenMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", FileName)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Clusters) != 1 || cfg.Clusters[0].Name != "local" {
		t.Errorf("default config should hold one cluster named local, got %+v", cfg.ClusterNames())
	}
	if cfg.CurrentCluster != "local" {
		t.Errorf("current-cluster = %q, want local", cfg.CurrentCluster)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the config file was not created: %v", err)
	}
	// The file holds passwords, so nobody else may read it.
	if perm := info.Mode().Perm(); perm != FileMode {
		t.Errorf("config file mode = %04o, want %04o", perm, FileMode)
	}

	// The generated file must itself be loadable.
	if _, err := Load(path); err != nil {
		t.Errorf("reloading the generated config failed: %v", err)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"ui.theme", cfg.UI.Theme, DefaultTheme},
		{"ui.timestamp-format", cfg.UI.TimestampFormat, DefaultTimestampFormat},
		{"ui.refresh-interval", cfg.UI.RefreshInterval.D(), DefaultRefreshInterval},
		{"ui.record-buffer-size", cfg.UI.RecordBufferSize, DefaultRecordBufferSize},
		{"client-id", cfg.Clusters[0].ClientID, DefaultClientID},
		{"request-timeout", cfg.Clusters[0].RequestTimeout.D(), DefaultRequestTimeout},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)

	original := New()
	original.path = path
	original.UI.Theme = ThemeLight
	original.UI.RefreshInterval = Duration(3 * time.Second)
	original.AddQuery(`key contains "abc" limit 10`)
	original.Clusters[0].SASL = SASL{
		Enabled:   true,
		Mechanism: MechanismSCRAMSHA512,
		Username:  "kafman",
		Password:  "secret",
	}
	original.Clusters[0].NodeExporter = NodeExporter{
		Enabled: true,
		Targets: []NodeTarget{{BrokerID: 1, URL: "http://localhost:9100/metrics"}},
	}

	if err := original.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if reloaded.UI.Theme != ThemeLight {
		t.Errorf("theme = %q, want %q", reloaded.UI.Theme, ThemeLight)
	}
	if got := reloaded.UI.RefreshInterval.D(); got != 3*time.Second {
		t.Errorf("refresh-interval = %v, want 3s", got)
	}
	if got := reloaded.Clusters[0].SASL; got.Mechanism != MechanismSCRAMSHA512 || got.Password != "secret" {
		t.Errorf("sasl round-trip lost data: %+v", got)
	}
	if len(reloaded.UI.QueryHistory) != 1 {
		t.Errorf("query history = %v, want one entry", reloaded.UI.QueryHistory)
	}
	if got := reloaded.Clusters[0].NodeExporter; !got.Enabled || len(got.Targets) != 1 ||
		got.Targets[0].BrokerID != 1 || got.Targets[0].URL != "http://localhost:9100/metrics" {
		t.Errorf("node-exporter round-trip lost data: %+v", got)
	}
}

func TestEmptyNodeExporterIsOmittedFromTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)

	cfg := New()
	cfg.path = path
	if err := cfg.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "node-exporter") {
		t.Errorf("an unconfigured node-exporter section leaked into the file:\n%s", raw)
	}
}

func TestDurationIsWrittenAsText(t *testing.T) {
	cfg := New()
	cfg.UI.RefreshInterval = Duration(90 * time.Second)

	data, err := cfg.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), "refresh-interval: 1m30s") {
		t.Errorf("duration should be written as text, got:\n%s", data)
	}
}

func TestDurationAcceptsBareSeconds(t *testing.T) {
	cfg, err := LoadFrom([]byte(minimalConfig+"\nui:\n  refresh-interval: 15\n"), t.TempDir())
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if got := cfg.UI.RefreshInterval.D(); got != 15*time.Second {
		t.Errorf("refresh-interval = %v, want 15s", got)
	}
}

func TestMechanismNormalisation(t *testing.T) {
	// The spellings people actually write. kafkactl rejects several of these.
	cases := map[string]string{
		"PLAIN":         MechanismPlain,
		"plaintext":     MechanismPlain,
		"SCRAM-SHA-256": MechanismSCRAMSHA256,
		"scram-sha256":  MechanismSCRAMSHA256,
		"SCRAM_SHA_512": MechanismSCRAMSHA512,
		"scram-sha512":  MechanismSCRAMSHA512,
		" scram-512 ":   MechanismSCRAMSHA512,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			body := `
clusters:
  - name: c
    brokers: [localhost:9092]
    sasl:
      enabled: true
      mechanism: "` + input + `"
      username: u
      password: p
`
			cfg, err := LoadFrom([]byte(body), t.TempDir())
			if err != nil {
				t.Fatalf("LoadFrom: %v", err)
			}
			if got := cfg.Clusters[0].SASL.Mechanism; got != want {
				t.Errorf("mechanism %q normalised to %q, want %q", input, got, want)
			}
		})
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "no clusters",
			body: "clusters: []\n",
			want: "no clusters defined",
		},
		{
			name: "empty cluster name",
			body: "clusters:\n  - name: \"\"\n    brokers: [localhost:9092]\n",
			want: "name must not be empty",
		},
		{
			name: "no brokers",
			body: "clusters:\n  - name: c\n    brokers: []\n",
			want: "at least one bootstrap broker",
		},
		{
			name: "broker without port",
			body: "clusters:\n  - name: c\n    brokers: [localhost]\n",
			want: "is not a host:port address",
		},
		{
			name: "duplicate names",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n  - name: c\n    brokers: [h:2]\n",
			want: "duplicate cluster name",
		},
		{
			name: "unknown current cluster",
			body: "current-cluster: nope\nclusters:\n  - name: c\n    brokers: [h:1]\n",
			want: `unknown cluster "nope"; available: c`,
		},
		{
			name: "unknown sasl mechanism",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    sasl:\n      enabled: true\n      mechanism: kerberos\n      username: u\n      password: p\n",
			want: "unsupported mechanism",
		},
		{
			name: "sasl without credentials",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    sasl:\n      enabled: true\n      mechanism: plain\n",
			want: "username is required",
		},
		{
			name: "bad kafka version",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    kafka-version: latest\n",
			want: "is not a Kafka version",
		},
		{
			name: "unknown theme",
			body: "ui:\n  theme: neon\nclusters:\n  - name: c\n    brokers: [h:1]\n",
			want: "unknown theme",
		},
		{
			name: "missing ca file",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n      ca: /nonexistent/ca.pem\n",
			want: "does not exist",
		},
		{
			name: "cert without key",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n      cert: /tmp/c.pem\n",
			want: "must be set together",
		},
		{
			name: "jmx enabled without targets",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    jmx:\n      enabled: true\n",
			want: "at least one target",
		},
		{
			name: "jmx target without scheme",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    jmx:\n      enabled: true\n      targets:\n        - broker-id: 1\n          url: localhost:9404\n",
			want: "must start with http",
		},
		{
			name: "node-exporter enabled without targets",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    node-exporter:\n      enabled: true\n",
			want: "at least one target is required when node-exporter is enabled",
		},
		{
			name: "node-exporter target without scheme",
			body: "clusters:\n  - name: c\n    brokers: [h:1]\n    node-exporter:\n      enabled: true\n      targets:\n        - broker-id: 1\n          url: localhost:9100\n",
			want: "must start with http",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFrom([]byte(tc.body), t.TempDir())
			if err == nil {
				t.Fatalf("expected an error containing %q, got none", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v\nwant it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidationReportsEveryProblemAtOnce(t *testing.T) {
	body := "ui:\n  theme: neon\nclusters:\n  - name: \"\"\n    brokers: []\n"

	_, err := LoadFrom([]byte(body), t.TempDir())
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"unknown theme", "name must not be empty", "at least one bootstrap broker"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got:\n%v", want, err)
		}
	}
}

func TestRelativeCertificatePathsResolveAgainstConfigDir(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "certs", "ca.crt")
	if err := os.MkdirAll(filepath.Dir(caPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, FileName)
	body := "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n      ca: certs/ca.crt\n"
	if err := os.WriteFile(path, []byte(body), FileMode); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	tls := cfg.Clusters[0].TLS
	if tls.CAFile() != caPath {
		t.Errorf("resolved CA = %q, want %q", tls.CAFile(), caPath)
	}
	// The stored value stays relative so Save writes back what the user typed.
	if tls.CA != "certs/ca.crt" {
		t.Errorf("stored CA path changed to %q", tls.CA)
	}
}

func TestClusterLookup(t *testing.T) {
	cfg, err := LoadFrom([]byte("current-cluster: b\nclusters:\n  - name: a\n    brokers: [h:1]\n  - name: b\n    brokers: [h:2]\n"), t.TempDir())
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	current, err := cfg.Current()
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if current.Name != "b" {
		t.Errorf("current = %q, want b", current.Name)
	}

	_, err = cfg.Cluster("missing")
	if err == nil || !strings.Contains(err.Error(), "available: a, b") {
		t.Errorf("lookup error should list the available clusters, got %v", err)
	}
}

func TestSwitchAddUpdateRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	t.Run("add", func(t *testing.T) {
		err := cfg.AddCluster(Cluster{Name: "staging", Brokers: []string{"stg:9092"}})
		if err != nil {
			t.Fatalf("AddCluster: %v", err)
		}
		if err := cfg.AddCluster(Cluster{Name: "staging", Brokers: []string{"stg:9092"}}); err == nil {
			t.Error("adding a duplicate should fail")
		}
		if err := cfg.AddCluster(Cluster{Name: "broken"}); err == nil {
			t.Error("adding an invalid cluster should fail")
		}
	})

	t.Run("switch", func(t *testing.T) {
		if err := cfg.SwitchCluster("staging"); err != nil {
			t.Fatalf("SwitchCluster: %v", err)
		}
		reloaded, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if reloaded.CurrentCluster != "staging" {
			t.Errorf("current-cluster was not persisted, got %q", reloaded.CurrentCluster)
		}
		if err := cfg.SwitchCluster("nope"); err == nil {
			t.Error("switching to an unknown cluster should fail")
		}
	})

	t.Run("update", func(t *testing.T) {
		err := cfg.UpdateCluster(Cluster{Name: "staging", Brokers: []string{"stg-2:9092"}, Color: "#FFAF00"})
		if err != nil {
			t.Fatalf("UpdateCluster: %v", err)
		}
		reloaded, _ := Load(path)
		updated, _ := reloaded.Cluster("staging")
		if updated.Brokers[0] != "stg-2:9092" || updated.Color != "#FFAF00" {
			t.Errorf("update was not persisted: %+v", updated)
		}
	})

	t.Run("remove reassigns current", func(t *testing.T) {
		if err := cfg.RemoveCluster("staging"); err != nil {
			t.Fatalf("RemoveCluster: %v", err)
		}
		if cfg.CurrentCluster != "local" {
			t.Errorf("current-cluster = %q, want it to fall back to local", cfg.CurrentCluster)
		}
		if err := cfg.RemoveCluster("staging"); err == nil {
			t.Error("removing a missing cluster should fail")
		}
	})
}

func TestSaveIsAtomicAndLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := New()
	cfg.path = filepath.Join(dir, FileName)

	for range 3 {
		if err := cfg.Save(); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("temporary file left behind: %s", e.Name())
		}
	}
	if len(entries) != 1 {
		t.Errorf("expected exactly one file in the config directory, got %d", len(entries))
	}
}

func TestAddQueryDeduplicatesAndCaps(t *testing.T) {
	cfg := New()

	cfg.AddQuery("a")
	cfg.AddQuery("b")
	cfg.AddQuery("a") // moves to the end rather than duplicating
	if got := cfg.UI.QueryHistory; len(got) != 2 || got[1] != "a" {
		t.Errorf("history = %v, want [b a]", got)
	}

	for i := range MaxQueryHistory + 50 {
		cfg.AddQuery(string(rune('A'+i%26)) + string(rune('0'+i/26)))
	}
	if len(cfg.UI.QueryHistory) > MaxQueryHistory {
		t.Errorf("history grew to %d entries, cap is %d", len(cfg.UI.QueryHistory), MaxQueryHistory)
	}
}

func TestSecurityProfile(t *testing.T) {
	cases := []struct {
		name string
		cl   Cluster
		want string
	}{
		{"plaintext", Cluster{}, "PLAINTEXT"},
		{"ssl", Cluster{TLS: TLS{Enabled: true}}, "SSL"},
		{"mtls", Cluster{TLS: TLS{Enabled: true, Cert: "c.pem", CertKey: "k.pem"}}, "SSL (mTLS)"},
		{"sasl", Cluster{SASL: SASL{Enabled: true, Mechanism: MechanismPlain}}, "SASL_PLAINTEXT/PLAIN"},
		{
			"sasl_ssl",
			Cluster{TLS: TLS{Enabled: true}, SASL: SASL{Enabled: true, Mechanism: MechanismSCRAMSHA512}},
			"SASL_SSL/SCRAM-SHA-512",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cl.SecurityProfile(); got != tc.want {
				t.Errorf("SecurityProfile() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLoadRejectsMalformedYAML(t *testing.T) {
	_, err := Load(writeConfig(t, "clusters: [unclosed\n"))
	if err == nil || !strings.Contains(err.Error(), "parse config") {
		t.Errorf("expected a parse error, got %v", err)
	}
}
