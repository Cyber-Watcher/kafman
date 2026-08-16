package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Config is the whole kafman configuration file.
//
// Field order determines the order of keys when the file is written, so the
// layout of a saved file stays close to the documented example.
type Config struct {
	CurrentCluster string    `yaml:"current-cluster"`
	UI             UI        `yaml:"ui"`
	Clusters       []Cluster `yaml:"clusters"`

	// path is where this config was read from, and where Save writes it back.
	// Not serialised.
	path string `yaml:"-"`
}

// UI holds presentation settings shared by every cluster.
type UI struct {
	Theme            string   `yaml:"theme"`
	PlainFonts       bool     `yaml:"plain-fonts"`
	TimestampFormat  string   `yaml:"timestamp-format"`
	RefreshInterval  Duration `yaml:"refresh-interval"`
	RecordBufferSize int      `yaml:"record-buffer-size"`

	// ExportDir is where record exports are written. Empty means the working
	// directory.
	ExportDir string `yaml:"export-dir,omitempty"`

	// QueryHistory is appended to by the Messages tab; omitted while empty so a
	// fresh config file stays tidy.
	QueryHistory []string `yaml:"query-history,omitempty"`
}

// Cluster is one Kafka cluster kafman can connect to.
type Cluster struct {
	Name string `yaml:"name"`

	// Color tints the cluster chip in the header — the "am I on production?" cue.
	Color string `yaml:"color,omitempty"`

	Brokers []string `yaml:"brokers"`

	// KafkaVersion is the protocol version to negotiate, e.g. "3.9.0". Empty
	// means the client default. Kept as a string so this package does not depend
	// on the Kafka client library; internal/kafka parses it.
	KafkaVersion string `yaml:"kafka-version,omitempty"`

	ClientID       string   `yaml:"client-id,omitempty"`
	RequestTimeout Duration `yaml:"request-timeout,omitempty"`

	TLS            TLS             `yaml:"tls,omitempty"`
	SASL           SASL            `yaml:"sasl,omitempty"`
	SchemaRegistry *SchemaRegistry `yaml:"schema-registry,omitempty"`
	JMX            JMX             `yaml:"jmx,omitempty"`
	NodeExporter   NodeExporter    `yaml:"node-exporter,omitempty"`
}

// TLS configures the transport security of a cluster connection.
type TLS struct {
	Enabled bool `yaml:"enabled"`

	// CA is the certificate authority to trust in addition to the system pool.
	// Relative paths are resolved against the directory holding the config file.
	CA string `yaml:"ca,omitempty"`

	// Cert and CertKey enable mutual TLS.
	Cert    string `yaml:"cert,omitempty"`
	CertKey string `yaml:"cert-key,omitempty"`

	// CertKeyPassphrase decrypts an encrypted PKCS#8 private key.
	CertKeyPassphrase string `yaml:"cert-key-passphrase,omitempty"`

	// Insecure disables certificate verification. Development only.
	Insecure bool `yaml:"insecure,omitempty"`

	// Absolute paths computed at load time; not serialised.
	caFile   string `yaml:"-"`
	certFile string `yaml:"-"`
	keyFile  string `yaml:"-"`
}

// CAFile returns the absolute path to the CA certificate, or "" if unset.
func (t TLS) CAFile() string { return t.caFile }

// CertFile returns the absolute path to the client certificate, or "" if unset.
func (t TLS) CertFile() string { return t.certFile }

// KeyFile returns the absolute path to the client key, or "" if unset.
func (t TLS) KeyFile() string { return t.keyFile }

// SASL configures authentication.
type SASL struct {
	Enabled bool `yaml:"enabled"`

	// Mechanism is one of the Mechanism* constants. Values are normalised on
	// load, so "SCRAM-SHA-512", "scram-sha512" and "scram_sha_512" all work.
	Mechanism string `yaml:"mechanism,omitempty"`

	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

// Supported SASL mechanisms, in their canonical form.
const (
	MechanismPlain       = "plain"
	MechanismSCRAMSHA256 = "scram-sha-256"
	MechanismSCRAMSHA512 = "scram-sha-512"
)

// SchemaRegistry is reserved: kafman does not use it yet, but the key is
// accepted so configs written today keep working when support lands.
type SchemaRegistry struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

// JMX points at Prometheus endpoints exposed by a jmx_exporter agent on the
// brokers. Optional: without it kafman falls back to Kafka API metrics only.
type JMX struct {
	Enabled bool        `yaml:"enabled"`
	Targets []JMXTarget `yaml:"targets,omitempty"`
}

// JMXTarget maps a broker to its metrics endpoint.
type JMXTarget struct {
	BrokerID int32  `yaml:"broker-id"`
	URL      string `yaml:"url"`
}

// NodeExporter points at prometheus/node_exporter endpoints on the Kafka
// nodes. Optional: without it the Monitoring tab's OS section reports that it
// is not configured.
type NodeExporter struct {
	Enabled bool         `yaml:"enabled"`
	Targets []NodeTarget `yaml:"targets,omitempty"`
}

// NodeTarget maps a broker to the node_exporter on its host.
type NodeTarget struct {
	BrokerID int32  `yaml:"broker-id"`
	URL      string `yaml:"url"`
}

// Path returns the file this config was loaded from.
func (c *Config) Path() string { return c.path }

// Dir returns the directory holding the config file. Relative certificate paths
// are resolved against it.
func (c *Config) Dir() string { return filepath.Dir(c.path) }

// ClusterNames lists cluster names in file order.
func (c *Config) ClusterNames() []string {
	names := make([]string, len(c.Clusters))
	for i := range c.Clusters {
		names[i] = c.Clusters[i].Name
	}
	return names
}

// Cluster returns the named cluster, or an error naming the available ones.
func (c *Config) Cluster(name string) (*Cluster, error) {
	for i := range c.Clusters {
		if c.Clusters[i].Name == name {
			return &c.Clusters[i], nil
		}
	}
	return nil, fmt.Errorf("unknown cluster %q; available: %s",
		name, strings.Join(c.ClusterNames(), ", "))
}

// Current returns the cluster named by current-cluster, falling back to the
// first one when it is unset.
func (c *Config) Current() (*Cluster, error) {
	if len(c.Clusters) == 0 {
		return nil, fmt.Errorf("no clusters defined in %s", c.path)
	}
	if c.CurrentCluster == "" {
		return &c.Clusters[0], nil
	}
	return c.Cluster(c.CurrentCluster)
}

// SwitchCluster makes the named cluster current and persists the change.
func (c *Config) SwitchCluster(name string) error {
	if _, err := c.Cluster(name); err != nil {
		return err
	}
	c.CurrentCluster = name
	return c.Save()
}

// AddCluster appends a cluster and persists it.
func (c *Config) AddCluster(cluster Cluster) error {
	if _, err := c.Cluster(cluster.Name); err == nil {
		return fmt.Errorf("cluster %q already exists", cluster.Name)
	}
	if err := cluster.validate(c.Dir(), 0); err != nil {
		return err
	}
	cluster.resolvePaths(c.Dir())
	c.Clusters = append(c.Clusters, cluster)
	return c.Save()
}

// UpdateCluster replaces the cluster with the same name and persists the change.
func (c *Config) UpdateCluster(cluster Cluster) error {
	idx := slices.IndexFunc(c.Clusters, func(x Cluster) bool { return x.Name == cluster.Name })
	if idx < 0 {
		return fmt.Errorf("unknown cluster %q; available: %s",
			cluster.Name, strings.Join(c.ClusterNames(), ", "))
	}
	if err := cluster.validate(c.Dir(), idx); err != nil {
		return err
	}
	cluster.resolvePaths(c.Dir())
	c.Clusters[idx] = cluster
	return c.Save()
}

// RemoveCluster deletes a cluster and persists the change. If it was the
// current one, the first remaining cluster takes over.
func (c *Config) RemoveCluster(name string) error {
	idx := slices.IndexFunc(c.Clusters, func(x Cluster) bool { return x.Name == name })
	if idx < 0 {
		return fmt.Errorf("unknown cluster %q; available: %s",
			name, strings.Join(c.ClusterNames(), ", "))
	}
	c.Clusters = slices.Delete(c.Clusters, idx, idx+1)
	if c.CurrentCluster == name {
		c.CurrentCluster = ""
		if len(c.Clusters) > 0 {
			c.CurrentCluster = c.Clusters[0].Name
		}
	}
	return c.Save()
}

// SecurityProfile describes how a cluster authenticates, for display in the
// Clusters tab. It never includes credentials.
func (cl *Cluster) SecurityProfile() string {
	switch {
	case cl.TLS.Enabled && cl.SASL.Enabled:
		return "SASL_SSL/" + strings.ToUpper(cl.SASL.Mechanism)
	case cl.TLS.Enabled && cl.TLS.Cert != "":
		return "SSL (mTLS)"
	case cl.TLS.Enabled:
		return "SSL"
	case cl.SASL.Enabled:
		return "SASL_PLAINTEXT/" + strings.ToUpper(cl.SASL.Mechanism)
	default:
		return "PLAINTEXT"
	}
}

// resolvePaths turns relative certificate paths into absolute ones, expanding a
// leading ~. The stored values are left untouched so Save writes back what the
// user originally typed.
func (cl *Cluster) resolvePaths(baseDir string) {
	cl.TLS.caFile = resolveFile(cl.TLS.CA, baseDir)
	cl.TLS.certFile = resolveFile(cl.TLS.Cert, baseDir)
	cl.TLS.keyFile = resolveFile(cl.TLS.CertKey, baseDir)
}

func resolveFile(path, baseDir string) string {
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(baseDir, path)
}

// NormaliseMechanism exposes the same normalisation to callers outside the
// package — the SCRAM user form, which must read a mechanism typed by hand the
// way the configuration reads one written by hand.
func NormaliseMechanism(s string) string { return normaliseMechanism(s) }

// normaliseMechanism maps the spellings people actually write to the canonical
// form. kafkactl compares the raw string and rejects "SCRAM-SHA-256" as unknown;
// see docs/project_kb.md.
func normaliseMechanism(s string) string {
	n := strings.ToLower(strings.TrimSpace(s))
	n = strings.ReplaceAll(n, "_", "-")
	switch n {
	case "plain", "plaintext", "sasl-plain", "sasl-plaintext":
		return MechanismPlain
	case "scram-sha-256", "scram-sha256", "sha-256", "sha256", "scram-256":
		return MechanismSCRAMSHA256
	case "scram-sha-512", "scram-sha512", "sha-512", "sha512", "scram-512":
		return MechanismSCRAMSHA512
	default:
		return n
	}
}
