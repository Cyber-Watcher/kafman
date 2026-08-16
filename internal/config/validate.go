package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
)

// kafkaVersionRe matches the versions the Kafka client accepts: 3.9.0 or 0.11.0.2.
// The value is not parsed here — internal/kafka does that — but an obvious typo
// should be caught while the user is still looking at their config file.
var kafkaVersionRe = regexp.MustCompile(`^v?\d+\.\d+\.\d+(\.\d+)?$`)

// Validate checks the whole configuration and reports every problem at once,
// rather than stopping at the first, so one edit can fix them all.
func (c *Config) Validate() error {
	var errs []error

	if len(c.Clusters) == 0 {
		errs = append(errs, errors.New("no clusters defined: add at least one entry under the clusters key"))
	}

	switch c.UI.Theme {
	case ThemeDark, ThemeLight:
	default:
		errs = append(errs, fmt.Errorf("ui.theme: unknown theme %q; want %s or %s",
			c.UI.Theme, ThemeDark, ThemeLight))
	}

	switch c.UI.TimestampFormat {
	case TimestampDateTime, TimestampAgo:
	default:
		errs = append(errs, fmt.Errorf("ui.timestamp-format: unknown format %q; want %s or %s",
			c.UI.TimestampFormat, TimestampDateTime, TimestampAgo))
	}

	seen := make(map[string]int, len(c.Clusters))
	for i := range c.Clusters {
		cl := &c.Clusters[i]
		if first, dup := seen[cl.Name]; dup && cl.Name != "" {
			errs = append(errs, fmt.Errorf("clusters[%d].name: duplicate cluster name %q, already used by clusters[%d]",
				i, cl.Name, first))
		} else {
			seen[cl.Name] = i
		}
		if err := cl.validate(c.Dir(), i); err != nil {
			errs = append(errs, err)
		}
	}

	if c.CurrentCluster != "" && len(c.Clusters) > 0 {
		if _, ok := seen[c.CurrentCluster]; !ok {
			errs = append(errs, fmt.Errorf("current-cluster: unknown cluster %q; available: %s",
				c.CurrentCluster, strings.Join(c.ClusterNames(), ", ")))
		}
	}

	return errors.Join(errs...)
}

// validate checks one cluster. index is used to build readable error paths.
func (cl *Cluster) validate(baseDir string, index int) error {
	var errs []error
	at := func(field string) string { return fmt.Sprintf("clusters[%d].%s", index, field) }

	if strings.TrimSpace(cl.Name) == "" {
		errs = append(errs, fmt.Errorf("%s: cluster name must not be empty", at("name")))
	}

	if len(cl.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("%s: at least one bootstrap broker is required", at("brokers")))
	}
	for j, broker := range cl.Brokers {
		if err := validateBroker(broker); err != nil {
			errs = append(errs, fmt.Errorf("%s[%d]: %w", at("brokers"), j, err))
		}
	}

	if cl.KafkaVersion != "" && !kafkaVersionRe.MatchString(cl.KafkaVersion) {
		errs = append(errs, fmt.Errorf("%s: %q is not a Kafka version; want something like 3.9.0",
			at("kafka-version"), cl.KafkaVersion))
	}

	if cl.RequestTimeout < 0 {
		errs = append(errs, fmt.Errorf("%s: must not be negative", at("request-timeout")))
	}

	errs = append(errs, cl.SASL.validate(at)...)
	errs = append(errs, cl.TLS.validate(at, baseDir)...)
	errs = append(errs, cl.JMX.validate(at)...)
	errs = append(errs, cl.NodeExporter.validate(at)...)

	if cl.SchemaRegistry != nil && strings.TrimSpace(cl.SchemaRegistry.URL) == "" {
		errs = append(errs, fmt.Errorf("%s: url must not be empty when schema-registry is present",
			at("schema-registry.url")))
	}

	return errors.Join(errs...)
}

func (s SASL) validate(at func(string) string) []error {
	if !s.Enabled {
		return nil
	}
	var errs []error

	switch s.Mechanism {
	case MechanismPlain, MechanismSCRAMSHA256, MechanismSCRAMSHA512:
	default:
		errs = append(errs, fmt.Errorf("%s: unsupported mechanism %q; want %s, %s or %s",
			at("sasl.mechanism"), s.Mechanism,
			MechanismPlain, MechanismSCRAMSHA256, MechanismSCRAMSHA512))
	}

	if s.Username == "" {
		errs = append(errs, fmt.Errorf("%s: username is required when SASL is enabled", at("sasl.username")))
	}
	if s.Password == "" {
		errs = append(errs, fmt.Errorf("%s: password is required when SASL is enabled", at("sasl.password")))
	}
	return errs
}

func (t TLS) validate(at func(string) string, baseDir string) []error {
	if !t.Enabled {
		return nil
	}
	var errs []error

	// A client certificate without its key (or the other way round) is a
	// half-configured mTLS setup and will fail at connect time with a much less
	// obvious message.
	if (t.Cert == "") != (t.CertKey == "") {
		errs = append(errs, fmt.Errorf("%s: cert and cert-key must be set together for mutual TLS",
			at("tls")))
	}

	for field, path := range map[string]string{
		"tls.ca":       t.CA,
		"tls.cert":     t.Cert,
		"tls.cert-key": t.CertKey,
	} {
		if path == "" {
			continue
		}
		if err := checkReadable(resolveFile(path, baseDir)); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", at(field), err))
		}
	}

	if t.Insecure && t.CA != "" {
		errs = append(errs, fmt.Errorf("%s: insecure disables verification, so the ca certificate is never used; set one or the other",
			at("tls.insecure")))
	}
	return errs
}

func (j JMX) validate(at func(string) string) []error {
	if !j.Enabled {
		return nil
	}
	var errs []error

	if len(j.Targets) == 0 {
		errs = append(errs, fmt.Errorf("%s: at least one target is required when jmx is enabled", at("jmx.targets")))
	}
	for i, t := range j.Targets {
		field := fmt.Sprintf("jmx.targets[%d].url", i)
		switch {
		case strings.TrimSpace(t.URL) == "":
			errs = append(errs, fmt.Errorf("%s: url must not be empty", at(field)))
		case !strings.HasPrefix(t.URL, "http://") && !strings.HasPrefix(t.URL, "https://"):
			errs = append(errs, fmt.Errorf("%s: %q must start with http:// or https://", at(field), t.URL))
		}
	}
	return errs
}

func (n NodeExporter) validate(at func(string) string) []error {
	if !n.Enabled {
		return nil
	}
	var errs []error

	if len(n.Targets) == 0 {
		errs = append(errs, fmt.Errorf("%s: at least one target is required when node-exporter is enabled",
			at("node-exporter.targets")))
	}
	for i, t := range n.Targets {
		field := fmt.Sprintf("node-exporter.targets[%d].url", i)
		switch {
		case strings.TrimSpace(t.URL) == "":
			errs = append(errs, fmt.Errorf("%s: url must not be empty", at(field)))
		case !strings.HasPrefix(t.URL, "http://") && !strings.HasPrefix(t.URL, "https://"):
			errs = append(errs, fmt.Errorf("%s: %q must start with http:// or https://", at(field), t.URL))
		}
	}
	return errs
}

func validateBroker(broker string) error {
	broker = strings.TrimSpace(broker)
	if broker == "" {
		return errors.New("broker address must not be empty")
	}
	host, port, err := net.SplitHostPort(broker)
	if err != nil {
		return fmt.Errorf("%q is not a host:port address (for example localhost:9092)", broker)
	}
	if host == "" {
		return fmt.Errorf("%q has no host part", broker)
	}
	if port == "" {
		return fmt.Errorf("%q has no port", broker)
	}
	return nil
}

// checkReadable verifies a file exists and can actually be opened. Checking
// os.Stat alone would miss permission problems, which is the common case for
// private keys.
func checkReadable(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", path)
		}
		if os.IsPermission(err) {
			return fmt.Errorf("file %s is not readable", path)
		}
		return err
	}
	return f.Close()
}
