package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// BuildSaramaConfig translates a cluster definition into a sarama configuration.
//
// It performs no I/O beyond reading the certificate files named in the config,
// so it is safe to call while validating a cluster the user is still editing.
func BuildSaramaConfig(c *config.Cluster) (*sarama.Config, error) {
	cfg := sarama.NewConfig()

	version, err := parseKafkaVersion(c.KafkaVersion)
	if err != nil {
		return nil, err
	}
	cfg.Version = version

	cfg.ClientID = c.ClientID
	if cfg.ClientID == "" {
		cfg.ClientID = config.DefaultClientID
	}

	timeout := c.RequestTimeout.D()
	if timeout <= 0 {
		timeout = config.DefaultRequestTimeout
	}
	cfg.Net.DialTimeout = timeout
	cfg.Net.ReadTimeout = timeout
	cfg.Net.WriteTimeout = timeout
	cfg.Admin.Timeout = timeout

	// An interactive tool must fail fast and say so, rather than retrying
	// silently behind a spinner. sarama's defaults (3 retries, 250ms apart)
	// would turn one unreachable broker into seconds of unexplained waiting.
	cfg.Metadata.Retry.Max = 1
	cfg.Metadata.Retry.Backoff = 250 * time.Millisecond

	// Metadata.Full must stay true: with it off, sarama refuses the initial
	// refresh that NewClient performs ("no specific topics to update metadata"),
	// because it then expects an explicit topic list on every call.
	cfg.Metadata.Full = true
	cfg.Metadata.RefreshFrequency = 5 * time.Minute

	// Producer defaults. Waiting for all in-sync replicas is the right trade-off
	// for a tool a human drives: correctness over throughput.
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Timeout = timeout

	// The partitioner has to be set here, on the config the client is built
	// from: NewSyncProducerFromClient reads the client's own configuration and
	// ignores any copy made later. Getting this wrong means records routed by
	// FNV-1a instead of murmur2, landing in different partitions than a Java
	// producer would choose.
	cfg.Producer.Partitioner = newKafmanPartitioner

	// Consumers here only ever read; offsets are never committed, and the
	// Messages tab assigns partitions directly instead of joining a group, so
	// kafman can never disturb a production consumer.
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	if err := applyTLS(cfg, c); err != nil {
		return nil, err
	}
	if err := applySASL(cfg, c); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid client configuration for cluster %q: %w", c.Name, err)
	}
	return cfg, nil
}

func applyTLS(cfg *sarama.Config, c *config.Cluster) error {
	if !c.TLS.Enabled {
		return nil
	}
	tlsConfig, err := buildTLSConfig(c)
	if err != nil {
		return err
	}
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsConfig
	return nil
}

func applySASL(cfg *sarama.Config, c *config.Cluster) error {
	if !c.SASL.Enabled {
		return nil
	}

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = true
	cfg.Net.SASL.User = c.SASL.Username
	cfg.Net.SASL.Password = c.SASL.Password

	switch c.SASL.Mechanism {
	case config.MechanismPlain:
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	case config.MechanismSCRAMSHA256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{HashGeneratorFcn: sha256Gen}
		}

	case config.MechanismSCRAMSHA512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{HashGeneratorFcn: sha512Gen}
		}

	default:
		// config.Validate rejects unknown mechanisms, so reaching this means the
		// two lists have drifted apart.
		return fmt.Errorf("unsupported SASL mechanism %q for cluster %q", c.SASL.Mechanism, c.Name)
	}

	return nil
}

// parseKafkaVersion turns the configured version string into a sarama version.
// An empty value means "use the client default".
func parseKafkaVersion(s string) (sarama.KafkaVersion, error) {
	if s == "" {
		return sarama.DefaultVersion, nil
	}
	// "v3.9.0" is a natural thing to write; sarama does not accept the prefix.
	version, err := sarama.ParseKafkaVersion(trimVersionPrefix(s))
	if err != nil {
		return sarama.KafkaVersion{}, fmt.Errorf("unsupported kafka-version %q: %w", s, err)
	}
	return version, nil
}

func trimVersionPrefix(s string) string {
	if len(s) > 0 && (s[0] == 'v' || s[0] == 'V') {
		return s[1:]
	}
	return s
}
