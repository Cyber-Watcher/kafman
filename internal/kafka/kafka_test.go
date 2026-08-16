package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/youmark/pkcs8"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// --------------------------------------------------------------- helpers ---

// writeCertificate generates a self-signed certificate and writes the cert and
// key to dir. When passphrase is non-empty the key is written as encrypted
// PKCS#8.
func writeCertificate(t *testing.T, dir, name, passphrase string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPath = filepath.Join(dir, name+".crt")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	keyPath = filepath.Join(dir, name+".key")
	var keyPEM []byte
	if passphrase == "" {
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	} else {
		keyDER, err := pkcs8.MarshalPrivateKey(key, []byte(passphrase), nil)
		if err != nil {
			t.Fatalf("encrypt key: %v", err)
		}
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: keyDER})
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// loadCluster builds a cluster through the config package, so certificate paths
// go through the same resolution the application uses.
func loadCluster(t *testing.T, dir, body string) *config.Cluster {
	t.Helper()
	cfg, err := config.LoadFrom([]byte(body), dir)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cl, err := cfg.Current()
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// ------------------------------------------------------ sarama config ---

func TestBuildSaramaConfigDefaults(t *testing.T) {
	cl := loadCluster(t, t.TempDir(), "clusters:\n  - name: c\n    brokers: [localhost:9092]\n")

	conf, err := BuildSaramaConfig(cl)
	if err != nil {
		t.Fatalf("BuildSaramaConfig: %v", err)
	}

	if conf.ClientID != config.DefaultClientID {
		t.Errorf("ClientID = %q, want %q", conf.ClientID, config.DefaultClientID)
	}
	if conf.Net.DialTimeout != config.DefaultRequestTimeout {
		t.Errorf("DialTimeout = %v, want %v", conf.Net.DialTimeout, config.DefaultRequestTimeout)
	}
	if conf.Net.TLS.Enable {
		t.Error("TLS should be off by default")
	}
	if conf.Net.SASL.Enable {
		t.Error("SASL should be off by default")
	}
	// kafman must never commit offsets on someone else's cluster.
	if conf.Consumer.Offsets.AutoCommit.Enable {
		t.Error("offset auto-commit must be disabled")
	}
	// Metadata.Full off makes NewClient fail with "no specific topics to update
	// metadata"; this guards against re-introducing that.
	if !conf.Metadata.Full {
		t.Error("Metadata.Full must stay true")
	}
}

func TestBuildSaramaConfigKafkaVersion(t *testing.T) {
	cases := []struct {
		version string
		want    sarama.KafkaVersion
		wantErr bool
	}{
		{version: "", want: sarama.DefaultVersion},
		{version: "3.9.0", want: sarama.V3_9_0_0},
		{version: "v3.9.0", want: sarama.V3_9_0_0},
		{version: "2.8.0", want: sarama.V2_8_0_0},
		// sarama accepts any well-formed triple, including versions that do not
		// exist yet, so only the shape can be rejected here. config.Validate
		// catches typos earlier with its own pattern.
		{version: "3.9", wantErr: true},
		{version: "not-a-version", wantErr: true},
	}

	for _, tc := range cases {
		name := tc.version
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			cl := &config.Cluster{Name: "c", Brokers: []string{"h:1"}, KafkaVersion: tc.version}
			conf, err := BuildSaramaConfig(cl)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				if !strings.Contains(err.Error(), "unsupported kafka-version") {
					t.Errorf("error = %v, want it to mention the version", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildSaramaConfig: %v", err)
			}
			if !conf.Version.IsAtLeast(tc.want) || conf.Version.String() != tc.want.String() {
				t.Errorf("Version = %s, want %s", conf.Version, tc.want)
			}
		})
	}
}

func TestBuildSaramaConfigSASL(t *testing.T) {
	cases := []struct {
		mechanism string
		want      sarama.SASLMechanism
		wantSCRAM bool
	}{
		{config.MechanismPlain, sarama.SASLTypePlaintext, false},
		{config.MechanismSCRAMSHA256, sarama.SASLTypeSCRAMSHA256, true},
		{config.MechanismSCRAMSHA512, sarama.SASLTypeSCRAMSHA512, true},
	}

	for _, tc := range cases {
		t.Run(tc.mechanism, func(t *testing.T) {
			cl := &config.Cluster{
				Name:    "c",
				Brokers: []string{"h:1"},
				SASL:    config.SASL{Enabled: true, Mechanism: tc.mechanism, Username: "u", Password: "p"},
			}

			conf, err := BuildSaramaConfig(cl)
			if err != nil {
				t.Fatalf("BuildSaramaConfig: %v", err)
			}
			if !conf.Net.SASL.Enable {
				t.Fatal("SASL should be enabled")
			}
			if conf.Net.SASL.Mechanism != tc.want {
				t.Errorf("Mechanism = %v, want %v", conf.Net.SASL.Mechanism, tc.want)
			}
			if conf.Net.SASL.User != "u" || conf.Net.SASL.Password != "p" {
				t.Error("credentials were not passed through")
			}

			if !tc.wantSCRAM {
				return
			}
			if conf.Net.SASL.SCRAMClientGeneratorFunc == nil {
				t.Fatal("SCRAM mechanisms need a client generator")
			}
			// The generator must produce a client that can start a conversation:
			// a nil hash function would only fail at connect time.
			client := conf.Net.SASL.SCRAMClientGeneratorFunc()
			if err := client.Begin("user", "pass", ""); err != nil {
				t.Errorf("SCRAM client Begin: %v", err)
			}
			response, err := client.Step("")
			if err != nil {
				t.Errorf("SCRAM client Step: %v", err)
			}
			if !strings.Contains(response, "user") {
				t.Errorf("first SCRAM message should carry the username, got %q", response)
			}
		})
	}
}

func TestBuildSaramaConfigRejectsUnknownMechanism(t *testing.T) {
	// config.Validate normally catches this; the client must not silently accept
	// it if the two lists ever drift apart.
	cl := &config.Cluster{
		Name:    "c",
		Brokers: []string{"h:1"},
		SASL:    config.SASL{Enabled: true, Mechanism: "kerberos", Username: "u", Password: "p"},
	}
	if _, err := BuildSaramaConfig(cl); err == nil {
		t.Fatal("expected an error for an unknown mechanism")
	}
}

// ---------------------------------------------------------------- TLS ---

func TestTLSWithCustomCA(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeCertificate(t, dir, "ca", "")

	cl := loadCluster(t, dir, "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n      ca: "+
		filepath.Base(caPath)+"\n")

	conf, err := BuildSaramaConfig(cl)
	if err != nil {
		t.Fatalf("BuildSaramaConfig: %v", err)
	}
	if !conf.Net.TLS.Enable {
		t.Fatal("TLS should be enabled")
	}
	if conf.Net.TLS.Config.RootCAs == nil {
		t.Error("the certificate pool is empty")
	}
	if conf.Net.TLS.Config.MinVersion != tls.VersionTLS12 {
		t.Error("TLS 1.2 should be the floor")
	}
}

func TestTLSRejectsGarbageCA(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, []byte("this is not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cl := loadCluster(t, dir, "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n      ca: ca.crt\n")

	_, err := BuildSaramaConfig(cl)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "no usable PEM certificate") {
		t.Errorf("error = %v, want it to explain the file is not a certificate", err)
	}
}

func TestTLSMutual(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertificate(t, dir, "client", "")

	cl := loadCluster(t, dir, "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n"+
		"      cert: "+filepath.Base(certPath)+"\n      cert-key: "+filepath.Base(keyPath)+"\n")

	conf, err := BuildSaramaConfig(cl)
	if err != nil {
		t.Fatalf("BuildSaramaConfig: %v", err)
	}
	if len(conf.Net.TLS.Config.Certificates) != 1 {
		t.Errorf("got %d client certificates, want 1", len(conf.Net.TLS.Config.Certificates))
	}
}

func TestTLSEncryptedKey(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertificate(t, dir, "client", "s3cret")
	base := "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n" +
		"      cert: " + filepath.Base(certPath) + "\n      cert-key: " + filepath.Base(keyPath) + "\n"

	t.Run("correct passphrase", func(t *testing.T) {
		cl := loadCluster(t, dir, base+"      cert-key-passphrase: s3cret\n")
		conf, err := BuildSaramaConfig(cl)
		if err != nil {
			t.Fatalf("BuildSaramaConfig: %v", err)
		}
		if len(conf.Net.TLS.Config.Certificates) != 1 {
			t.Error("the encrypted key was not loaded")
		}
	})

	t.Run("wrong passphrase", func(t *testing.T) {
		cl := loadCluster(t, dir, base+"      cert-key-passphrase: wrong\n")
		_, err := BuildSaramaConfig(cl)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "cert-key-passphrase") {
			t.Errorf("error = %v, want it to point at the passphrase", err)
		}
	})

	t.Run("missing passphrase", func(t *testing.T) {
		cl := loadCluster(t, dir, base)
		_, err := BuildSaramaConfig(cl)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "set tls.cert-key-passphrase") {
			t.Errorf("error = %v, want it to name the setting to add", err)
		}
	})
}

func TestTLSLegacyEncryptedKeyIsRejectedWithInstructions(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertificate(t, dir, "client", "")

	// Old-style OpenSSL encryption: PKCS#1 with DEK-Info headers. Go dropped
	// support for it, so the user has to convert the file.
	legacy := "-----BEGIN RSA PRIVATE KEY-----\n" +
		"Proc-Type: 4,ENCRYPTED\n" +
		"DEK-Info: AES-256-CBC,0123456789ABCDEF0123456789ABCDEF\n\n" +
		"MIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu\n" +
		"-----END RSA PRIVATE KEY-----\n"
	if err := os.WriteFile(keyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	cl := loadCluster(t, dir, "clusters:\n  - name: c\n    brokers: [h:1]\n    tls:\n      enabled: true\n"+
		"      cert: "+filepath.Base(certPath)+"\n      cert-key: "+filepath.Base(keyPath)+"\n")

	_, err := BuildSaramaConfig(cl)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "openssl pkcs8 -topk8") {
		t.Errorf("the error must include the conversion command, got: %v", err)
	}
}

func TestTLSInsecure(t *testing.T) {
	cl := &config.Cluster{
		Name: "c", Brokers: []string{"h:1"},
		TLS: config.TLS{Enabled: true, Insecure: true},
	}
	conf, err := BuildSaramaConfig(cl)
	if err != nil {
		t.Fatalf("BuildSaramaConfig: %v", err)
	}
	if !conf.Net.TLS.Config.InsecureSkipVerify {
		t.Error("insecure should disable verification")
	}
}

// ----------------------------------------------------------- diagnosis ---

func TestDiagnose(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{
			"unknown authority",
			// Wrapped the way sarama delivers it, to prove errors.As reaches through.
			errors.Join(sarama.ErrOutOfBrokers, x509.UnknownAuthorityError{}),
			"not trusted",
		},
		{
			"hostname mismatch",
			x509.HostnameError{Host: "broker.example"},
			"not valid for the address",
		},
		{"sasl failure", sarama.ErrSASLAuthenticationFailed, "authentication was rejected"},
		{"topic acl", sarama.ErrTopicAuthorizationFailed, "ACL is missing"},
		{"group acl", sarama.ErrGroupAuthorizationFailed, "ACL is missing"},
		{
			"connection refused",
			&net.OpError{Op: "dial", Err: errors.New("connect: connection refused")},
			"nothing is listening",
		},
		{"dns", &net.DNSError{Name: "nowhere.invalid"}, "does not resolve"},
		{"deadline", context.DeadlineExceeded, "did not answer in time"},
		{"out of brokers", sarama.ErrOutOfBrokers, "none of the bootstrap brokers"},
		{"unrecognised", errors.New("something else entirely"), ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Diagnose(tc.err)
			if tc.want == "" {
				if got != "" {
					t.Errorf("Diagnose() = %q, want an empty diagnosis", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("Diagnose() = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------- mock ---

func TestMock(t *testing.T) {
	cluster := &config.Cluster{Name: "test", Brokers: []string{"h:1"}}
	m := NewMock(cluster)

	if m.Cluster().Name != "test" {
		t.Error("Cluster() returned the wrong cluster")
	}

	info, err := m.DescribeCluster(context.Background())
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	if len(info.Brokers) != 3 {
		t.Errorf("got %d brokers, want 3", len(info.Brokers))
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	want := []string{"Cluster", "DescribeCluster", "Close"}
	got := m.Calls()
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMockFailure(t *testing.T) {
	m := NewMock(nil)
	m.DescribeClusterFunc = func(context.Context) (ClusterInfo, error) {
		return ClusterInfo{}, sarama.ErrOutOfBrokers
	}

	if _, err := m.DescribeCluster(context.Background()); !errors.Is(err, sarama.ErrOutOfBrokers) {
		t.Errorf("error = %v, want ErrOutOfBrokers", err)
	}
}

func TestMockSatisfiesInstantiator(t *testing.T) {
	instantiate := MockInstantiator(NewMock(nil))
	facade, err := instantiate(&config.Cluster{Name: "c"})
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	if facade == nil {
		t.Fatal("instantiator returned nil")
	}
}

// ----------------------------------------------------------------- run ---

func TestRunHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := run(ctx, func() (int, error) {
		time.Sleep(2 * time.Second) // would outlive the test if it were awaited
		return 42, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestRunReturnsValue(t *testing.T) {
	got, err := run(context.Background(), func() (string, error) { return "value", nil })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}
}

// Every operational error a person can meet has to arrive as a sentence, not as
// a protocol code. These are the ones the tabs surface.
func TestDiagnoseOperationalErrors(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{sarama.ErrUnknownTopicOrPartition, "does not exist"},
		{sarama.ErrInvalidTopic, "letters, digits"},
		{sarama.ErrTopicAlreadyExists, "already exists"},
		{sarama.ErrInvalidPartitions, "only be added"},
		{sarama.ErrInvalidReplicationFactor, "exceeds the number of brokers"},
		{sarama.ErrInvalidConfig, "rejected that setting"},
		{sarama.ErrPolicyViolation, "policy refused"},
		{sarama.ErrLeaderNotAvailable, "no leader"},
		{sarama.ErrNotLeaderForPartition, "no leader"},
		{sarama.ErrBrokerNotAvailable, "not available"},
		{sarama.ErrKafkaStorageError, "log directory"},
		{sarama.ErrRequestTimedOut, "did not answer in time"},
		{sarama.ErrNotEnoughReplicas, "min.insync.replicas"},
		{sarama.ErrOffsetOutOfRange, "outside the partition"},
		{sarama.ErrGroupIDNotFound, "does not exist"},
		{sarama.ErrNonEmptyGroup, "stop its consumers"},
		{sarama.ErrUnknownMemberId, "stop its consumers"},
		{sarama.ErrGroupSubscribedToTopic, "stop its consumers"},
		{sarama.ErrConsumerCoordinatorNotAvailable, "coordinator"},
		{sarama.ErrOffsetsLoadInProgress, "loading offsets"},
		{sarama.ErrSecurityDisabled, "no authorizer"},
		{sarama.ErrClosedClient, "connection was closed"},
		{sarama.ErrTopicAuthorizationFailed, "ACL is missing"},
		{sarama.ErrGroupAuthorizationFailed, "ACL is missing"},
	}

	for _, test := range cases {
		t.Run(test.err.Error(), func(t *testing.T) {
			got := Diagnose(test.err)
			if got == "" {
				t.Fatalf("%v has no diagnosis", test.err)
			}
			if !strings.Contains(got, test.want) {
				t.Errorf("diagnosis = %q, want it to mention %q", got, test.want)
			}
			// The diagnosis has to survive being wrapped, which is how every
			// error reaches the UI.
			wrapped := fmt.Errorf("create the topic: %w", test.err)
			if Diagnose(wrapped) != got {
				t.Errorf("wrapping lost the diagnosis: %q", Diagnose(wrapped))
			}
		})
	}
}

// Explain is what the screens call: kafman's wording when there is one, the
// error's own text when there is not.
func TestExplain(t *testing.T) {
	if got := Explain(nil); got != "" {
		t.Errorf("Explain(nil) = %q, want empty", got)
	}

	known := Explain(fmt.Errorf("delete: %w", sarama.ErrNonEmptyGroup))
	if !strings.Contains(known, "stop its consumers") {
		t.Errorf("a known error should be explained, got %q", known)
	}

	// Anything kafman has no wording for keeps its own text rather than being
	// swallowed or replaced by something vague.
	unknown := Explain(errors.New("something specific happened"))
	if unknown != "something specific happened" {
		t.Errorf("an unknown error should keep its text, got %q", unknown)
	}
}
