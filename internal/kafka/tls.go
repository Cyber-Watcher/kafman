package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/youmark/pkcs8"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// buildTLSConfig turns the cluster's TLS settings into a *tls.Config.
//
// Every failure returns an error. ktea panics here instead, which kills the
// whole TUI over a typo in a certificate path; see docs/project_kb.md.
func buildTLSConfig(c *config.Cluster) (*tls.Config, error) {
	t := c.TLS

	// Start from the system pool and add to it, rather than replacing it: a
	// cluster may present a certificate from a public CA while also needing a
	// private one for something else.
	pool, err := x509.SystemCertPool()
	if err != nil {
		slog.Warn("cannot read the system certificate pool, starting from an empty one", "error", err)
		pool = x509.NewCertPool()
	}

	if path := t.CAFile(); path != "" {
		pem, err := os.ReadFile(path) //nolint:gosec // path comes from the user's config, by design
		if err != nil {
			return nil, fmt.Errorf("read CA certificate %s: %w", path, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA certificate %s contains no usable PEM certificate", path)
		}
	}

	cfg := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: t.Insecure, //nolint:gosec // explicitly requested by the user
		MinVersion:         tls.VersionTLS12,
	}

	if t.Insecure {
		slog.Warn("TLS certificate verification is disabled for this cluster", "cluster", c.Name)
	}

	// Mutual TLS. The config layer already rejects one without the other.
	if t.CertFile() != "" && t.KeyFile() != "" {
		cert, err := loadClientCertificate(t.CertFile(), t.KeyFile(), t.CertKeyPassphrase)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// loadClientCertificate reads a certificate and its key, decrypting the key when
// a passphrase is supplied.
func loadClientCertificate(certFile, keyFile, passphrase string) (tls.Certificate, error) {
	certPEM, err := os.ReadFile(certFile) //nolint:gosec // path comes from the user's config
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("read client certificate %s: %w", certFile, err)
	}

	keyPEM, err := os.ReadFile(keyFile) //nolint:gosec // path comes from the user's config
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("read client key %s: %w", keyFile, err)
	}

	keyPEM, err = decryptKeyPEM(keyPEM, keyFile, passphrase)
	if err != nil {
		return tls.Certificate{}, err
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("client certificate %s and key %s do not form a valid pair: %w",
			certFile, keyFile, err)
	}
	return cert, nil
}

// decryptKeyPEM returns the key in a form crypto/tls accepts.
//
// Three cases are distinguished so the user gets a message they can act on:
// an unencrypted key passes through, an encrypted PKCS#8 key is decrypted with
// the passphrase, and a legacy PKCS#1 key encrypted with DEK-Info headers is
// rejected with the conversion command, because Go removed support for it
// (see https://pkg.go.dev/encoding/pem#DecryptPEMBlock, deprecated as insecure).
func decryptKeyPEM(keyPEM []byte, keyFile, passphrase string) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("client key %s is not valid PEM", keyFile)
	}

	if isLegacyEncryptedPEM(block) {
		return nil, fmt.Errorf(
			"client key %s uses legacy PEM encryption (PKCS#1 with DEK-Info), which is not supported.\n"+
				"Convert it to PKCS#8:\n"+
				"    openssl pkcs8 -topk8 -v2 aes-256-cbc -in %s -out key.pk8.pem",
			keyFile, keyFile)
	}

	if !isEncryptedPKCS8(block) {
		if passphrase != "" {
			slog.Warn("a cert-key-passphrase is set but the key is not encrypted; ignoring it",
				"key", keyFile)
		}
		return keyPEM, nil
	}

	if passphrase == "" {
		return nil, fmt.Errorf(
			"client key %s is encrypted but no passphrase was given; set tls.cert-key-passphrase",
			keyFile)
	}

	key, err := pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
	if err != nil {
		return nil, fmt.Errorf("decrypt client key %s: %w (wrong cert-key-passphrase?)", keyFile, err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("re-encode client key %s: %w", keyFile, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// isLegacyEncryptedPEM reports the old OpenSSL format: a PKCS#1 block carrying
// "Proc-Type: 4,ENCRYPTED" and "DEK-Info" headers.
func isLegacyEncryptedPEM(block *pem.Block) bool {
	if strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") {
		return true
	}
	_, hasDEK := block.Headers["DEK-Info"]
	return hasDEK
}

// isEncryptedPKCS8 reports the modern format: "-----BEGIN ENCRYPTED PRIVATE KEY-----".
func isEncryptedPKCS8(block *pem.Block) bool {
	return block.Type == "ENCRYPTED PRIVATE KEY"
}
