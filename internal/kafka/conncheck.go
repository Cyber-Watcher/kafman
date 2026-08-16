package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// ConnCheck is the outcome of testing a cluster connection.
type ConnCheck struct {
	Cluster *config.Cluster
	Info    ClusterInfo
	Latency time.Duration

	// Err is nil on success. Diagnosis carries a human-readable explanation.
	Err       error
	Diagnosis string
}

// OK reports whether the cluster was reachable.
func (r ConnCheck) OK() bool { return r.Err == nil }

// CheckConnectivity opens a throwaway connection, describes the cluster and
// closes it again. It never returns an error: a failure is part of the result,
// because the Clusters tab has to render it rather than propagate it.
func CheckConnectivity(ctx context.Context, c *config.Cluster) ConnCheck {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultConnectTimeout)
		defer cancel()
	}

	started := time.Now()

	client, err := run(ctx, func() (*Client, error) { return New(c) })
	if err != nil {
		return ConnCheck{Cluster: c, Latency: time.Since(started), Err: err, Diagnosis: Diagnose(err)}
	}
	defer func() { _ = client.Close() }()

	info, err := client.DescribeCluster(ctx)
	if err != nil {
		return ConnCheck{Cluster: c, Latency: time.Since(started), Err: err, Diagnosis: Diagnose(err)}
	}

	return ConnCheck{Cluster: c, Info: info, Latency: time.Since(started)}
}

// Diagnose turns a Kafka or network error into a sentence that says what to do.
//
// sarama's errors are accurate but unhelpful to anyone who is not already
// familiar with them: "kafka: client has run out of available brokers to talk
// to" is what you get for a wrong port, a wrong password and a firewall alike.
func Diagnose(err error) string {
	if err == nil {
		return ""
	}

	// Certificate problems first: they are specific and actionable.
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return "the broker's certificate is signed by a CA that is not trusted — set tls.ca to the issuing certificate, or tls.insecure: true to skip verification"
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return "the broker's certificate is not valid for the address it was reached at (" +
			hostnameErr.Host + ") — check the broker address, or the certificate's subject alternative names"
	}
	var invalidCert x509.CertificateInvalidError
	if errors.As(err, &invalidCert) {
		if invalidCert.Reason == x509.Expired {
			return "the broker's certificate has expired"
		}
		return "the broker's certificate was rejected: " + invalidCert.Error()
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return "the broker replied with something that is not TLS — the listener is probably plaintext, so turn tls.enabled off"
	}

	// Authentication.
	if errors.Is(err, sarama.ErrSASLAuthenticationFailed) || mentions(err, "SASL authentication failed") {
		return "authentication was rejected — check sasl.username, sasl.password and that sasl.mechanism matches what the broker expects"
	}
	if mentions(err, "unsupported SASL mechanism") || errors.Is(err, sarama.ErrUnsupportedSASLMechanism) {
		return "the broker does not offer this SASL mechanism — check sasl.mechanism against the listener's configuration"
	}
	if errors.Is(err, sarama.ErrIllegalSASLState) {
		return "the broker did not expect SASL on this listener — it is probably a plaintext port, so turn sasl.enabled off"
	}

	// Authorisation.
	if errors.Is(err, sarama.ErrTopicAuthorizationFailed) {
		return "this user is not allowed to access that topic — an ACL is missing"
	}
	if errors.Is(err, sarama.ErrGroupAuthorizationFailed) {
		return "this user is not allowed to access that consumer group — an ACL is missing"
	}
	if errors.Is(err, sarama.ErrClusterAuthorizationFailed) {
		return "this user is not allowed to perform cluster-level operations — an ACL is missing"
	}

	// Network.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "the broker hostname " + dnsErr.Name + " does not resolve — check the address in brokers"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if strings.Contains(opErr.Error(), "connection refused") {
			return "nothing is listening on that address — check the port, and that the broker is running"
		}
		return "the network connection failed: " + opErr.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) || mentions(err, "i/o timeout") {
		return "the broker did not answer in time — it may be unreachable, or request-timeout may be too short"
	}
	if errors.Is(err, context.Canceled) {
		return "the connection attempt was cancelled"
	}

	// sarama's catch-all, which hides all of the above when it wraps them.
	if errors.Is(err, sarama.ErrOutOfBrokers) {
		return "none of the bootstrap brokers could be reached — check brokers, the security settings, and that the cluster is up"
	}

	if errors.Is(err, sarama.ErrUnsupportedVersion) {
		return "this request is not available at the configured kafka-version — set kafka-version to the cluster's real version (an empty value is treated as 2.8, not as the newest)"
	}

	return diagnoseOperation(err)
}

// diagnoseOperation explains the errors that come back from working with a
// cluster rather than from connecting to one.
//
// Kafka's protocol errors are precise and unhelpful in equal measure:
// "kafka server: Request was for a topic or partition that does not exist on
// this broker" says nothing about which of the two it was, or what to do.
func diagnoseOperation(err error) string {
	switch {
	case errors.Is(err, sarama.ErrUnknownTopicOrPartition):
		return "that topic or partition does not exist — it may have been deleted, or the metadata may not have reached this broker yet"
	case errors.Is(err, sarama.ErrInvalidTopic):
		return "the topic name is not one Kafka accepts — letters, digits, dot, underscore and hyphen only"
	case errors.Is(err, sarama.ErrTopicAlreadyExists):
		return "a topic with that name already exists"
	case errors.Is(err, sarama.ErrInvalidPartitions):
		return "that partition count is not allowed — partitions can only be added, never removed"
	case errors.Is(err, sarama.ErrInvalidReplicationFactor):
		return "the replication factor exceeds the number of brokers"
	case errors.Is(err, sarama.ErrInvalidConfig):
		return "the broker rejected that setting — check the name and the value's units"
	case errors.Is(err, sarama.ErrPolicyViolation):
		return "a broker-side policy refused the request; the broker's log says which"

	case errors.Is(err, sarama.ErrLeaderNotAvailable), errors.Is(err, sarama.ErrNotLeaderForPartition):
		return "that partition has no leader right now — a broker is down or an election is in progress"
	case errors.Is(err, sarama.ErrBrokerNotAvailable), errors.Is(err, sarama.ErrReplicaNotAvailable):
		return "the broker holding that data is not available"
	case errors.Is(err, sarama.ErrKafkaStorageError):
		return "the broker could not read or write its log directory — its disk is the place to look"
	case errors.Is(err, sarama.ErrRequestTimedOut):
		return "the broker did not answer in time — it may be overloaded, or request-timeout may be too short"
	case errors.Is(err, sarama.ErrNotEnoughReplicas), errors.Is(err, sarama.ErrNotEnoughReplicasAfterAppend):
		return "fewer replicas are in sync than min.insync.replicas requires — the write was refused to keep the data safe"
	case errors.Is(err, sarama.ErrOffsetOutOfRange):
		return "that offset is outside the partition — it has been deleted by retention, or it is past the end"

	case errors.Is(err, sarama.ErrGroupIDNotFound):
		return "that consumer group does not exist"
	case errors.Is(err, sarama.ErrNonEmptyGroup):
		return "the consumer group still has members — stop its consumers first"
	case errors.Is(err, sarama.ErrUnknownMemberId), errors.Is(err, sarama.ErrRebalanceInProgress):
		return "the consumer group has live members — stop its consumers first"
	case errors.Is(err, sarama.ErrGroupSubscribedToTopic):
		return "the consumer group is subscribed to that topic — stop its consumers first"
	case errors.Is(err, sarama.ErrConsumerCoordinatorNotAvailable), errors.Is(err, sarama.ErrNotCoordinatorForConsumer):
		return "the group's coordinator is unavailable — usually brief, try again"
	case errors.Is(err, sarama.ErrOffsetsLoadInProgress):
		return "the coordinator is still loading offsets — usually brief, try again"

	case errors.Is(err, sarama.ErrSecurityDisabled):
		return "this cluster has no authorizer configured, so it has no ACLs to manage"
	case errors.Is(err, sarama.ErrUnsupportedForMessageFormat):
		return "the topic's message format is too old for this operation"
	case errors.Is(err, sarama.ErrClosedClient):
		return "the connection was closed — reconnect with F5, or switch cluster with Ctrl+K"
	}
	return ""
}

// Explain renders an error the way it should reach a person: kafman's own
// wording when it has one, and the error's own text when it does not.
//
// Everything the UI shows goes through here, so a protocol code never reaches
// the screen on its own.
func Explain(err error) string {
	if err == nil {
		return ""
	}
	if diagnosis := Diagnose(err); diagnosis != "" {
		return diagnosis
	}
	return err.Error()
}

func mentions(err error, substr string) bool {
	return strings.Contains(strings.ToLower(err.Error()), strings.ToLower(substr))
}
