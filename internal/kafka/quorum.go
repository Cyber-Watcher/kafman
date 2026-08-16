package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// The KRaft metadata quorum, over a raw DescribeQuorum (API 55) request.
//
// sarama has no DescribeQuorum and its protocol layer is sealed, but brokers
// do accept the request over a plain client connection and forward it to the
// quorum — kafka-metadata-quorum.sh works through --bootstrap-server the same
// way. So kafman opens one short-lived connection of its own, using the same
// TLS and SASL material as the sarama client, and speaks just enough of the
// protocol for this one call. The kmsg package supplies the message codecs;
// the framing, header and SASL steps live here.

// QuorumReplica is one member of the metadata quorum.
type QuorumReplica struct {
	ID           int32
	LogEndOffset int64

	// Lag is how far this replica's log is behind the leader's, 0 for the
	// leader itself, -1 when either end is unknown.
	Lag int64
}

// QuorumInfo is what DescribeQuorum reports about the metadata quorum.
type QuorumInfo struct {
	LeaderID      int32
	LeaderEpoch   int32
	HighWatermark int64

	// Voters are the controller nodes of the quorum; Observers are the nodes
	// that follow it without voting — on KRaft these are the plain brokers.
	Voters    []QuorumReplica
	Observers []QuorumReplica
}

// Role names a node's place in the quorum: "leader", "follower", "observer",
// or "" for a node the quorum does not know.
func (q QuorumInfo) Role(id int32) string {
	for _, voter := range q.Voters {
		if voter.ID == id {
			if id == q.LeaderID {
				return "leader"
			}
			return "follower"
		}
	}
	for _, observer := range q.Observers {
		if observer.ID == id {
			return "observer"
		}
	}
	return ""
}

// ErrNoQuorum reports a cluster without a describable metadata quorum: a
// ZooKeeper-era cluster, or brokers too old to answer.
var ErrNoQuorum = errors.New("the cluster does not expose a metadata quorum (not KRaft, or too old)")

// clusterMetadataTopic is the internal log the quorum replicates.
const clusterMetadataTopic = "__cluster_metadata"

// DescribeQuorum reports the KRaft metadata quorum: the elected leader, its
// epoch, and every voter and observer with how far behind it is.
func (c *Client) DescribeQuorum(ctx context.Context) (QuorumInfo, error) {
	return run(ctx, func() (QuorumInfo, error) {
		maybeDelay("describe quorum")

		// Prefer the addresses the cluster advertises, fall back to the
		// bootstrap list; the first broker that answers decides.
		var addrs []string
		for _, broker := range c.client.Brokers() {
			addrs = append(addrs, broker.Addr())
		}
		if len(addrs) == 0 {
			addrs = c.cluster.Brokers
		}

		timeout := c.conf.Net.DialTimeout
		var lastErr error
		for _, addr := range addrs {
			info, err := describeQuorumAt(c.cluster, addr, timeout)
			if err == nil || errors.Is(err, ErrNoQuorum) {
				return info, err
			}
			lastErr = err
		}
		return QuorumInfo{}, fmt.Errorf("describe quorum: %w", lastErr)
	})
}

// describeQuorumAt runs the whole exchange against one broker.
func describeQuorumAt(cl *config.Cluster, addr string, timeout time.Duration) (QuorumInfo, error) {
	conn, err := dialProto(cl, addr, timeout)
	if err != nil {
		return QuorumInfo{}, err
	}
	defer func() { _ = conn.Close() }()

	// The broker's ApiVersions decides whether the call exists at all and at
	// which version; v0 of ApiVersions itself is understood by every broker.
	versions := kmsg.NewApiVersionsResponse()
	apiReq := kmsg.NewApiVersionsRequest()
	apiReq.SetVersion(0)
	if err := conn.roundTrip(&apiReq, &versions); err != nil {
		return QuorumInfo{}, fmt.Errorf("api versions: %w", err)
	}

	version := int16(-1)
	quorumReq := kmsg.NewDescribeQuorumRequest()
	for _, api := range versions.ApiKeys {
		if api.ApiKey == quorumReq.Key() {
			version = min(api.MaxVersion, quorumReq.MaxVersion())
		}
	}
	if version < 0 {
		return QuorumInfo{}, ErrNoQuorum
	}

	partition := kmsg.NewDescribeQuorumRequestTopicPartition()
	partition.Partition = 0
	topic := kmsg.NewDescribeQuorumRequestTopic()
	topic.Topic = clusterMetadataTopic
	topic.Partitions = []kmsg.DescribeQuorumRequestTopicPartition{partition}
	quorumReq.SetVersion(version)
	quorumReq.Topics = []kmsg.DescribeQuorumRequestTopic{topic}

	response := kmsg.NewDescribeQuorumResponse()
	if err := conn.roundTrip(&quorumReq, &response); err != nil {
		return QuorumInfo{}, fmt.Errorf("describe quorum: %w", err)
	}
	return quorumFromResponse(&response)
}

// quorumFromResponse checks the errors and reshapes the wire response.
func quorumFromResponse(response *kmsg.DescribeQuorumResponse) (QuorumInfo, error) {
	if err := kafkaError(response.ErrorCode, response.ErrorMessage); err != nil {
		return QuorumInfo{}, err
	}
	for _, topic := range response.Topics {
		if topic.Topic != clusterMetadataTopic {
			continue
		}
		for _, partition := range topic.Partitions {
			if partition.Partition != 0 {
				continue
			}
			if err := kafkaError(partition.ErrorCode, partition.ErrorMessage); err != nil {
				return QuorumInfo{}, err
			}

			info := QuorumInfo{
				LeaderID:      partition.LeaderID,
				LeaderEpoch:   partition.LeaderEpoch,
				HighWatermark: partition.HighWatermark,
			}

			// The leader's own log end decides everyone else's lag.
			leaderEnd := int64(-1)
			for _, voter := range partition.CurrentVoters {
				if voter.ReplicaID == partition.LeaderID {
					leaderEnd = voter.LogEndOffset
				}
			}
			info.Voters = replicaStates(partition.CurrentVoters, partition.LeaderID, leaderEnd)
			info.Observers = replicaStates(partition.Observers, partition.LeaderID, leaderEnd)
			return info, nil
		}
	}
	return QuorumInfo{}, fmt.Errorf("describe quorum: the response covers no %s partition", clusterMetadataTopic)
}

func replicaStates(states []kmsg.DescribeQuorumResponseTopicPartitionReplicaState, leaderID int32, leaderEnd int64) []QuorumReplica {
	out := make([]QuorumReplica, 0, len(states))
	for _, state := range states {
		replica := QuorumReplica{ID: state.ReplicaID, LogEndOffset: state.LogEndOffset, Lag: -1}
		switch {
		case state.ReplicaID == leaderID:
			replica.Lag = 0
		case leaderEnd >= 0 && state.LogEndOffset >= 0:
			replica.Lag = max(leaderEnd-state.LogEndOffset, 0)
		}
		out = append(out, replica)
	}
	return out
}

// kafkaError turns a wire error code into an error, mapping the codes that
// mean "there is no quorum here" onto ErrNoQuorum.
func kafkaError(code int16, message *string) error {
	if code == 0 {
		return nil
	}
	// 35 UNSUPPORTED_VERSION, 3 UNKNOWN_TOPIC_OR_PARTITION: a broker that has
	// no metadata quorum to describe.
	if code == 35 || code == 3 {
		return ErrNoQuorum
	}
	if message != nil && *message != "" {
		return fmt.Errorf("kafka error %d: %s", code, *message)
	}
	return fmt.Errorf("kafka error %d", code)
}

// ------------------------------------------------------- the tiny transport ---

// protoConn is a single-purpose Kafka protocol connection: framing, request
// headers and SASL, nothing more.
type protoConn struct {
	conn        net.Conn
	clientID    string
	correlation int32
	timeout     time.Duration
}

// dialProto opens a connection the way the cluster's configuration says to:
// TCP, then TLS if enabled, then SASL if enabled.
func dialProto(cl *config.Cluster, addr string, timeout time.Duration) (*protoConn, error) {
	if timeout <= 0 {
		timeout = config.DefaultRequestTimeout
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	if cl.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(cl)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		tlsConfig = tlsConfig.Clone()
		if tlsConfig.ServerName == "" {
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil {
				tlsConfig.ServerName = host
			}
		}
		tlsConn := tls.Client(conn, tlsConfig)
		_ = tlsConn.SetDeadline(time.Now().Add(timeout))
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		_ = tlsConn.SetDeadline(time.Time{})
		conn = tlsConn
	}

	clientID := cl.ClientID
	if clientID == "" {
		clientID = config.DefaultClientID
	}
	p := &protoConn{conn: conn, clientID: clientID, timeout: timeout}

	if cl.SASL.Enabled {
		if err := p.authenticate(cl); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("sasl: %w", err)
		}
	}
	return p, nil
}

func (p *protoConn) Close() error { return p.conn.Close() }

// roundTrip writes one framed request and decodes its response.
func (p *protoConn) roundTrip(req kmsg.Request, res kmsg.Response) error {
	p.correlation++

	// Request header: v2 with a tag section for flexible requests, v1 without
	// one for the rest.
	header := make([]byte, 0, 32)
	header = binary.BigEndian.AppendUint16(header, uint16(req.Key()))
	header = binary.BigEndian.AppendUint16(header, uint16(req.GetVersion()))
	header = binary.BigEndian.AppendUint32(header, uint32(p.correlation))
	header = binary.BigEndian.AppendUint16(header, uint16(len(p.clientID)))
	header = append(header, p.clientID...)
	if req.IsFlexible() {
		header = append(header, 0) // no tagged fields
	}
	payload := req.AppendTo(header)

	frame := binary.BigEndian.AppendUint32(make([]byte, 0, len(payload)+4), uint32(len(payload)))
	frame = append(frame, payload...)

	_ = p.conn.SetDeadline(time.Now().Add(p.timeout))
	defer func() { _ = p.conn.SetDeadline(time.Time{}) }()
	if _, err := p.conn.Write(frame); err != nil {
		return err
	}

	var sizeBuf [4]byte
	if _, err := io.ReadFull(p.conn, sizeBuf[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	const maxResponse = 64 << 20
	if size > maxResponse {
		return fmt.Errorf("response of %d bytes is beyond reason", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(p.conn, body); err != nil {
		return err
	}

	// Response header: the correlation id, plus a tag section when the
	// response is flexible. ApiVersions is exempt from the tag section by
	// protocol fiat, but at v0 it is not flexible anyway.
	if len(body) < 4 {
		return fmt.Errorf("response of %d bytes has no header", len(body))
	}
	if got := int32(binary.BigEndian.Uint32(body)); got != p.correlation {
		return fmt.Errorf("correlation id %d does not answer request %d", got, p.correlation)
	}
	body = body[4:]

	res.SetVersion(req.GetVersion())
	if res.IsFlexible() {
		var err error
		if body, err = skipTagSection(body); err != nil {
			return err
		}
	}
	return res.ReadFrom(body)
}

// skipTagSection drops a flexible header's tagged fields.
func skipTagSection(b []byte) ([]byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("malformed tag section")
	}
	b = b[n:]
	for range count {
		if _, n = binary.Uvarint(b); n <= 0 {
			return nil, errors.New("malformed tag")
		}
		b = b[n:]
		size, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < size {
			return nil, errors.New("malformed tag size")
		}
		b = b[n+int(size):]
	}
	return b, nil
}

// authenticate runs the SASL exchange the cluster is configured for.
func (p *protoConn) authenticate(cl *config.Cluster) error {
	var mechanism string
	switch cl.SASL.Mechanism {
	case config.MechanismPlain:
		mechanism = "PLAIN"
	case config.MechanismSCRAMSHA256:
		mechanism = "SCRAM-SHA-256"
	case config.MechanismSCRAMSHA512:
		mechanism = "SCRAM-SHA-512"
	default:
		return fmt.Errorf("unsupported mechanism %q", cl.SASL.Mechanism)
	}

	handshake := kmsg.NewSASLHandshakeRequest()
	handshake.SetVersion(1)
	handshake.Mechanism = mechanism
	handshakeRes := kmsg.NewSASLHandshakeResponse()
	if err := p.roundTrip(&handshake, &handshakeRes); err != nil {
		return err
	}
	if handshakeRes.ErrorCode != 0 {
		return fmt.Errorf("the broker refused mechanism %s (error %d, offers %v)",
			mechanism, handshakeRes.ErrorCode, handshakeRes.SupportedMechanisms)
	}

	if mechanism == "PLAIN" {
		payload := []byte("\x00" + cl.SASL.Username + "\x00" + cl.SASL.Password)
		_, err := p.saslStep(payload)
		return err
	}

	// SCRAM: the same client sarama uses, driven directly.
	client := &scramClient{HashGeneratorFcn: sha256Gen}
	if mechanism == "SCRAM-SHA-512" {
		client = &scramClient{HashGeneratorFcn: sha512Gen}
	}
	if err := client.Begin(cl.SASL.Username, cl.SASL.Password, ""); err != nil {
		return err
	}
	message, err := client.Step("")
	if err != nil {
		return err
	}
	for !client.Done() {
		challenge, err := p.saslStep([]byte(message))
		if err != nil {
			return err
		}
		if message, err = client.Step(string(challenge)); err != nil {
			return err
		}
	}
	return nil
}

// saslStep sends one SASLAuthenticate round and returns the server's bytes.
func (p *protoConn) saslStep(payload []byte) ([]byte, error) {
	request := kmsg.NewSASLAuthenticateRequest()
	request.SetVersion(1)
	request.SASLAuthBytes = payload

	response := kmsg.NewSASLAuthenticateResponse()
	if err := p.roundTrip(&request, &response); err != nil {
		return nil, err
	}
	if response.ErrorCode != 0 {
		if response.ErrorMessage != nil && *response.ErrorMessage != "" {
			return nil, fmt.Errorf("authentication failed: %s", *response.ErrorMessage)
		}
		return nil, fmt.Errorf("authentication failed (error %d)", response.ErrorCode)
	}
	return response.SASLAuthBytes, nil
}
