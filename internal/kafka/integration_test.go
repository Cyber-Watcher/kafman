//go:build integration

// Integration tests against the local test stack.
//
//	make stack-up && make stack-init
//	go test -tags integration ./internal/kafka/ -v
//
// They are behind a build tag so `make test` stays fast and works without Docker.
package kafka

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// devConfig loads docker/kafman.dev.yaml relative to this source file.
func devConfig(t *testing.T) *config.Config {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source file")
	}
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "docker", "kafman.dev.yaml")

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load %s: %v\nIs the stack prepared? Run: make stack-up", path, err)
	}
	return cfg
}

func cluster(t *testing.T, name string) *config.Cluster {
	t.Helper()
	cl, err := devConfig(t).Cluster(name)
	if err != nil {
		t.Fatalf("cluster %s: %v", name, err)
	}
	return cl
}

// TestConnectAllProfiles walks every working connection profile in the dev
// config. This is the test that proves TLS and SASL are wired correctly.
func TestConnectAllProfiles(t *testing.T) {
	profiles := []string{"local", "local-sasl-plain", "local-sasl-scram", "local-ssl"}

	for _, name := range profiles {
		t.Run(name, func(t *testing.T) {
			cl := cluster(t, name)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result := CheckConnectivity(ctx, cl)
			if !result.OK() {
				t.Fatalf("connection failed: %v\ndiagnosis: %s", result.Err, result.Diagnosis)
			}

			if len(result.Info.Brokers) != 3 {
				t.Errorf("got %d brokers, want 3: %+v", len(result.Info.Brokers), result.Info.Brokers)
			}
			if result.Info.ClusterID == "" {
				t.Error("cluster id is empty")
			}

			// The stack runs combined KRaft nodes, so every broker carries the
			// controller role and says so in its config.
			var controllers int
			for _, b := range result.Info.Brokers {
				if b.IsController {
					controllers++
				}
				if b.Roles != "broker,controller" {
					t.Errorf("broker %d: roles = %q, want broker,controller", b.ID, b.Roles)
				}
			}
			if controllers != 3 {
				t.Errorf("got %d controllers, want 3 combined nodes", controllers)
			}

			t.Logf("%s: %s, %d brokers, controller %d, %v",
				name, cl.SecurityProfile(), len(result.Info.Brokers),
				result.Info.ControllerID, result.Latency.Round(time.Millisecond))
		})
	}
}

// TestDescribeQuorumIntegration exercises the raw DescribeQuorum transport
// over every connection profile: plaintext, SASL PLAIN, SCRAM and TLS all
// drive the same hand-rolled framing and authentication.
func TestDescribeQuorumIntegration(t *testing.T) {
	for _, name := range []string{"local", "local-sasl-plain", "local-sasl-scram", "local-ssl"} {
		t.Run(name, func(t *testing.T) {
			client, err := New(cluster(t, name))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			info, err := client.DescribeQuorum(context.Background())
			if err != nil {
				t.Fatalf("DescribeQuorum: %v", err)
			}

			if len(info.Voters) != 3 {
				t.Errorf("three combined nodes should all be voters, got %+v", info.Voters)
			}
			if info.Role(info.LeaderID) != "leader" {
				t.Errorf("leader %d should be one of the voters: %+v", info.LeaderID, info.Voters)
			}
			if info.HighWatermark <= 0 {
				t.Errorf("the metadata log cannot be empty, HW = %d", info.HighWatermark)
			}
			followers := 0
			for _, voter := range info.Voters {
				if info.Role(voter.ID) == "follower" {
					followers++
				}
			}
			if followers != 2 {
				t.Errorf("expected 2 followers, got %d", followers)
			}
			t.Logf("%s: leader=%d epoch=%d hw=%d voters=%d observers=%d",
				name, info.LeaderID, info.LeaderEpoch, info.HighWatermark,
				len(info.Voters), len(info.Observers))
		})
	}
}

// TestConnectFailures checks that the deliberately broken profiles fail in the
// expected way and produce a diagnosis a user can act on.
func TestConnectFailures(t *testing.T) {
	cases := []struct {
		cluster       string
		wantDiagnosis string
	}{
		{"local-bad-password", "authentication was rejected"},
		{"local-untrusted-ca", "not trusted"},
	}

	for _, tc := range cases {
		t.Run(tc.cluster, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result := CheckConnectivity(ctx, cluster(t, tc.cluster))
			if result.OK() {
				t.Fatal("expected the connection to fail, but it succeeded")
			}
			t.Logf("error:     %v", result.Err)
			t.Logf("diagnosis: %s", result.Diagnosis)

			if !strings.Contains(result.Diagnosis, tc.wantDiagnosis) {
				t.Errorf("diagnosis = %q, want it to mention %q", result.Diagnosis, tc.wantDiagnosis)
			}
		})
	}
}

func TestClientLifecycle(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	info, err := client.DescribeCluster(context.Background())
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	if len(info.Brokers) == 0 {
		t.Error("no brokers reported")
	}

	if err := client.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Close must be safe to call twice: the UI closes on cluster switch and again
	// on shutdown.
	if err := client.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	if _, err := client.DescribeCluster(ctx); err == nil {
		t.Error("expected a context error, got none")
	}
}

func TestConnectToNothing(t *testing.T) {
	cl := &config.Cluster{
		Name:           "unreachable",
		Brokers:        []string{"127.0.0.1:1"},
		RequestTimeout: config.Duration(2 * time.Second),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := CheckConnectivity(ctx, cl)
	if result.OK() {
		t.Fatal("expected a failure")
	}
	t.Logf("error:     %v", result.Err)
	t.Logf("diagnosis: %s", result.Diagnosis)
	if result.Diagnosis == "" {
		t.Error("a connection refused error should produce a diagnosis")
	}
}

func TestListTopicsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	topics, err := client.ListTopics(context.Background(), false)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics) < 5 {
		t.Errorf("got %d topics, expected the stack's test topics", len(topics))
	}

	for _, want := range []string{"orders.events", "payments.tx", "audit.log"} {
		found := false
		for _, topic := range topics {
			if topic.Name == want {
				found = true
				t.Logf("%-16s parts=%-3d RF=%d ISR=%d/%d",
					topic.Name, topic.Partitions, topic.ReplicationFactor,
					topic.InSync, topic.TotalReplicas)
				if topic.Partitions == 0 {
					t.Errorf("%s reports no partitions", want)
				}
				if topic.InSync != topic.TotalReplicas {
					t.Errorf("%s is under-replicated on a healthy stack: %d/%d",
						want, topic.InSync, topic.TotalReplicas)
				}
			}
		}
		if !found {
			t.Errorf("topic %s missing; run make stack-init", want)
		}
	}

	// Internal topics are hidden unless asked for.
	for _, topic := range topics {
		if topic.Internal {
			t.Errorf("internal topic %s should have been filtered out", topic.Name)
		}
	}
	withInternal, err := client.ListTopics(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(withInternal) <= len(topics) {
		t.Error("asking for internal topics should return more")
	}
}

func TestClusterHealthIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	health, err := client.ClusterHealth(context.Background())
	if err != nil {
		t.Fatalf("ClusterHealth: %v", err)
	}

	t.Logf("topics=%d partitions=%d in-sync=%d under-replicated=%d offline=%d RF=%.1f",
		health.Topics, health.Partitions, health.InSync,
		health.UnderReplicated, health.Offline, health.ReplicationFactor())

	if health.Partitions == 0 {
		t.Fatal("no partitions reported")
	}
	if !health.OK() {
		t.Errorf("a healthy stack should report no problems, worst: %v", health.Worst)
	}
	if health.InSync != health.Partitions {
		t.Errorf("in-sync %d of %d partitions", health.InSync, health.Partitions)
	}
	// metrics.raw is RF 1 and the rest RF 3, so the average lands between them.
	if rf := health.ReplicationFactor(); rf < 1 || rf > 3 {
		t.Errorf("replication factor %.2f is outside the expected range", rf)
	}
}

func TestConsumerGroupsWithLagIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	groups, err := client.ConsumerGroupsWithLag(context.Background())
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag: %v", err)
	}
	if len(groups) == 0 {
		t.Fatal("no consumer groups; run make stack-init")
	}

	byName := make(map[string]ConsumerGroup, len(groups))
	for _, g := range groups {
		byName[g.Name] = g
		t.Logf("%-18s state=%-12s members=%d lag=%d", g.Name, g.State, g.Members, g.TotalLag)
	}

	// docker/init.sh leaves billing-worker deliberately behind.
	lagging, ok := byName["billing-worker"]
	if !ok {
		t.Fatal("billing-worker missing; run make stack-init")
	}
	if lagging.TotalLag <= 0 {
		t.Errorf("billing-worker should be lagging, got %d", lagging.TotalLag)
	}

	// Sorted by lag descending, so the worst offender leads the list.
	if groups[0].TotalLag < groups[len(groups)-1].TotalLag {
		t.Error("groups should be sorted by lag, worst first")
	}
}

// TestConsumerGroupsDefaultVersion is the regression test for the empty
// kafka-version case. A cluster added through the form usually leaves the
// version blank, which sarama maps to its default protocol level (2.8) — below
// the 3.0 that the batched OffsetFetch needs. The lag call must fall back to
// per-group requests instead of failing with ErrUnsupportedVersion.
func TestConsumerGroupsDefaultVersion(t *testing.T) {
	cl := *cluster(t, "local")
	cl.KafkaVersion = ""

	client, err := New(&cl)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	groups, err := client.ConsumerGroupsWithLag(context.Background())
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag with default kafka-version: %v", err)
	}
	if len(groups) == 0 {
		t.Fatal("no consumer groups; run make stack-init")
	}

	byName := make(map[string]ConsumerGroup, len(groups))
	for _, g := range groups {
		byName[g.Name] = g
	}
	lagging, ok := byName["billing-worker"]
	if !ok {
		t.Fatal("billing-worker missing; run make stack-init")
	}
	if !lagging.LagKnown || lagging.TotalLag <= 0 {
		t.Errorf("billing-worker lag should be known and positive, got known=%v lag=%d",
			lagging.LagKnown, lagging.TotalLag)
	}
}

func TestDescribeConsumerGroupIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	group, offsets, err := client.DescribeConsumerGroup(context.Background(), "billing-worker")
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}
	if len(offsets) == 0 {
		t.Fatal("no partition offsets reported")
	}

	var withLag int
	for _, o := range offsets {
		if o.Lag > 0 {
			withLag++
		}
		// lag = end - current, so it can never exceed the end offset.
		if o.Lag > o.NewestOffset {
			t.Errorf("%s p%d: lag %d exceeds the end offset %d",
				o.Topic, o.Partition, o.Lag, o.NewestOffset)
		}
	}
	t.Logf("group=%s state=%s partitions=%d lagging=%d total lag=%d",
		group.Name, group.State, len(offsets), withLag, group.TotalLag)

	if withLag == 0 {
		t.Error("billing-worker should have partitions behind the end")
	}
}

func TestTopicSizesIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	sizes, err := client.TopicSizes(context.Background())
	if err != nil {
		t.Fatalf("TopicSizes: %v", err)
	}
	if len(sizes) == 0 {
		t.Fatal("no topic sizes reported")
	}
	if sizes["orders.events"] <= 0 {
		t.Errorf("orders.events has 500 records, so its size should be positive, got %d",
			sizes["orders.events"])
	}
	t.Logf("orders.events on disk: %d bytes across all brokers", sizes["orders.events"])
}

func TestReadRecordsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	t.Run("from the beginning, bounded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err := client.ReadRecords(ctx, ReadOptions{
			Topics: []string{"orders.events"},
			Start:  Start{Kind: StartBeginning},
		})
		if err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}

		var records []Record
		for batch := range session.Records {
			records = append(records, batch.Records...)
		}
		if err := session.Err(); err != nil {
			t.Fatalf("read failed: %v", err)
		}

		// docker/init.sh produces exactly 500 records into orders.events.
		if len(records) != 500 {
			t.Errorf("read %d records, want the 500 produced by init.sh", len(records))
		}
		t.Logf("read %d records without following", len(records))

		// Payloads must survive intact.
		for _, r := range records[:1] {
			if len(r.Key) == 0 {
				t.Error("records should carry their key")
			}
			if !strings.Contains(string(r.Value), `"status"`) {
				t.Errorf("value looks wrong: %s", r.Value)
			}
			if r.Timestamp.IsZero() {
				t.Error("records should carry a timestamp")
			}
		}
	})

	t.Run("limit stops the read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err := client.ReadRecords(ctx, ReadOptions{
			Topics: []string{"orders.events"},
			Start:  Start{Kind: StartBeginning},
			Limit:  25,
		})
		if err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}

		var count int
		for batch := range session.Records {
			count += len(batch.Records)
		}
		// Batching means the limit can be reached mid-batch, so a few extra are
		// acceptable; what matters is that it stopped early.
		if count < 25 || count > 60 {
			t.Errorf("read %d records for a limit of 25", count)
		}
		t.Logf("limit 25 delivered %d records", count)
	})

	t.Run("last N per partition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err := client.ReadRecords(ctx, ReadOptions{
			Topics: []string{"orders.events"},
			Start:  Start{Kind: StartLastN, N: 2},
		})
		if err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}

		var count int
		for batch := range session.Records {
			count += len(batch.Records)
		}
		// 12 partitions, at most 2 each.
		if count == 0 || count > 24 {
			t.Errorf("read %d records, want at most 2 per partition across 12 partitions", count)
		}
		t.Logf("last 2 per partition delivered %d records", count)
	})

	t.Run("multiple topics at once", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err := client.ReadRecords(ctx, ReadOptions{
			Topics: []string{"orders.events", "payments.tx"},
			Start:  Start{Kind: StartLastN, N: 3},
		})
		if err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}

		seen := make(map[string]int)
		for batch := range session.Records {
			for _, r := range batch.Records {
				seen[r.Topic]++
			}
		}
		if len(seen) != 2 {
			t.Errorf("read from %d topics, want both: %v", len(seen), seen)
		}
		t.Logf("multi-topic read: %v", seen)
	})

	t.Run("cancellation stops a following read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err := client.ReadRecords(ctx, ReadOptions{
			Topics: []string{"orders.events"},
			Start:  Start{Kind: StartBeginning},
			Follow: true, // would otherwise run forever
		})
		if err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}

		// Take one batch, then stop.
		<-session.Records
		session.Stop()

		// The channel must close promptly rather than leaking goroutines.
		done := make(chan struct{})
		go func() {
			for range session.Records { //nolint:revive // draining
			}
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the session did not stop within 10s of Stop()")
		}
	})
}

// TestReadDoesNotCommitOffsets is the safety property that lets kafman be
// pointed at production: reading must never move a consumer group's offsets.
func TestReadDoesNotCommitOffsets(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	before, _, err := client.DescribeConsumerGroup(context.Background(), "billing-worker")
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := client.ReadRecords(ctx, ReadOptions{
		Topics: []string{"orders.events"},
		Start:  Start{Kind: StartBeginning},
	})
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	for range session.Records { //nolint:revive // draining
	}

	after, _, err := client.DescribeConsumerGroup(context.Background(), "billing-worker")
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}

	if before.TotalLag != after.TotalLag {
		t.Errorf("reading changed billing-worker's lag from %d to %d — offsets were committed",
			before.TotalLag, after.TotalLag)
	}
	t.Logf("group lag unchanged at %d after reading the whole topic", after.TotalLag)
}

func TestProduceIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "kafman-test-" + time.Now().Format("150405.000")
	value := `{"id":"` + key + `","status":"PRODUCED_BY_TEST"}`

	result, err := client.Produce(ctx, ProduceRequest{
		Topic: "dlq.orders",
		Key:   []byte(key),
		Value: []byte(value),
		Headers: []Header{
			{Key: "source", Value: "kafman-test"},
			{Key: "trace-id", Value: "abc123"},
		},
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	t.Logf("published to %s partition %d at offset %d", result.Topic, result.Partition, result.Offset)

	// The record must come back exactly as it went in.
	session, err := client.ReadRecords(ctx, ReadOptions{
		Topics: []string{"dlq.orders"},
		Start:  Start{Kind: StartBeginning},
	})
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}

	var found *Record
	for batch := range session.Records {
		for i, record := range batch.Records {
			if string(record.Key) == key {
				found = &batch.Records[i]
			}
		}
	}
	if found == nil {
		t.Fatal("the published record was not read back")
	}

	if string(found.Value) != value {
		t.Errorf("value = %s, want %s", found.Value, value)
	}
	if found.Partition != result.Partition {
		t.Errorf("read from partition %d, published to %d", found.Partition, result.Partition)
	}
	if found.Offset != result.Offset {
		t.Errorf("read at offset %d, published at %d", found.Offset, result.Offset)
	}

	headers := make(map[string]string)
	for _, h := range found.Headers {
		headers[h.Key] = h.Value
	}
	if headers["source"] != "kafman-test" || headers["trace-id"] != "abc123" {
		t.Errorf("headers = %v, want both to survive the round trip", headers)
	}
}

// TestProducePartitioningMatchesKafka publishes with a key and checks the broker
// put it where Kafka's own partitioner would.
func TestProducePartitioningMatchesKafka(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// dlq.orders has 4 partitions.
	partitions, err := client.DescribePartitions(ctx, "dlq.orders", false)
	if err != nil {
		t.Fatal(err)
	}
	count := int32(len(partitions))

	for _, key := range []string{"ord-99f", "user-42", "kafka"} {
		want := (murmur2([]byte(key)) & 0x7fffffff) % count

		result, err := client.Produce(ctx, ProduceRequest{
			Topic: "dlq.orders",
			Key:   []byte(key + "-" + time.Now().Format("150405.000000")),
			Value: []byte(`{"t":"partitioning"}`),
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		// The keys are unique per run, so only the routing rule is checked here
		// rather than a fixed partition.
		_ = want
		if result.Partition < 0 || result.Partition >= count {
			t.Errorf("record landed on partition %d, outside 0..%d", result.Partition, count-1)
		}
	}

	// The same key must always route to the same partition.
	const stable = "stable-key"
	var first int32 = -1
	for range 3 {
		result, err := client.Produce(ctx, ProduceRequest{
			Topic: "dlq.orders", Key: []byte(stable), Value: []byte("{}"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if first < 0 {
			first = result.Partition
			continue
		}
		if result.Partition != first {
			t.Errorf("the same key reached partitions %d and %d", first, result.Partition)
		}
	}
	want := (murmur2([]byte(stable)) & 0x7fffffff) % count
	if first != want {
		t.Errorf("key %q landed on partition %d, Kafka's partitioner would choose %d",
			stable, first, want)
	}
	t.Logf("key %q consistently routed to partition %d, matching murmur2", stable, first)
}

// TestTopicAdminIntegration exercises the full lifecycle of a topic against the
// real cluster: create, configure, grow, truncate, delete.
func TestTopicAdminIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	name := "kafman-admin-test-" + time.Now().Format("150405")
	t.Cleanup(func() {
		// Whatever the test does, the cluster is left as it was found.
		_ = client.DeleteTopic(context.Background(), name)
	})

	t.Run("create", func(t *testing.T) {
		err := client.CreateTopic(ctx, TopicSpec{
			Name: name, Partitions: 3, ReplicationFactor: 2,
			Configs: map[string]string{
				"cleanup.policy": "delete",
				"retention.ms":   "86400000",
			},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		// Metadata reaches brokers at slightly different times, so the listing
		// can lag a successful creation by a moment.
		var found *Topic
		for range 20 {
			topics, err := client.ListTopics(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			for i, topic := range topics {
				if topic.Name == name {
					found = &topics[i]
				}
			}
			if found != nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if found == nil {
			t.Fatal("the created topic is not in the list")
		}
		if found.Partitions != 3 {
			t.Errorf("partitions = %d, want 3", found.Partitions)
		}
		if found.ReplicationFactor != 2 {
			t.Errorf("replication factor = %d, want 2", found.ReplicationFactor)
		}
		t.Logf("created %s: %d partitions, RF %d", name, found.Partitions, found.ReplicationFactor)
	})

	t.Run("config carries the overrides", func(t *testing.T) {
		entries, err := client.TopicConfig(ctx, name, false)
		if err != nil {
			t.Fatalf("TopicConfig: %v", err)
		}

		values := make(map[string]string)
		for _, entry := range entries {
			values[entry.Name] = entry.Value
			if entry.Default {
				t.Errorf("%s came back as a default although defaults were not requested", entry.Name)
			}
		}
		if values["cleanup.policy"] != "delete" {
			t.Errorf("cleanup.policy = %q, want delete", values["cleanup.policy"])
		}
		if values["retention.ms"] != "86400000" {
			t.Errorf("retention.ms = %q, want 86400000", values["retention.ms"])
		}
		t.Logf("%d explicit overrides", len(entries))

		// With defaults, the list is much longer.
		withDefaults, err := client.TopicConfig(ctx, name, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(withDefaults) <= len(entries) {
			t.Error("asking for defaults should return more entries")
		}
		t.Logf("%d entries including defaults", len(withDefaults))
	})

	t.Run("change a config", func(t *testing.T) {
		if err := client.SetTopicConfig(ctx, name, "retention.ms", "3600000"); err != nil {
			t.Fatalf("SetTopicConfig: %v", err)
		}

		var settled bool
		for range 20 {
			entries, err := client.TopicConfig(ctx, name, false)
			if err != nil {
				t.Fatal(err)
			}
			values := make(map[string]string)
			for _, entry := range entries {
				values[entry.Name] = entry.Value
			}
			if values["retention.ms"] == "3600000" {
				settled = true
				// IncrementalAlterConfig must not have wiped the other override.
				if values["cleanup.policy"] != "delete" {
					t.Errorf("cleanup.policy = %q; changing one setting must not reset another",
						values["cleanup.policy"])
				}
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !settled {
			t.Error("retention.ms never took the new value")
		}
	})

	t.Run("reset a config", func(t *testing.T) {
		if err := client.ResetTopicConfig(ctx, name, "retention.ms"); err != nil {
			t.Fatalf("ResetTopicConfig: %v", err)
		}

		// Config changes propagate asynchronously, so the override can survive a
		// read taken immediately afterwards.
		var last []ConfigEntry
		for range 20 {
			entries, err := client.TopicConfig(ctx, name, false)
			if err != nil {
				t.Fatal(err)
			}
			last = entries

			stillThere := false
			for _, entry := range entries {
				if entry.Name == "retention.ms" {
					stillThere = true
				}
			}
			if !stillThere {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Errorf("retention.ms is still an override after reset: %v", last)
	})

	t.Run("add partitions", func(t *testing.T) {
		if err := client.AddPartitions(ctx, name, 6); err != nil {
			t.Fatalf("AddPartitions: %v", err)
		}

		partitions, err := client.DescribePartitions(ctx, name, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(partitions) != 6 {
			t.Errorf("got %d partitions, want 6", len(partitions))
		}

		// Kafka cannot shrink a topic, and the error should say so plainly.
		err = client.AddPartitions(ctx, name, 2)
		if err == nil {
			t.Error("reducing the partition count should be refused")
		} else if !strings.Contains(err.Error(), "cannot reduce") {
			t.Errorf("error = %v, want it to explain that Kafka cannot reduce the count", err)
		}
	})

	t.Run("truncate", func(t *testing.T) {
		for i := range 5 {
			_, err := client.Produce(ctx, ProduceRequest{
				Topic: name, Key: []byte("k"), Value: []byte(`{"n":` + itoa64(int64(i)) + `}`),
			})
			if err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}

		before, err := client.DescribePartitions(ctx, name, true)
		if err != nil {
			t.Fatal(err)
		}
		var retained int64
		for _, p := range before {
			retained += p.Messages()
		}
		if retained == 0 {
			t.Fatal("nothing was produced to truncate")
		}

		if err := client.TruncateTopic(ctx, name); err != nil {
			t.Fatalf("TruncateTopic: %v", err)
		}

		after, err := client.DescribePartitions(ctx, name, true)
		if err != nil {
			t.Fatal(err)
		}
		var left int64
		for _, p := range after {
			left += p.Messages()
		}
		if left != 0 {
			t.Errorf("%d records remain after truncating", left)
		}
		t.Logf("truncated %d records; the partitions remain", retained)
	})

	t.Run("delete", func(t *testing.T) {
		if err := client.DeleteTopic(ctx, name); err != nil {
			t.Fatalf("DeleteTopic: %v", err)
		}

		// Deletion is asynchronous, so give the controller a moment.
		var gone bool
		for range 20 {
			topics, err := client.ListTopics(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			gone = true
			for _, topic := range topics {
				if topic.Name == name {
					gone = false
				}
			}
			if gone {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if !gone {
			t.Error("the topic is still listed after being deleted")
		}
	})
}

// TestDeleteRecordsOnCompactedTopic checks that the broker's opaque policy error
// is turned into something a user can act on.
func TestDeleteRecordsOnCompactedTopic(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// user.profile is created with cleanup.policy=compact by docker/init.sh.
	err = client.TruncateTopic(ctx, "user.profile")
	if err == nil {
		t.Skip("the broker allowed it; nothing to check")
	}
	if !strings.Contains(err.Error(), "compacted topic") {
		t.Errorf("error = %v\nwant it to explain that compaction is the reason", err)
	}
	t.Logf("refusal explained as: %v", err)
}

// TestOffsetResetIntegration is the acceptance test of the consumer-group stage:
// a group is moved back to the start of a topic and then actually re-reads the
// records it had already consumed.
func TestOffsetResetIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const records = 30
	stamp := time.Now().Format("150405")
	topic := "kafman-reset-test-" + stamp
	group := "kafman-reset-group-" + stamp

	t.Cleanup(func() {
		_ = client.DeleteConsumerGroup(context.Background(), group)
		_ = client.DeleteTopic(context.Background(), topic)
	})

	if err := client.CreateTopic(ctx, TopicSpec{
		Name: topic, Partitions: 3, ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for i := range records {
		if _, err := client.Produce(ctx, ProduceRequest{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("k%02d", i)),
			Value: []byte(fmt.Sprintf(`{"seq":%d}`, i)),
		}); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// A group is born the moment offsets are committed for it, so seeding it at
	// the end of the topic is both the setup and the first reset.
	t.Run("seed at the end", func(t *testing.T) {
		changes, err := client.PreviewOffsetReset(ctx, ResetSpec{
			Group: group, Topics: []string{topic}, Kind: ResetToLatest,
		})
		if err != nil {
			t.Fatalf("PreviewOffsetReset: %v", err)
		}
		if len(changes) != 3 {
			t.Fatalf("preview covers %d partitions, want 3", len(changes))
		}

		var total int64
		for _, change := range changes {
			if change.Current != -1 {
				t.Errorf("%s p%d: current is %d, want -1 for a group that never committed",
					change.Topic, change.Partition, change.Current)
			}
			total += change.New
		}
		if total != records {
			t.Errorf("the end offsets add up to %d, want %d", total, records)
		}

		if err := client.ApplyOffsetReset(ctx, group, changes); err != nil {
			t.Fatalf("ApplyOffsetReset: %v", err)
		}

		group, offsets, err := client.DescribeConsumerGroup(ctx, group)
		if err != nil {
			t.Fatalf("DescribeConsumerGroup: %v", err)
		}
		if group.TotalLag != 0 {
			t.Errorf("lag after seeding at the end is %d, want 0", group.TotalLag)
		}
		if len(offsets) != 3 {
			t.Errorf("the group has offsets for %d partitions, want 3", len(offsets))
		}
	})

	t.Run("preview changes nothing", func(t *testing.T) {
		before := committedOffsets(t, ctx, client, group)

		if _, err := client.PreviewOffsetReset(ctx, ResetSpec{
			Group: group, Topics: []string{topic}, Kind: ResetToEarliest,
		}); err != nil {
			t.Fatalf("PreviewOffsetReset: %v", err)
		}

		after := committedOffsets(t, ctx, client, group)
		if !reflect.DeepEqual(before, after) {
			t.Errorf("a preview moved the offsets: %v → %v", before, after)
		}
	})

	t.Run("reset to earliest and re-read", func(t *testing.T) {
		changes, err := client.PreviewOffsetReset(ctx, ResetSpec{
			Group: group, Topics: []string{topic}, Kind: ResetToEarliest,
		})
		if err != nil {
			t.Fatalf("PreviewOffsetReset: %v", err)
		}
		for _, change := range changes {
			if change.New != 0 {
				t.Errorf("%s p%d: new offset is %d, want the start of the partition",
					change.Topic, change.Partition, change.New)
			}
		}
		if err := client.ApplyOffsetReset(ctx, group, changes); err != nil {
			t.Fatalf("ApplyOffsetReset: %v", err)
		}

		described, _, err := client.DescribeConsumerGroup(ctx, group)
		if err != nil {
			t.Fatalf("DescribeConsumerGroup: %v", err)
		}
		if described.TotalLag != records {
			t.Errorf("lag after the reset is %d, want %d", described.TotalLag, records)
		}

		// The point of the whole stage: a real consumer of the group reads the
		// records again.
		got := consumeWithGroup(t, ctx, group, topic, records)
		if got != records {
			t.Errorf("the group re-read %d records, want %d", got, records)
		}
		t.Logf("the group re-read %d records after the reset", got)
	})

	t.Run("shift forward", func(t *testing.T) {
		changes, err := client.PreviewOffsetReset(ctx, ResetSpec{
			Group: group, Topics: []string{topic}, Kind: ResetShiftBy, Shift: 2,
		})
		if err != nil {
			t.Fatalf("PreviewOffsetReset: %v", err)
		}
		for _, change := range changes {
			if change.Current < 0 {
				t.Fatalf("%s p%d has no committed offset to shift from",
					change.Topic, change.Partition)
			}
			if want := min(change.Current+2, change.Newest); change.New != want {
				t.Errorf("%s p%d: %d + 2 landed on %d, want %d",
					change.Topic, change.Partition, change.Current, change.New, want)
			}
		}
	})

	t.Run("clear the offsets of a topic", func(t *testing.T) {
		if err := client.DeleteGroupOffsets(ctx, group, topic); err != nil {
			t.Fatalf("DeleteGroupOffsets: %v", err)
		}

		_, offsets, err := client.DescribeConsumerGroup(ctx, group)
		if err != nil {
			t.Fatalf("DescribeConsumerGroup: %v", err)
		}
		for _, offset := range offsets {
			if offset.Topic == topic {
				t.Errorf("%s p%d still has a committed offset %d",
					offset.Topic, offset.Partition, offset.Current)
			}
		}
	})

	t.Run("delete the group", func(t *testing.T) {
		// Clearing the last of a group's offsets makes Kafka drop the group
		// itself, so it has to be seeded again before there is anything to
		// delete.
		changes, err := client.PreviewOffsetReset(ctx, ResetSpec{
			Group: group, Topics: []string{topic}, Kind: ResetToLatest,
		})
		if err != nil {
			t.Fatalf("PreviewOffsetReset: %v", err)
		}
		if err := client.ApplyOffsetReset(ctx, group, changes); err != nil {
			t.Fatalf("ApplyOffsetReset: %v", err)
		}

		if err := client.DeleteConsumerGroup(ctx, group); err != nil {
			t.Fatalf("DeleteConsumerGroup: %v", err)
		}

		groups, err := client.ListConsumerGroups(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range groups {
			if g.Name == group {
				t.Error("the group is still listed after being deleted")
			}
		}
	})
}

// TestResetRefusedWhileTheGroupIsLive checks the refusal a person is most likely
// to meet, and that it says what to do about it.
func TestResetRefusedWhileTheGroupIsLive(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stamp := time.Now().Format("150405")
	topic := "kafman-live-test-" + stamp
	group := "kafman-live-group-" + stamp

	t.Cleanup(func() {
		_ = client.DeleteConsumerGroup(context.Background(), group)
		_ = client.DeleteTopic(context.Background(), topic)
	})

	if err := client.CreateTopic(ctx, TopicSpec{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := client.Produce(ctx, ProduceRequest{
		Topic: topic, Value: []byte(`{"seq":0}`),
	}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// A consumer that stays in the group for the duration of the attempt.
	consumer, err := sarama.NewConsumerGroup(cluster(t, "local").Brokers, group, groupConfig(t))
	if err != nil {
		t.Fatalf("NewConsumerGroup: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	joined := make(chan struct{})
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		handler := &countingHandler{ready: joined}
		for consumeCtx.Err() == nil {
			if err := consumer.Consume(consumeCtx, []string{topic}, handler); err != nil {
				return
			}
		}
	}()

	select {
	case <-joined:
	case <-time.After(30 * time.Second):
		t.Fatal("the consumer never joined the group")
	}

	changes := []OffsetChange{{Topic: topic, Partition: 0, Current: 0, New: 0, Newest: 1}}
	err = client.ApplyOffsetReset(ctx, group, changes)
	if err == nil {
		t.Fatal("resetting a live group should be refused")
	}
	if !strings.Contains(err.Error(), "stop its consumers") {
		t.Errorf("error = %v\nwant it to say the consumers have to be stopped", err)
	}
	t.Logf("refusal explained as: %v", err)
}

// committedOffsets reads the group's positions as a comparable map.
func committedOffsets(t *testing.T, ctx context.Context, client *Client, group string) map[string]int64 {
	t.Helper()

	_, offsets, err := client.DescribeConsumerGroup(ctx, group)
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}

	out := make(map[string]int64, len(offsets))
	for _, offset := range offsets {
		out[fmt.Sprintf("%s/%d", offset.Topic, offset.Partition)] = offset.Current
	}
	return out
}

// consumeWithGroup joins the group as a real consumer and counts what it gets,
// which is the only way to prove a reset actually took effect.
func consumeWithGroup(t *testing.T, ctx context.Context, group, topic string, want int) int {
	t.Helper()

	consumer, err := sarama.NewConsumerGroup(cluster(t, "local").Brokers, group, groupConfig(t))
	if err != nil {
		t.Fatalf("NewConsumerGroup: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	consumeCtx, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()

	handler := &countingHandler{want: want, done: make(chan struct{})}
	go func() {
		for consumeCtx.Err() == nil {
			if err := consumer.Consume(consumeCtx, []string{topic}, handler); err != nil {
				return
			}
		}
	}()

	select {
	case <-handler.done:
	case <-consumeCtx.Done():
	}
	return handler.count()
}

func groupConfig(t *testing.T) *sarama.Config {
	t.Helper()

	conf, err := BuildSaramaConfig(cluster(t, "local"))
	if err != nil {
		t.Fatalf("BuildSaramaConfig: %v", err)
	}
	// The test consumer is a normal Kafka client, not kafman: it starts where the
	// group's committed offsets say and commits as it goes.
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Offsets.AutoCommit.Enable = true
	return conf
}

// countingHandler counts the records a consumer group session delivers.
type countingHandler struct {
	want  int
	done  chan struct{}
	ready chan struct{}

	mu        sync.Mutex
	seen      int
	closeOnce sync.Once
	readyOnce sync.Once
}

func (h *countingHandler) Setup(sarama.ConsumerGroupSession) error {
	if h.ready != nil {
		h.readyOnce.Do(func() { close(h.ready) })
	}
	return nil
}

func (h *countingHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *countingHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkMessage(message, "")

		h.mu.Lock()
		h.seen++
		reached := h.want > 0 && h.seen >= h.want
		h.mu.Unlock()

		if reached && h.done != nil {
			h.closeOnce.Do(func() { close(h.done) })
			return nil
		}
	}
	return nil
}

func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen
}

// TestScramUserLifecycleIntegration is the acceptance test of the user half of
// the stage: a user created from kafman can actually authenticate against the
// stack's SASL listener, and stops being able to once it is deleted.
func TestScramUserLifecycleIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	name := "kafman-user-" + time.Now().Format("150405")
	const password = "first-secret"

	t.Cleanup(func() {
		_ = client.DeleteScramUser(context.Background(), name, nil)
	})

	t.Run("create", func(t *testing.T) {
		if err := client.UpsertScramUser(ctx, ScramUserSpec{
			Name: name, Mechanism: ScramSHA512, Password: password,
		}); err != nil {
			t.Fatalf("UpsertScramUser: %v", err)
		}

		user := findUser(t, ctx, client, name)
		if user == nil {
			t.Fatal("the created user is not in the list")
		}
		if !user.Has(ScramSHA512) {
			t.Errorf("mechanisms = %v, want SCRAM-SHA-512", user.Mechanisms())
		}
		if got := user.Credentials[0].Iterations; got != DefaultScramIterations {
			t.Errorf("iterations = %d, want the default %d", got, DefaultScramIterations)
		}
	})

	// This is the criterion the stage is finished by: the credential works.
	t.Run("the user can connect to the SASL listener", func(t *testing.T) {
		result := CheckConnectivity(ctx, saslCluster(t, name, password, "scram-sha-512"))
		if !result.OK() {
			t.Fatalf("the created user cannot authenticate: %v\n%s", result.Err, result.Diagnosis)
		}
		t.Logf("%s authenticated over SASL_PLAINTEXT in %s", name, result.Latency)
	})

	t.Run("the old password stops working after a change", func(t *testing.T) {
		const changed = "second-secret"
		if err := client.UpsertScramUser(ctx, ScramUserSpec{
			Name: name, Mechanism: ScramSHA512, Password: changed,
		}); err != nil {
			t.Fatalf("UpsertScramUser: %v", err)
		}

		// Neither direction of a password change is instant: the broker that
		// answers the handshake applies the new credential a moment after the
		// controller accepts it, and keeps accepting the old one until then.
		// Polling is the honest assertion.
		waitForAuth(ctx, t, name, changed, true)
		waitForAuth(ctx, t, name, password, false)
	})

	t.Run("too few iterations are refused", func(t *testing.T) {
		err := client.UpsertScramUser(ctx, ScramUserSpec{
			Name: name, Mechanism: ScramSHA512, Password: password, Iterations: 1024,
		})
		if err == nil {
			t.Fatal("fewer iterations than Kafka's minimum should be refused")
		}
		if !strings.Contains(err.Error(), "4096") {
			t.Errorf("error = %v\nwant it to name the minimum", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		if err := client.DeleteScramUser(ctx, name, nil); err != nil {
			t.Fatalf("DeleteScramUser: %v", err)
		}
		if findUser(t, ctx, client, name) != nil {
			t.Error("the user is still listed after being deleted")
		}

		// And the credential is genuinely gone, not merely hidden from the list.
		waitForAuth(ctx, t, name, "second-secret", false)
	})

	t.Run("deleting it twice says so", func(t *testing.T) {
		err := client.DeleteScramUser(ctx, name, nil)
		if err == nil {
			t.Fatal("deleting a user that is gone should be reported")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("error = %v\nwant it to say the user does not exist", err)
		}
	})
}

// TestACLLifecycleIntegration is the acceptance test of the ACL half: a rule
// created from kafman actually restricts what a principal can do.
func TestACLLifecycleIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	stamp := time.Now().Format("150405")
	user := "kafman-acl-user-" + stamp
	topic := "kafman-acl-topic-" + stamp
	const password = "acl-secret"

	t.Cleanup(func() {
		_ = client.DeleteScramUser(context.Background(), user, nil)
		_ = client.DeleteTopic(context.Background(), topic)
	})

	if err := client.CreateTopic(ctx, TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := client.Produce(ctx, ProduceRequest{Topic: topic, Value: []byte(`{"seq":0}`)}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if err := client.UpsertScramUser(ctx, ScramUserSpec{
		Name: user, Mechanism: ScramSHA512, Password: password,
	}); err != nil {
		t.Fatalf("UpsertScramUser: %v", err)
	}

	allow := ACL{
		Principal: "User:" + user, Host: "*", Operation: "Read", Permission: "Allow",
		ResourceType: "Topic", ResourceName: topic, PatternType: "Literal",
	}
	deny := allow
	deny.Permission = "Deny"

	t.Cleanup(func() {
		_, _ = client.DeleteACL(context.Background(), allow)
		_, _ = client.DeleteACL(context.Background(), deny)
	})

	// The stack's own rules already cover every topic, so "allow everyone if no
	// ACL is found" does not apply here: a new principal has to be granted the
	// read before it can be denied it.
	t.Run("an allow rule opens the read", func(t *testing.T) {
		if err := client.CreateACL(ctx, allow); err != nil {
			t.Fatalf("CreateACL: %v", err)
		}
		waitForRead(ctx, t, user, password, topic, true)
	})

	t.Run("create", func(t *testing.T) {
		if err := client.CreateACL(ctx, deny); err != nil {
			t.Fatalf("CreateACL: %v", err)
		}

		acls, err := client.ListACLs(ctx, ACLFilter{Principal: deny.Principal})
		if err != nil {
			t.Fatalf("ListACLs: %v", err)
		}
		if len(acls) != 2 {
			t.Fatalf("the filter should find the allow and the deny, got %d: %+v", len(acls), acls)
		}
		// Denials sort first: they are the rules that change what is possible.
		if got := acls[0]; !got.Denies() || got.Operation != "Read" || got.ResourceName != topic {
			t.Errorf("acl = %+v, want the denial that was created", got)
		}
	})

	// This is the criterion the stage is finished by: the rule bites. A denial
	// overrides the allowance that was just proven to work.
	t.Run("the deny rule stops the read", func(t *testing.T) {
		waitForRead(ctx, t, user, password, topic, false)
	})

	t.Run("delete", func(t *testing.T) {
		removed, err := client.DeleteACL(ctx, deny)
		if err != nil {
			t.Fatalf("DeleteACL: %v", err)
		}
		if removed != 1 {
			t.Errorf("removed %d rules, want exactly the one", removed)
		}

		acls, err := client.ListACLs(ctx, ACLFilter{Principal: deny.Principal})
		if err != nil {
			t.Fatal(err)
		}
		for _, acl := range acls {
			if acl.Denies() {
				t.Errorf("the denial is still listed: %+v", acl)
			}
		}

		// Access comes back, which is what makes removing a denial worth a
		// confirmation in the UI.
		waitForRead(ctx, t, user, password, topic, true)
	})
}

// TestACLPatternTypeIsExactOnDelete checks that deleting a literal rule leaves
// the prefixed rule of the same name alone.
func TestACLPatternTypeIsExactOnDelete(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	principal := "User:kafman-pattern-" + time.Now().Format("150405")
	base := ACL{
		Principal: principal, Host: "*", Operation: "Read", Permission: "Allow",
		ResourceType: "Group", ResourceName: "kafman-pattern-group", PatternType: "Literal",
	}
	prefixed := base
	prefixed.PatternType = "Prefixed"

	t.Cleanup(func() {
		_, _ = client.DeleteACL(context.Background(), base)
		_, _ = client.DeleteACL(context.Background(), prefixed)
	})

	for _, acl := range []ACL{base, prefixed} {
		if err := client.CreateACL(ctx, acl); err != nil {
			t.Fatalf("CreateACL %s: %v", acl.PatternType, err)
		}
	}

	if removed, err := client.DeleteACL(ctx, base); err != nil || removed != 1 {
		t.Fatalf("DeleteACL removed %d rules, err %v; want exactly one", removed, err)
	}

	acls, err := client.ListACLs(ctx, ACLFilter{Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	if len(acls) != 1 || acls[0].PatternType != "Prefixed" {
		t.Errorf("remaining rules = %+v, want only the prefixed one", acls)
	}
}

// saslCluster builds a cluster definition that authenticates as the given user
// against the stack's SASL_PLAINTEXT listener.
func saslCluster(t *testing.T, user, password, mechanism string) *config.Cluster {
	t.Helper()

	base := cluster(t, "local-sasl-scram")
	copied := *base
	copied.Name = "as-" + user
	copied.SASL = config.SASL{
		Enabled:   true,
		Mechanism: mechanism,
		Username:  user,
		Password:  password,
	}
	return &copied
}

// waitForAuth polls until authenticating with the given password succeeds or
// fails as expected.
func waitForAuth(ctx context.Context, t *testing.T, user, password string, want bool) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for {
		result := CheckConnectivity(ctx, saslCluster(t, user, password, "scram-sha-512"))
		if result.OK() == want {
			return
		}
		if time.Now().After(deadline) {
			if want {
				t.Fatalf("the credential should have been accepted within 20s: %v", result.Err)
			}
			t.Fatal("the credential should have stopped working within 20s")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitForRead polls until reading the topic as the user is allowed or refused
// as expected.
//
// A rule is visible in ListAcls as soon as the controller has written it, but
// each broker applies it to its own authorizer a moment later. Asserting the
// effect immediately would be asserting a promise Kafka does not make.
func waitForRead(ctx context.Context, t *testing.T, user, password, topic string, allowed bool) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	var last error
	for {
		last = readOneAs(ctx, t, user, password, topic)
		if (last == nil) == allowed {
			if !allowed {
				t.Logf("the read was refused as: %v", last)
			}
			return
		}
		if time.Now().After(deadline) {
			if allowed {
				t.Fatalf("the read should have been allowed within 20s: %v", last)
			}
			t.Fatal("the read should have been refused within 20s")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// readOneAs connects as the given user and tries to read the topic, which is
// what an ACL either permits or refuses.
func readOneAs(ctx context.Context, t *testing.T, user, password, topic string) error {
	t.Helper()

	client, err := New(saslCluster(t, user, password, "scram-sha-512"))
	if err != nil {
		return err
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them — which is
	// how test topics, users and rules used to survive their own tests.
	t.Cleanup(func() { _ = client.Close() })

	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	session, err := client.ReadRecords(readCtx, ReadOptions{
		Topics: []string{topic},
		Start:  Start{Kind: StartBeginning},
	})
	if err != nil {
		return err
	}
	for range session.Records { //nolint:revive // draining is the point
	}
	return session.Err()
}

// findUser looks one user up in the listing.
func findUser(t *testing.T, ctx context.Context, client *Client, name string) *ScramUser {
	t.Helper()

	users, err := client.ListScramUsers(ctx)
	if err != nil {
		t.Fatalf("ListScramUsers: %v", err)
	}
	for i, user := range users {
		if user.Name == name {
			return &users[i]
		}
	}
	return nil
}

// TestTopicEndOffsetsIntegration checks the source of every message rate: the
// sum of end offsets, and how it moves when records are written.
func TestTopicEndOffsetsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Registered before the cleanups that use this client: t.Cleanup runs in
	// reverse order, and a deferred Close would run before any of them.
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := "kafman-rate-test-" + time.Now().Format("150405")
	t.Cleanup(func() { _ = client.DeleteTopic(context.Background(), topic) })

	if err := client.CreateTopic(ctx, TopicSpec{
		Name: topic, Partitions: 3, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// A partition whose leader is still being elected cannot answer, and the
	// reading honestly leaves it out rather than reporting zero; the baseline is
	// taken once the topic can actually be measured.
	var before map[string]int64
	for range 20 {
		if before, err = client.TopicEndOffsets(ctx, false); err != nil {
			t.Fatalf("TopicEndOffsets: %v", err)
		}
		if _, ok := before[topic]; ok {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, ok := before[topic]; !ok {
		t.Fatal("the new topic never became measurable")
	}
	// Internal topics are excluded when not asked for; __consumer_offsets is
	// always busy and would drown a per-topic rate.
	for name := range before {
		if strings.HasPrefix(name, "_") {
			t.Errorf("internal topic %s should have been filtered out", name)
		}
	}

	const records = 25
	for i := range records {
		if _, err := client.Produce(ctx, ProduceRequest{
			Topic: topic, Value: []byte(fmt.Sprintf(`{"seq":%d}`, i)),
		}); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	after, err := client.TopicEndOffsets(ctx, false)
	if err != nil {
		t.Fatalf("TopicEndOffsets: %v", err)
	}

	// The delta is exactly the number of records written — this is the identity
	// every rate on the Monitoring tab rests on.
	if delta := after[topic] - before[topic]; delta != records {
		t.Errorf("end offsets moved by %d, want %d", delta, records)
	}

	withInternal, err := client.TopicEndOffsets(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(withInternal) <= len(after) {
		t.Error("asking for internal topics should return more")
	}
}

// TestTopicStartOffsetsIntegration checks the retention head: starts never
// exceed ends, internal topics stay out, and end minus start is the retained
// record count.
func TestTopicStartOffsetsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ends, err := client.TopicEndOffsets(ctx, false)
	if err != nil {
		t.Fatalf("TopicEndOffsets: %v", err)
	}
	starts, err := client.TopicStartOffsets(ctx, false)
	if err != nil {
		t.Fatalf("TopicStartOffsets: %v", err)
	}

	if len(starts) == 0 {
		t.Fatal("the stack has topics; their oldest offsets should be readable")
	}
	for topic, start := range starts {
		if strings.HasPrefix(topic, "_") {
			t.Errorf("internal topic %s should have been filtered out", topic)
		}
		end, ok := ends[topic]
		if !ok {
			continue
		}
		if start > end {
			t.Errorf("%s: oldest %d beyond end %d — retained would be negative", topic, start, end)
		}
	}

	// The fixture topics were produced to and never truncated, so at least one
	// retains records.
	retainedSomewhere := false
	for topic, end := range ends {
		if end-starts[topic] > 0 {
			retainedSomewhere = true
			break
		}
	}
	if !retainedSomewhere {
		t.Error("every topic reads as empty — the retention head is wrong")
	}
}

// TestGroupCommitStatsIntegration checks the consume-rate source on a real
// group: the committed sum and the never-committed count.
func TestGroupCommitStatsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	groups, err := client.ConsumerGroupsWithLag(ctx)
	if err != nil {
		t.Fatalf("ConsumerGroupsWithLag: %v", err)
	}
	if len(groups) == 0 {
		t.Skip("no consumer groups on the stack; run make stack-init")
	}

	committedSomewhere := false
	for _, group := range groups {
		if !group.CommittedKnown {
			t.Errorf("group %s: the committed sum should be known", group.Name)
			continue
		}
		if group.CommittedSum < 0 {
			t.Errorf("group %s: committed sum %d is negative", group.Name, group.CommittedSum)
		}
		if group.CommittedParts > 0 {
			committedSomewhere = true
		}
		if group.NoCommit < 0 {
			t.Errorf("group %s: NoCommit %d is negative", group.Name, group.NoCommit)
		}
		t.Logf("group %s: committed %d over %d partitions, %d without a commit",
			group.Name, group.CommittedSum, group.CommittedParts, group.NoCommit)
	}
	if !committedSomewhere {
		t.Error("the fixture groups have commits; none were counted")
	}
}

func TestBrokerLoadsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loads, err := client.BrokerLoads(ctx)
	if err != nil {
		t.Fatalf("BrokerLoads: %v", err)
	}
	if len(loads) != 3 {
		t.Fatalf("got %d brokers, want the stack's 3", len(loads))
	}

	var controllers, leaders, replicas int
	for _, load := range loads {
		t.Logf("broker %d: %d leaders, %d replicas, %d out of sync, controller=%v",
			load.ID, load.Leaders, load.Replicas, load.OutOfSync, load.IsController)
		if load.IsController {
			controllers++
		}
		leaders += load.Leaders
		replicas += load.Replicas
		if load.Leaders == 0 {
			t.Errorf("broker %d leads nothing on a healthy cluster", load.ID)
		}
	}

	// Combined KRaft nodes: every broker carries the controller role, and that
	// is what IsController now means — never the metadata "controller id",
	// which on KRaft is a random broker.
	if controllers != 3 {
		t.Errorf("got %d controllers, want 3 combined nodes", controllers)
	}
	// Every partition has exactly one leader, and its replicas are spread over
	// the brokers, so the totals have to relate this way.
	if replicas <= leaders {
		t.Errorf("replicas %d should exceed leaders %d on a replicated cluster", replicas, leaders)
	}
}

// liveGroups are the groups the "live" compose profile keeps connected. Without
// them every group on the stack is Empty — committed offsets left by consumers
// that have exited — and the screens that show members, hosts and assignments
// have nothing to draw.
const (
	liveFastGroup = "live-orders-fast"
	liveSlowGroup = "live-orders-slow"
	liveTopic     = "live.orders"
	liveMembers   = 3
)

// requireLiveGroups skips a test when the live profile is not running, and says
// how to start it: a stack without it is a valid state, not a broken one.
func requireLiveGroups(t *testing.T, client *Client) {
	t.Helper()

	groups, err := client.ListConsumerGroups(context.Background())
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	for _, group := range groups {
		if group.Name == liveFastGroup && group.State == GroupStable {
			return
		}
	}
	t.Skipf("%s is not running; start it with: make stack-live", liveFastGroup)
}

// TestLiveConsumerGroupsIntegration checks what a group with live members
// reports: the state, the members with their hosts, and an assignment that
// covers the topic exactly once.
func TestLiveConsumerGroupsIntegration(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	requireLiveGroups(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	group, offsets, err := client.DescribeConsumerGroup(ctx, liveFastGroup)
	if err != nil {
		t.Fatalf("DescribeConsumerGroup: %v", err)
	}

	if group.State != GroupStable {
		t.Errorf("state = %q, want %q", group.State, GroupStable)
	}
	if group.Members != liveMembers || len(group.MemberList) != liveMembers {
		t.Fatalf("members = %d (%d described), want %d",
			group.Members, len(group.MemberList), liveMembers)
	}
	if group.Protocol == "" {
		t.Error("a group with members has agreed on an assignment strategy")
	}
	if group.Coordinator <= 0 {
		t.Errorf("coordinator = %d, want a broker id", group.Coordinator)
	}

	// Every member carries the identity a person needs to find the process.
	clients := make(map[string]bool)
	assigned := make(map[int32]string)
	for _, member := range group.MemberList {
		if member.ClientID == "" || member.Host == "" || member.MemberID == "" {
			t.Errorf("member is missing its identity: %+v", member)
		}
		clients[member.ClientID] = true

		if len(member.Assignment) == 0 {
			t.Errorf("member %s holds no partitions", member.ClientID)
		}
		for _, ref := range member.Assignment {
			if ref.Topic != liveTopic {
				t.Errorf("member %s is assigned %s, want %s", member.ClientID, ref.Topic, liveTopic)
			}
			if other, taken := assigned[ref.Partition]; taken {
				t.Errorf("partition %d is assigned to both %s and %s",
					ref.Partition, other, member.ClientID)
			}
			assigned[ref.Partition] = member.ClientID
		}
		t.Logf("%-8s %-14s %d partitions", member.ClientID, member.Host, len(member.Assignment))
	}

	for _, name := range []string{"fast-1", "fast-2", "fast-3"} {
		if !clients[name] {
			t.Errorf("member %s is missing; got %v", name, clients)
		}
	}

	// The whole topic is covered, which is what a healthy Stable group means.
	partitions, err := client.DescribePartitions(ctx, liveTopic, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(assigned) != len(partitions) {
		t.Errorf("%d of %d partitions are assigned", len(assigned), len(partitions))
	}

	// And the per-partition view knows which host holds each one.
	for _, offset := range offsets {
		if offset.Host == "" || offset.ClientID == "" {
			t.Errorf("%s p%d has no owner while the group is Stable", offset.Topic, offset.Partition)
		}
	}
}

// TestLiveLagGrowsAndDrains checks the numbers the lag column is built from
// against a group that is deliberately too slow for its topic.
func TestLiveLagGrowsAndDrains(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	requireLiveGroups(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	lagOf := func(name string) int64 {
		groups, err := client.ConsumerGroupsWithLag(ctx)
		if err != nil {
			t.Fatalf("ConsumerGroupsWithLag: %v", err)
		}
		for _, group := range groups {
			if group.Name == name {
				if !group.LagKnown {
					t.Fatalf("%s reports its lag as unknown", name)
				}
				return group.TotalLag
			}
		}
		t.Fatalf("%s is not in the listing", name)
		return 0
	}

	slowBefore, fastBefore := lagOf(liveSlowGroup), lagOf(liveFastGroup)

	// The slow group reads one record every 200 ms while the producer writes
	// about twenty a second, so its lag can only grow.
	deadline := time.Now().Add(30 * time.Second)
	var slowAfter int64
	for {
		slowAfter = lagOf(liveSlowGroup)
		if slowAfter > slowBefore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slow group's lag did not grow in 30s: %d → %d", slowBefore, slowAfter)
		}
		time.Sleep(2 * time.Second)
	}
	fastAfter := lagOf(liveFastGroup)

	t.Logf("slow %d → %d, fast %d → %d", slowBefore, slowAfter, fastBefore, fastAfter)

	// A consumer that keeps up stays close to the end of the topic; one that
	// cannot falls behind by more than a poll's worth of records.
	if fastAfter > slowAfter {
		t.Errorf("the group that keeps up (%d) is further behind than the slow one (%d)",
			fastAfter, slowAfter)
	}
}

// TestLiveTopicIsBeingWrittenTo checks the other half of the fixture: the live
// topic really is receiving records, which is what makes the rates and the lag
// move at all.
func TestLiveTopicIsBeingWrittenTo(t *testing.T) {
	client, err := New(cluster(t, "local"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	requireLiveGroups(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	before, err := client.TopicEndOffsets(ctx, false)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		after, err := client.TopicEndOffsets(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		if written := after[liveTopic] - before[liveTopic]; written > 0 {
			t.Logf("%s took %d records while the test watched", liveTopic, written)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s received nothing in 20s; is live-producer running?", liveTopic)
		}
		time.Sleep(2 * time.Second)
	}

	// The fixture topics are left alone by the live profile: their counts are
	// what other tests assert on.
	steady, err := client.TopicEndOffsets(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := steady["orders.events"]; got != 500 {
		t.Errorf("orders.events holds %d records, want the fixture's 500 — "+
			"something is writing into a topic the tests measure", got)
	}
}
