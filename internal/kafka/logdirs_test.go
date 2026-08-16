package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

// fakeLogDirsAdmin serves broker configs for the log.dirs cache.
type fakeLogDirsAdmin struct {
	sarama.ClusterAdmin
	dirs  map[string]string // broker id -> log.dirs value
	calls int
}

func (f *fakeLogDirsAdmin) DescribeConfigs(resources []*sarama.ConfigResource, _ sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	f.calls++
	out := make([]*sarama.ConfigResourceResult, 0, len(resources))
	for _, resource := range resources {
		value, ok := f.dirs[resource.Name]
		if !ok {
			out = append(out, &sarama.ConfigResourceResult{Name: resource.Name, ErrorCode: 8})
			continue
		}
		out = append(out, &sarama.ConfigResourceResult{
			Name:    resource.Name,
			Configs: []sarama.ConfigEntry{{Name: "log.dirs", Value: value}},
		})
	}
	return out, nil
}

func TestBrokerLogDirsAreCachedAndSplit(t *testing.T) {
	admin := &fakeLogDirsAdmin{dirs: map[string]string{
		"1": "/var/lib/kafka/data",
		"2": "/data/a, /data/b",
	}}
	c := &Client{admin: admin}

	dirs, err := c.brokerLogDirs([]int32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs[1]) != 1 || dirs[1][0] != "/var/lib/kafka/data" {
		t.Errorf("broker 1 dirs = %v", dirs[1])
	}
	if len(dirs[2]) != 2 || dirs[2][0] != "/data/a" || dirs[2][1] != "/data/b" {
		t.Errorf("a comma-separated log.dirs must be split and trimmed: %v", dirs[2])
	}
	if admin.calls != 1 {
		t.Fatalf("expected one batched DescribeConfigs, got %d", admin.calls)
	}

	// The second ask must come from the cache: log.dirs changes only on restart.
	if _, err := c.brokerLogDirs([]int32{1, 2}); err != nil {
		t.Fatal(err)
	}
	if admin.calls != 1 {
		t.Errorf("log.dirs should be cached, got %d calls", admin.calls)
	}
}

// An unreadable broker stays absent from the result rather than failing the
// whole describe; it is asked again next time.
func TestBrokerLogDirsBestEffortPerBroker(t *testing.T) {
	admin := &fakeLogDirsAdmin{dirs: map[string]string{"1": "/var/lib/kafka/data"}}
	c := &Client{admin: admin}

	dirs, err := c.brokerLogDirs([]int32{1, 9})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dirs[1]; !ok {
		t.Errorf("the readable broker should have its dirs: %v", dirs)
	}
	if _, ok := dirs[9]; ok {
		t.Errorf("the unreadable broker should be absent, got %v", dirs)
	}

	if _, err := c.brokerLogDirs([]int32{9}); err != nil {
		t.Fatal(err)
	}
	if admin.calls != 2 {
		t.Errorf("an unresolved broker must be retried, got %d calls", admin.calls)
	}
}
