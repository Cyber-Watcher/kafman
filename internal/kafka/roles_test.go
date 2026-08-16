package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

// fakeConfigAdmin serves broker configs for the roles cache.
type fakeConfigAdmin struct {
	sarama.ClusterAdmin
	roles map[string]string // broker id -> process.roles
	calls int
}

func (f *fakeConfigAdmin) DescribeConfigs(resources []*sarama.ConfigResource, _ sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	f.calls++
	out := make([]*sarama.ConfigResourceResult, 0, len(resources))
	for _, resource := range resources {
		roles, ok := f.roles[resource.Name]
		if !ok {
			// An unreachable broker: a per-resource error, not a batch failure.
			out = append(out, &sarama.ConfigResourceResult{Name: resource.Name, ErrorCode: 8})
			continue
		}
		result := &sarama.ConfigResourceResult{Name: resource.Name}
		if roles == "" {
			// A ZooKeeper-era broker has no process.roles entry at all.
			result.Configs = []sarama.ConfigEntry{{Name: "broker.id", Value: resource.Name}}
		} else {
			result.Configs = []sarama.ConfigEntry{{Name: "process.roles", Value: roles}}
		}
		out = append(out, result)
	}
	return out, nil
}

func TestBrokerRolesAreCached(t *testing.T) {
	admin := &fakeConfigAdmin{roles: map[string]string{"1": "broker,controller", "2": "broker"}}
	c := &Client{admin: admin}

	roles := c.brokerRoles([]int32{1, 2})
	if roles[1] != "broker,controller" || roles[2] != "broker" {
		t.Fatalf("unexpected roles: %v", roles)
	}
	if admin.calls != 1 {
		t.Fatalf("expected one batched DescribeConfigs, got %d", admin.calls)
	}

	// The second ask must come from the cache: roles change only on restart.
	c.brokerRoles([]int32{1, 2})
	if admin.calls != 1 {
		t.Errorf("roles should be cached, got %d calls", admin.calls)
	}
}

func TestResolveController(t *testing.T) {
	kraft := map[int32]string{1: "broker,controller", 2: "broker"}
	if !resolveController(kraft, 1, 2) {
		t.Error("a KRaft node with the controller role is a controller")
	}
	if resolveController(kraft, 2, 2) {
		t.Error("on KRaft the metadata controller id must be ignored: it is a random broker")
	}

	zk := map[int32]string{1: "", 2: ""}
	if !resolveController(zk, 2, 2) {
		t.Error("on ZooKeeper the metadata controller id is the real controller")
	}
	if resolveController(zk, 1, 2) {
		t.Error("a ZooKeeper broker that is not the controller must not be marked")
	}
}

// The roles must survive an unreadable broker config: the node simply stays
// without a role rather than failing the whole describe.
func TestBrokerRolesBestEffort(t *testing.T) {
	admin := &fakeConfigAdmin{roles: map[string]string{"1": "broker,controller"}}
	c := &Client{admin: admin}

	roles := c.brokerRoles([]int32{1, 9})
	if roles[1] != "broker,controller" {
		t.Errorf("the readable broker should have its roles: %v", roles)
	}
	if _, ok := roles[9]; ok {
		t.Errorf("the unreadable broker should be absent, got %v", roles)
	}
}
