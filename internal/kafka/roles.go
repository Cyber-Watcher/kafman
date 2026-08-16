package kafka

import (
	"strconv"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

// Broker roles, from each broker's static configuration.
//
// The motivation is a KRaft peculiarity: the "controller id" in a metadata
// response is NOT the quorum leader — brokers hand out a random live broker
// there, and the answer changes between requests (verified against a live
// cluster, where kafka-metadata-quorum reported a different node). The quorum
// itself is only reachable on the controller listener, which clients do not
// have. What a client can learn honestly is each visible node's `process.roles`
// via DescribeConfigs — empty on a ZooKeeper-era broker, where the metadata
// controller id is the real controller.

// rolesCache remembers each broker's process.roles for the life of the
// connection: a node's role can only change with a restart.
type rolesCache struct {
	mu    sync.Mutex
	roles map[int32]string
}

// brokerRoles returns process.roles per broker id. Uncached entries are
// fetched with one batched DescribeConfigs — sarama fans it out to the node
// each broker resource lives on. Best effort: a broker that cannot be asked is
// simply absent from the result and will be asked again next time.
func (c *Client) brokerRoles(ids []int32) map[int32]string {
	c.rolesCache.mu.Lock()
	defer c.rolesCache.mu.Unlock()

	if c.rolesCache.roles == nil {
		c.rolesCache.roles = make(map[int32]string)
	}

	out := make(map[int32]string, len(ids))
	var missing []*sarama.ConfigResource
	for _, id := range ids {
		if roles, ok := c.rolesCache.roles[id]; ok {
			out[id] = roles
			continue
		}
		missing = append(missing, &sarama.ConfigResource{
			Type: sarama.BrokerResource,
			Name: strconv.FormatInt(int64(id), 10),
		})
	}
	if len(missing) == 0 {
		return out
	}

	results, err := c.admin.DescribeConfigs(missing, sarama.DescribeConfigsOptions{})
	if err != nil {
		return out
	}
	for _, result := range results {
		if result.ErrorCode != 0 {
			continue
		}
		id, err := strconv.ParseInt(result.Name, 10, 32)
		if err != nil {
			continue
		}
		roles := ""
		for _, entry := range result.Configs {
			if entry.Name == "process.roles" {
				roles = entry.Value
				break
			}
		}
		c.rolesCache.roles[int32(id)] = roles
		out[int32(id)] = roles
	}
	return out
}

// resolveController decides what "controller" honestly means for one node: on
// a KRaft cluster it is the node's own role, on a ZooKeeper cluster (or when
// no roles could be read) it is the metadata controller id.
func resolveController(roles map[int32]string, id, controllerID int32) bool {
	if kraftCluster(roles) {
		return strings.Contains(roles[id], "controller")
	}
	return id == controllerID
}

// kraftCluster reports whether any node advertises a KRaft role.
func kraftCluster(roles map[int32]string) bool {
	for _, r := range roles {
		if r != "" {
			return true
		}
	}
	return false
}
