package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

// Each broker's log.dirs config, for matching a data directory against the
// filesystems its node_exporter reports. Unlike roles.go this returns an
// error: the OS panel must distinguish "could not be read" from "no data".

// logDirsCache remembers each broker's log.dirs for the life of the
// connection: the value only changes with a restart.
type logDirsCache struct {
	mu   sync.Mutex
	dirs map[int32][]string
}

// BrokerLogDirs returns log.dirs per broker id, split on commas.
func (c *Client) BrokerLogDirs(ctx context.Context) (map[int32][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids := make([]int32, 0, len(c.client.Brokers()))
	for _, broker := range c.client.Brokers() {
		ids = append(ids, broker.ID())
	}
	return c.brokerLogDirs(ids)
}

// brokerLogDirs serves ids from the cache; the rest are fetched with one
// batched DescribeConfigs.
func (c *Client) brokerLogDirs(ids []int32) (map[int32][]string, error) {
	c.logDirsCache.mu.Lock()
	defer c.logDirsCache.mu.Unlock()

	if c.logDirsCache.dirs == nil {
		c.logDirsCache.dirs = make(map[int32][]string)
	}

	out := make(map[int32][]string)
	var missing []*sarama.ConfigResource
	for _, id := range ids {
		if dirs, ok := c.logDirsCache.dirs[id]; ok {
			out[id] = dirs
			continue
		}
		missing = append(missing, &sarama.ConfigResource{
			Type:        sarama.BrokerResource,
			Name:        strconv.FormatInt(int64(id), 10),
			ConfigNames: []string{"log.dirs"},
		})
	}
	if len(missing) == 0 {
		return out, nil
	}

	results, err := c.admin.DescribeConfigs(missing, sarama.DescribeConfigsOptions{})
	if err != nil {
		return out, fmt.Errorf("describe broker configs: %w", err)
	}
	for _, result := range results {
		if result.ErrorCode != 0 {
			continue
		}
		id, err := strconv.ParseInt(result.Name, 10, 32)
		if err != nil {
			continue
		}
		for _, entry := range result.Configs {
			if entry.Name != "log.dirs" {
				continue
			}
			dirs := splitLogDirs(entry.Value)
			c.logDirsCache.dirs[int32(id)] = dirs
			out[int32(id)] = dirs
			break
		}
	}
	return out, nil
}

// splitLogDirs turns a log.dirs value into clean paths.
func splitLogDirs(value string) []string {
	var dirs []string
	for _, dir := range strings.Split(value, ",") {
		if dir = strings.TrimSpace(dir); dir != "" {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}
