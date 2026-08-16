package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func quorumResponse(leader int32) *kmsg.DescribeQuorumResponse {
	replica := func(id int32, end int64) kmsg.DescribeQuorumResponseTopicPartitionReplicaState {
		r := kmsg.NewDescribeQuorumResponseTopicPartitionReplicaState()
		r.ReplicaID, r.LogEndOffset = id, end
		return r
	}

	partition := kmsg.NewDescribeQuorumResponseTopicPartition()
	partition.Partition = 0
	partition.LeaderID = leader
	partition.LeaderEpoch = 7
	partition.HighWatermark = 950
	partition.CurrentVoters = []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{
		replica(1, 1000), replica(2, 1000), replica(3, 940),
	}
	partition.Observers = []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{
		replica(4, 990), replica(5, -1),
	}

	topic := kmsg.NewDescribeQuorumResponseTopic()
	topic.Topic = clusterMetadataTopic
	topic.Partitions = []kmsg.DescribeQuorumResponseTopicPartition{partition}

	response := kmsg.NewDescribeQuorumResponse()
	response.Topics = []kmsg.DescribeQuorumResponseTopic{topic}
	return &response
}

func TestQuorumFromResponse(t *testing.T) {
	info, err := quorumFromResponse(quorumResponse(1))
	if err != nil {
		t.Fatalf("quorumFromResponse: %v", err)
	}

	if info.LeaderID != 1 || info.LeaderEpoch != 7 || info.HighWatermark != 950 {
		t.Errorf("header fields wrong: %+v", info)
	}

	// Lags are measured against the leader's log end: the leader is at 0 by
	// definition, a caught-up voter at 0, a behind one at the difference, and
	// an unknown log end stays unknown rather than becoming a number.
	wantLags := map[int32]int64{1: 0, 2: 0, 3: 60, 4: 10, 5: -1}
	for _, replica := range append(info.Voters, info.Observers...) {
		if got := wantLags[replica.ID]; replica.Lag != got {
			t.Errorf("replica %d: lag %d, want %d", replica.ID, replica.Lag, got)
		}
	}

	for id, want := range map[int32]string{1: "leader", 2: "follower", 4: "observer", 9: ""} {
		if got := info.Role(id); got != want {
			t.Errorf("Role(%d) = %q, want %q", id, got, want)
		}
	}
}

// The codes meaning "there is no quorum here" map onto ErrNoQuorum, so the UI
// can fall back without treating a ZooKeeper cluster as broken.
func TestQuorumErrorMapping(t *testing.T) {
	response := quorumResponse(1)
	response.ErrorCode = 35 // UNSUPPORTED_VERSION
	if _, err := quorumFromResponse(response); !errors.Is(err, ErrNoQuorum) {
		t.Errorf("code 35 should mean no quorum, got %v", err)
	}

	response = quorumResponse(1)
	response.Topics[0].Partitions[0].ErrorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
	if _, err := quorumFromResponse(response); !errors.Is(err, ErrNoQuorum) {
		t.Errorf("code 3 should mean no quorum, got %v", err)
	}

	response = quorumResponse(1)
	message := "boom"
	response.Topics[0].Partitions[0].ErrorCode = 58
	response.Topics[0].Partitions[0].ErrorMessage = &message
	if _, err := quorumFromResponse(response); err == nil || errors.Is(err, ErrNoQuorum) {
		t.Errorf("a real error must surface, got %v", err)
	}
}
