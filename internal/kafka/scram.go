package kafka

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// Hash generators for the two SCRAM mechanisms Kafka supports.
var (
	sha256Gen scram.HashGeneratorFcn = sha256.New
	sha512Gen scram.HashGeneratorFcn = sha512.New
)

// scramClient adapts xdg-go/scram to sarama.SCRAMClient.
//
// sarama defines the interface but ships no implementation, so every project
// using SCRAM writes this same adapter. Modelled on the one in kafkactl
// (Apache 2.0): example_projects/kafkactl/internal/helpers/scram.go.
type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

var _ sarama.SCRAMClient = (*scramClient)(nil)

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.Client = client
	c.ClientConversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}
