package redis

import (
	"github.com/quantumcycle/expedit/core/message"
	"github.com/redis/go-redis/v9"
)

type PublisherOption struct {
	// MessageMarshaller is a function that converts a message to a google Message. DefaultMessageMarshaller is used if not provided.
	Marshaller MessageMarshaller
}

type MessageMarshaller func(msg *message.Message) (map[string]interface{}, error)

type RedisPublisher struct {
	client *redis.Client
}

func NewRedisPublisher(c *redis.Client) (*RedisPublisher, error) {
	return &RedisPublisher{
		client: c,
	}, nil
}
