package publisher

import "github.com/quantumcycle/expedit/core/message"

type DestinationTopic string

type RoutingFunc func(msg *message.Message) (DestinationTopic, error)

func ConstantTopic(topic string) RoutingFunc {
	return func(msg *message.Message) (DestinationTopic, error) {
		return DestinationTopic(topic), nil
	}
}
