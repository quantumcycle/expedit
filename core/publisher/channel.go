package publisher

import (
	"github.com/quantumcycle/expedit/core/message"
)

type ChannelPublisher struct {
	channel chan *message.Message[any]
}

func NewChannelPublisher(c chan *message.Message[any]) *ChannelPublisher {
	return &ChannelPublisher{
		channel: c,
	}
}

func (p *ChannelPublisher) Publish(msg *message.Message[any]) error {
	p.channel <- msg
	return nil
}

func (p *ChannelPublisher) Close() error {
	close(p.channel)
	return nil
}
