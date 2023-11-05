package subscriber

import (
	"github.com/quantumcycle/expedit/core/message"
	"github.com/sourcegraph/conc/pool"
)

type ChannelSubscriber struct {
	inputCh       chan *message.Message
	maxConcurrent int
}

func (c *ChannelSubscriber) Close() error {
	close(c.inputCh)
	return nil
}

func (c *ChannelSubscriber) Subscribe() (<-chan *message.Message, error) {
	outputCh := make(chan *message.Message, c.maxConcurrent)
	go func() {
		p := pool.New().WithMaxGoroutines(c.maxConcurrent)
		for msg := range c.inputCh {
			msgCopy := msg.Copy()
			p.Go(func() {
				state := <-msgCopy.StateChange()
				if state == message.Nack {
					//Put the message back for another pass
					c.inputCh <- msg
				}
			})
			outputCh <- msgCopy
		}
	}()
	return outputCh, nil
}

func NewChannelSubscriber(inputCh chan *message.Message, maxConcurrent int) *ChannelSubscriber {
	return &ChannelSubscriber{
		inputCh:       inputCh,
		maxConcurrent: maxConcurrent,
	}
}
