package subscriber

import (
	"context"
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

func (c *ChannelSubscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	outputCh := make(chan *message.Message, c.maxConcurrent)
	go func() {
		p := pool.New().WithMaxGoroutines(c.maxConcurrent)
		for {
			select {
			case msg := <-c.inputCh:
				// handle channel closing case
				if msg == nil {
					return
				}
				msgCopy := msg.Copy()
				withCancelCtx, msgCtxCancel := context.WithCancel(msgCopy.Context())
				msgCopy.SetContext(withCancelCtx)
				p.Go(func() {
					state := <-msgCopy.StateChange()
					if state == message.Nack {
						//Put the message back for another pass
						c.inputCh <- msg
					}
					msgCtxCancel()
				})
				outputCh <- msgCopy
			case <-ctx.Done():
				return
			}
		}
	}()
	return outputCh, nil
}

func NewChannelSubscriber(inputCh chan *message.Message, maxConcurrent int) *ChannelSubscriber {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &ChannelSubscriber{
		inputCh:       inputCh,
		maxConcurrent: maxConcurrent,
	}
}
