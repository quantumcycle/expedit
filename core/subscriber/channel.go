package subscriber

import (
	"context"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/sourcegraph/conc/pool"
	"sync"
)

type ChannelSubscriber struct {
	inputCh       chan *message.Message
	maxConcurrent int

	// closeLock protects inputCh against a send after it is closed by Close.
	closeLock sync.RWMutex
	closeOnce sync.Once
	closing   chan struct{}
}

// Close closes the input channel. Messages nacked after that are dropped instead of being requeued.
func (c *ChannelSubscriber) Close() error {
	c.closeOnce.Do(func() {
		// Wake up the pending requeues so they release the lock
		close(c.closing)
		c.closeLock.Lock()
		defer c.closeLock.Unlock()
		close(c.inputCh)
	})
	return nil
}

// Err always returns nil, a channel subscriber cannot fail.
func (c *ChannelSubscriber) Err() error {
	return nil
}

// requeue puts a nacked message back in the input channel for another pass.
// It gives up when the context is done or the subscriber is closed.
func (c *ChannelSubscriber) requeue(ctx context.Context, msg *message.Message) {
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()
	select {
	case <-c.closing:
		return
	default:
	}
	defer func() {
		//The input channel can also be closed by its owner, in which case there is nowhere to requeue the message
		recover()
	}()
	select {
	case c.inputCh <- msg:
	case <-c.closing:
	case <-ctx.Done():
	}
}

func (c *ChannelSubscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	outputCh := make(chan *message.Message, c.maxConcurrent)
	go func() {
		p := pool.New().WithMaxGoroutines(c.maxConcurrent)
		defer func() {
			// wait for the in-flight messages (they stop waiting on ctx.Done) before closing the output
			p.Wait()
			close(outputCh)
		}()
		for {
			select {
			case msg, ok := <-c.inputCh:
				// handle channel closing case
				if !ok || msg == nil {
					return
				}
				msgCopy := msg.Copy()
				withCancelCtx, msgCtxCancel := context.WithCancel(msgCopy.Context())
				msgCopy.SetContext(withCancelCtx)
				// Subscribe to the state before the message is sent out, otherwise the change could be missed
				stateCh := msgCopy.StateChange()
				p.Go(func() {
					defer msgCtxCancel()
					select {
					case state := <-stateCh:
						if state == message.Nack {
							c.requeue(ctx, msg)
						}
					case <-ctx.Done():
					}
				})
				select {
				case outputCh <- msgCopy:
				case <-ctx.Done():
					return
				}
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
		closing:       make(chan struct{}),
	}
}
