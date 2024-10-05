package subscriber

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"sync"
	"time"
)

type Subscriber interface {
	// Subscribe will return a channel of messages. The channel will be closed when the subscriber is closed.
	// Be advised that when the channel is closed you will receive 'nil' if you are currently ranging over the channel.
	Subscribe(ctx context.Context) (<-chan *message.Message, error)
	Close() error
}

var ErrClosed = errors.New("subscriber is closed")

// MessageProcessor is a helper struct to facilitate the different implementations of Subscribers.
// You just need to provide the different functions to act on the underlying message of your implementation.
type MessageProcessor[T any] struct {
	Ack  func(ctx context.Context, msg T)
	Nack func(ctx context.Context, msg T)

	MessageUnmarshall    func(ctx context.Context, msgImpl T) (*message.Message, error)
	OnUnmarshallingError func(msgImpl T, err error)

	ProcessingTimeout   time.Duration
	OnProcessingTimeout func(ctx context.Context, msgImpl T)
}

func (p MessageProcessor[T]) ProcessMessage(ctx context.Context, msgImpl T, outputCh chan *message.Message) {
	withCancelCtx, msgCtxCancel := context.WithCancel(ctx)

	msg, err := p.MessageUnmarshall(withCancelCtx, msgImpl)
	if err != nil {
		if p.OnUnmarshallingError != nil {
			p.OnUnmarshallingError(msgImpl, err)
		} else {
			p.Nack(ctx, msgImpl)
		}
		msgCtxCancel()
		return
	}

	go func() {
		ctxProcessingTimeout, cancel := context.WithTimeout(ctx, p.ProcessingTimeout)
		defer cancel()

		select {
		case <-ctxProcessingTimeout.Done():
			if p.OnProcessingTimeout != nil {
				p.OnProcessingTimeout(withCancelCtx, msgImpl)
			}
			p.Nack(withCancelCtx, msgImpl)
		case state := <-msg.StateChange():
			if state == message.Ack {
				p.Ack(withCancelCtx, msgImpl)
			} else {
				p.Nack(withCancelCtx, msgImpl)
			}
		}
		msgCtxCancel()
	}()
	safeSendToChannel(outputCh, msg)
}

func safeSendToChannel(ch chan *message.Message, msg *message.Message) {
	defer func() {
		//ignore closed channel panic
		recover()
	}()
	ch <- msg
}

// MessageSubscriber is a helper struct to facilitate the different implementations of Subscribers.
type MessageSubscriber[T any] struct {
	// InitializeFn is a function that will be called when the subscriber is initialized. The context passed is a
	// cancel enabled context that will be cancelled when the subscriber is closed.
	InitializeFn func(ctx context.Context, outputCh chan *message.Message) error

	closed           bool
	initChannelLock  sync.RWMutex
	channel          chan *message.Message
	processingCancel context.CancelFunc
}

func (p *MessageSubscriber[T]) Subscribe(ctx context.Context) (chan *message.Message, error) {
	if p.closed {
		return nil, ErrClosed
	}

	if p.channel != nil {
		return p.channel, nil
	}

	//Make sure we don't have a concurrent double initialization going on
	p.initChannelLock.RLock()
	if p.channel != nil {
		p.initChannelLock.RUnlock()
		return p.channel, nil
	}
	p.initChannelLock.RUnlock()

	return p.initialize(ctx)
}

func (p *MessageSubscriber[T]) initialize(ctx context.Context) (chan *message.Message, error) {
	p.initChannelLock.Lock()
	defer p.initChannelLock.Unlock()

	//0 size blocking channel
	p.channel = make(chan *message.Message)

	ctx, cancel := context.WithCancel(ctx)
	p.processingCancel = cancel

	err := p.InitializeFn(ctx, p.channel)
	if err != nil {
		return nil, err
	}

	return p.channel, nil
}

func (p *MessageSubscriber[T]) Close() error {
	if p.closed {
		return nil
	}
	p.initChannelLock.Lock()
	defer p.initChannelLock.Unlock()

	var err error
	if p.channel != nil {
		p.processingCancel()
		close(p.channel)
		p.channel = nil
	}
	p.closed = true

	return err
}
