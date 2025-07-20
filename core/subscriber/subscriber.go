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

type AckNackFn[T any] func(ctx context.Context, msgImpl T) error

// OnAckNackErrorFn is an error handler for when Ack or Nack fails. The `ack` bool if the error occured for Ack (true)
// or Nack (false). The `ackNackFn` is the internal function that performs the Ack or Nack. It's provided to be able
// to retry the operation. `err` is the original error that caused the Ack or Nack to fail.
type OnAckNackErrorFn[T any] func(ctx context.Context, msgImpl T, ack bool, ackNackFn AckNackFn[T], err error)

// MessageProcessor is a helper struct to facilitate the different implementations of Subscribers.
// You just need to provide the different functions to act on the underlying message of your implementation.
type MessageProcessor[T any] struct {
	Ack  AckNackFn[T]
	Nack AckNackFn[T]

	MessageUnmarshall func(ctx context.Context, msgImpl T) *message.Message

	ProcessingTimeout   time.Duration
	OnProcessingTimeout func(ctx context.Context, msgImpl T)

	OnAckError  OnAckNackErrorFn[T]
	OnNackError OnAckNackErrorFn[T]
}

func (p MessageProcessor[T]) ProcessMessage(ctx context.Context, msgImpl T, outputCh chan *message.Message) {
	withCancelCtx, msgCtxCancel := context.WithCancel(ctx)

	msg := p.MessageUnmarshall(withCancelCtx, msgImpl)

	var stateChSubscribeDone sync.WaitGroup

	//Goroutine to nack message after processing timeout
	if p.ProcessingTimeout > 0 {
		stateChSubscribeDone.Add(1)
		go func() {
			ctxProcessingTimeout, cancel := context.WithTimeout(ctx, p.ProcessingTimeout)
			defer cancel()

			stateCh := msg.StateChange()
			stateChSubscribeDone.Done()

			select {
			case <-stateCh:
				//in case of state change, we just return. it means the message was ack or nack and we don't need to
				//track the timeout anymore
				return
			case <-ctxProcessingTimeout.Done():
				if p.OnProcessingTimeout != nil {
					p.OnProcessingTimeout(withCancelCtx, msgImpl)
				}
				//It's fine to Nack in all cases, because if the message is already acknowledged, the Nack will be ignored
				msg.Nack()
			}
		}()
	}

	//Goroutine to call the underlying Ack/Nack functions based on status changed
	stateChSubscribeDone.Add(1)
	go func() {
		stateCh := msg.StateChange()
		stateChSubscribeDone.Done()
		select {
		case state := <-stateCh:
			if state == message.Ack {
				err := p.Ack(withCancelCtx, msgImpl)
				//if error handler is not configured, the error is ignored
				//TODO: once we add logging, we should log the error as a fallback
				if err != nil && p.OnAckError != nil {
					p.OnAckError(withCancelCtx, msgImpl, true, p.Ack, err)
				}
			} else if state == message.Nack {
				err := p.Nack(withCancelCtx, msgImpl)
				//if error handler is not configured, the error is ignored
				//TODO: once we add logging, we should log the error as a fallback
				if err != nil && p.OnNackError != nil {
					p.OnAckError(withCancelCtx, msgImpl, false, p.Nack, err)
				}
			}
		}
		msgCtxCancel()
		msg.Destroy()
	}()

	stateChSubscribeDone.Wait()
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
