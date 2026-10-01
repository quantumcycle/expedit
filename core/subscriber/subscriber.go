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
	// The channel is also closed when the context is cancelled or when the subscriber stops receiving because of an
	// error, in which case Err returns that error.
	Subscribe(ctx context.Context) (<-chan *message.Message, error)
	// Err returns the error that stopped the subscriber from receiving messages. It is meant to be read once the
	// channel returned by Subscribe is closed. It returns nil if the subscriber stopped because the context was
	// cancelled or because it was closed.
	Err() error
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
					p.OnNackError(withCancelCtx, msgImpl, false, p.Nack, err)
				}
			}
		}
		msgCtxCancel()
		msg.Destroy()
	}()

	stateChSubscribeDone.Wait()
	if !sendToChannel(ctx, outputCh, msg) {
		//Nobody will ever see this message, release it so the underlying implementation can redeliver it
		msg.Nack()
	}
}

// sendToChannel sends the message to the channel unless the context is cancelled first. It returns true if the message
// was sent.
func sendToChannel(ctx context.Context, ch chan *message.Message, msg *message.Message) bool {
	select {
	case ch <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

// MessageSubscriber is a helper struct to facilitate the different implementations of Subscribers.
//
// The receive goroutine of the implementation sends to an internal channel that MessageSubscriber relays to the
// channel returned by Subscribe. That channel is closed as soon as the context is cancelled, Close is called or the
// receive goroutine reports that it stopped, even if the receive goroutine itself is slow to exit (for example blocked
// in a read that ignores the context).
type MessageSubscriber[T any] struct {
	// InitializeFn is a function that will be called when the subscriber is initialized. The context passed is a
	// cancel enabled context that will be cancelled when the subscriber is closed.
	//
	// InitializeFn starts the receive goroutine and returns. Messages are sent to outputCh with ProcessMessage, which
	// gives up when ctx is cancelled. When the receive goroutine stops, it must call `done` once, after its last send.
	// Pass the error that made it stop, or nil if it stopped because ctx was cancelled. The error is then returned by
	// Err and the channel returned by Subscribe is closed. An error passed along with a cancelled ctx is considered a
	// consequence of the cancellation and is not reported. outputCh is never closed, do not close it.
	InitializeFn func(ctx context.Context, outputCh chan *message.Message, done func(err error)) error

	lock      sync.Mutex
	closed    bool
	channel   chan *message.Message
	cancel    context.CancelFunc
	forwarded chan struct{}

	errLock sync.Mutex
	err     error
}

func (p *MessageSubscriber[T]) Subscribe(ctx context.Context) (chan *message.Message, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return nil, ErrClosed
	}

	if p.channel != nil {
		return p.channel, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	receiveCh := make(chan *message.Message)
	//0 size blocking channel
	channel := make(chan *message.Message)
	receiveStopped := make(chan struct{})
	forwarded := make(chan struct{})

	var once sync.Once
	done := func(err error) {
		once.Do(func() {
			p.errLock.Lock()
			if err != nil && ctx.Err() == nil {
				p.err = err
			}
			p.errLock.Unlock()
			close(receiveStopped)
		})
	}

	if err := p.InitializeFn(ctx, receiveCh, done); err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer close(forwarded)
		defer close(channel)
		defer cancel()
		for {
			select {
			case msg := <-receiveCh:
				select {
				case channel <- msg:
				case <-ctx.Done():
					//Nobody will ever see this message, release it so the underlying implementation can redeliver it
					msg.Nack()
					return
				}
			case <-receiveStopped:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	p.channel = channel
	p.cancel = cancel
	p.forwarded = forwarded
	return channel, nil
}

// Err returns the error reported by the receive goroutine when it stopped, or nil if it stopped because of a
// cancellation or if it is still running.
func (p *MessageSubscriber[T]) Err() error {
	p.errLock.Lock()
	defer p.errLock.Unlock()
	return p.err
}

// Close cancels the receive goroutine and closes the channel returned by Subscribe. It does not wait for the receive
// goroutine to exit.
func (p *MessageSubscriber[T]) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	if p.channel != nil {
		p.cancel()
		<-p.forwarded
	}
	return nil
}
