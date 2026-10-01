package subscriber

import (
	"context"
	"sync"
	"time"

	"github.com/quantumcycle/expedit/core/message"
)

// Delivery is a received message, with the functions to acknowledge it to the broker.
type Delivery struct {
	Message *message.Message
	Ack     func(ctx context.Context) error
	Nack    func(ctx context.Context) error
}

// DefaultMaxInFlight is the default for DispatchOptions.MaxInFlight.
const DefaultMaxInFlight = 10

// DispatchOptions configures a Dispatcher.
type DispatchOptions struct {
	// MaxInFlight is the maximum number of messages handled at the same time by Dispatch. 0 means
	// DefaultMaxInFlight.
	MaxInFlight int
	// ProcessingTimeout is the deadline of the context of each message. 0 means no deadline. The message is nacked
	// when its handler returns an error, usually the context error, after the deadline.
	ProcessingTimeout time.Duration
	// OnAckError is called when acking a message fails. If nil, the error is ignored.
	OnAckError func(msg *message.Message, err error)
	// OnNackError is called when nacking a message fails. If nil, the error is ignored.
	OnNackError func(msg *message.Message, err error)
}

// Dispatcher handles the messages received by a Subscriber implementation, and acknowledges them from the result of
// the handler. Implementations call Dispatch for each delivery, and Wait before Receive returns.
type Dispatcher struct {
	handler message.HandlerFunc
	opts    DispatchOptions
	slots   chan struct{}
	wg      sync.WaitGroup
}

func NewDispatcher(handler message.HandlerFunc, opts DispatchOptions) *Dispatcher {
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = DefaultMaxInFlight
	}
	return &Dispatcher{
		handler: handler,
		opts:    opts,
		slots:   make(chan struct{}, opts.MaxInFlight),
	}
}

// Dispatch waits until fewer than MaxInFlight messages are handled, and handles the delivery in a new goroutine.
// Blocking the receiving loop of the implementation applies backpressure on the broker. If ctx is done first, the
// delivery is nacked instead.
func (d *Dispatcher) Dispatch(ctx context.Context, delivery Delivery) {
	if ctx.Err() == nil {
		select {
		case d.slots <- struct{}{}:
			d.wg.Add(1)
			go func() {
				defer func() {
					<-d.slots
					d.wg.Done()
				}()
				d.Handle(delivery)
			}()
			return
		case <-ctx.Done():
		}
	}
	d.nack(context.WithoutCancel(delivery.Message.Context()), delivery)
}

// Handle handles the delivery in the calling goroutine. It is meant for implementations that already limit the
// concurrency, and otherwise use Dispatch.
//
// The handler gets the message with a context that keeps the values of the context of the delivery message, but is
// not cancelled with it. If the handler panics, the message is nacked and the panic continues. Add the
// middleware.ConvertPanicToError middleware to nack without crashing.
func (d *Dispatcher) Handle(delivery Delivery) {
	msg := delivery.Message
	// The acknowledgement must not be cancelled, so the in-flight messages are acknowledged during a shutdown
	ackCtx := context.WithoutCancel(msg.Context())
	msgCtx, cancel := ackCtx, context.CancelFunc(func() {})
	if d.opts.ProcessingTimeout > 0 {
		msgCtx, cancel = context.WithTimeout(ackCtx, d.opts.ProcessingTimeout)
	}
	defer cancel()
	msg.SetContext(msgCtx)

	defer func() {
		if recovered := recover(); recovered != nil {
			d.nack(ackCtx, delivery)
			panic(recovered)
		}
	}()
	if err := d.handler(msg); err != nil {
		d.nack(ackCtx, delivery)
		return
	}
	if err := delivery.Ack(ackCtx); err != nil && d.opts.OnAckError != nil {
		d.opts.OnAckError(msg, err)
	}
}

// Wait waits for the dispatched deliveries to be handled and acknowledged.
func (d *Dispatcher) Wait() {
	d.wg.Wait()
}

func (d *Dispatcher) nack(ctx context.Context, delivery Delivery) {
	if err := delivery.Nack(ctx); err != nil && d.opts.OnNackError != nil {
		d.opts.OnNackError(delivery.Message, err)
	}
}
