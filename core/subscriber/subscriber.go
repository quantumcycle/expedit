package subscriber

import (
	"github.com/quantumcycle/expedit/core/message"
)

type Subscriber interface {
	Subscribe() (<-chan *message.Message[any], error)
	Close() error
}
