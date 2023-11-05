package subscriber

import (
	"github.com/quantumcycle/expedit/core/message"
)

type Subscriber interface {
	Subscribe() (<-chan *message.Message, error)
	Close() error
}
