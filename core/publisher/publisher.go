package publisher

import (
	"github.com/quantumcycle/expedit/core/message"
	"io"
)

type Publisher interface {
	io.Closer
	Publish(message *message.Message[any]) error
}
