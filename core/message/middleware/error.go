package middleware

import (
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
)

func PanicRecoverer() Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			defer func() {
				if r := recover(); r != nil {
					if e, ok := r.(error); ok {
						err = e
					} else {
						err = fmt.Errorf("panic recovered: %v", r)
					}
				}
			}()
			return next(msg)
		}
	}
}

func OnError(errHandler func(msg *message.Message[any], err error)) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			err = next(msg)
			if err != nil {
				errHandler(msg, err)
			}
			return err
		}
	}
}
