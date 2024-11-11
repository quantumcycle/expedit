package middleware

import (
	"github.com/quantumcycle/expedit/core/message"
	mw "github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/sony/gobreaker/v2"
)

func CircuitBreaker(circuit *gobreaker.CircuitBreaker[*message.Message]) mw.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			_, err := circuit.Execute(func() (*message.Message, error) {
				return nil, next(msg)
			})
			return err
		}
	}
}
