package middleware

import "github.com/quantumcycle/expedit/core/message"

type Chain struct {
	middlewares []Middleware
}

func NewChain() *Chain {
	return &Chain{}
}

func (c *Chain) Add(middleware Middleware) {
	c.middlewares = append(c.middlewares, middleware)
}

func (c *Chain) Wrap(h message.HandlerFunc) message.HandlerFunc {
	final := h
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		final = c.middlewares[i](final)
	}
	return final
}
