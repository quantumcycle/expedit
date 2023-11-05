package middleware

import "github.com/quantumcycle/expedit/core/message"

type Middleware func(h message.HandlerFunc) message.HandlerFunc
