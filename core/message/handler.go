package message

type HandlerFunc func(msg *Message[any]) error
