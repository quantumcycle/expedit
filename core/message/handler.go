package message

type HandlerFunc func(msg *Message) error
