package log

import (
	"context"
	"log/slog"
	"sync"
)

// WithMessageContextLogger adds a new MessageContextLogger to the context and return the new context.
func WithMessageContextLogger(ctx context.Context, delegate Logger) context.Context {
	return context.WithValue(ctx, logEntriesContextKey{}, NewMessageContextLogger(delegate))
}

// GetMessageContextLogger returns the MessageContextLogger from an existing context or nil if it doesn't exist.
// WithMessageContextLogger needs to be called first to add the MessageContextLogger to the context.
func GetMessageContextLogger(ctx context.Context) *MessageContextLogger {
	if v := ctx.Value(logEntriesContextKey{}); v != nil {
		return v.(*MessageContextLogger)
	}
	return nil
}

type MessageContextLogger struct {
	delegate Logger
	mutex    sync.Mutex
	entries  []entry
}

type entry struct {
	ctx     context.Context
	level   slog.Level
	message string
	args    []interface{}
}

type logEntriesContextKey struct{}

func NewMessageContextLogger(delegate Logger) *MessageContextLogger {
	return &MessageContextLogger{
		delegate: delegate,
	}
}

func (m *MessageContextLogger) SendToDelegate() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, entry := range m.entries {
		switch entry.level {
		case slog.LevelDebug:
			m.delegate.Debugf(entry.ctx, entry.message, entry.args...)
		case slog.LevelInfo:
			m.delegate.Infof(entry.ctx, entry.message, entry.args...)
		case slog.LevelWarn:
			m.delegate.Warnf(entry.ctx, entry.message, entry.args...)
		case slog.LevelError:
			m.delegate.Errorf(entry.ctx, entry.message, entry.args...)
		}
	}
	m.entries = nil
}

func (m *MessageContextLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	m.addEntry(ctx, slog.LevelDebug, format, args...)
}

func (m *MessageContextLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	m.addEntry(ctx, slog.LevelInfo, format, args...)
}

func (m *MessageContextLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	m.addEntry(ctx, slog.LevelWarn, format, args...)
}

func (m *MessageContextLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	m.addEntry(ctx, slog.LevelError, format, args...)
}

func (m *MessageContextLogger) addEntry(ctx context.Context, level slog.Level, format string, args ...interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.entries = append(m.entries, entry{
		ctx:     ctx,
		level:   level,
		message: format,
		args:    args,
	})
}
