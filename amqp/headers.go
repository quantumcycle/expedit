package amqp

import (
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"strings"
)

// HeadersAsMetadata converts AMQP headers (amqp.Table) to message metadata.
// This is useful when you need to extract metadata from AMQP delivery headers.
func HeadersAsMetadata(headers amqp091.Table) message.Metadata {
	metadata := make(message.Metadata)
	for key, value := range headers {
		// Convert the value to string representation
		metadata[key] = fmt.Sprintf("%v", value)
	}
	return metadata
}

// Note: MetadataAsHeaders function is defined in publisher.go

// PrefixedHeadersProvider creates a HeadersProvider that adds a prefix to all metadata keys
// when converting them to AMQP headers. This is useful for namespacing custom headers.
func PrefixedHeadersProvider(prefix string) HeadersProvider {
	return func(msg *message.Message) map[string]interface{} {
		headers := make(map[string]interface{})
		for key, value := range msg.Metadata {
			headers[prefix+key] = value
		}
		return headers
	}
}

// FilteredHeadersProvider creates a HeadersProvider that only includes metadata keys
// that match the provided filter function.
func FilteredHeadersProvider(filter func(key string) bool) HeadersProvider {
	return func(msg *message.Message) map[string]interface{} {
		headers := make(map[string]interface{})
		for key, value := range msg.Metadata {
			if filter(key) {
				headers[key] = value
			}
		}
		return headers
	}
}

// ExcludePrefixHeadersProvider creates a HeadersProvider that excludes metadata keys
// that start with the specified prefix. This is useful for filtering out internal metadata.
func ExcludePrefixHeadersProvider(prefix string) HeadersProvider {
	return FilteredHeadersProvider(func(key string) bool {
		return !strings.HasPrefix(key, prefix)
	})
}

// OnlyPrefixHeadersProvider creates a HeadersProvider that only includes metadata keys
// that start with the specified prefix. This is useful for including only specific metadata.
func OnlyPrefixHeadersProvider(prefix string) HeadersProvider {
	return FilteredHeadersProvider(func(key string) bool {
		return strings.HasPrefix(key, prefix)
	})
}

// ExtractPrefixedMetadata extracts headers with a specific prefix and returns them as metadata
// with the prefix removed. This is useful for extracting namespaced headers from AMQP deliveries.
func ExtractPrefixedMetadata(headers amqp091.Table, prefix string) message.Metadata {
	metadata := make(message.Metadata)
	for key, value := range headers {
		if strings.HasPrefix(key, prefix) {
			// Remove prefix from key
			cleanKey := strings.TrimPrefix(key, prefix)
			metadata[cleanKey] = fmt.Sprintf("%v", value)
		}
	}
	return metadata
}

// MergeHeadersToMetadata merges AMQP headers into existing message metadata.
// If there are key conflicts, the header values will override the metadata values.
func MergeHeadersToMetadata(msg *message.Message, headers amqp091.Table) {
	for key, value := range headers {
		msg.Metadata[key] = fmt.Sprintf("%v", value)
	}
}