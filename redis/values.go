package redis

import (
	"github.com/quantumcycle/expedit/core/message"
	"strings"
)

// MetadataWithPrefix is a function that adds a prefix to all metadata keys.
func MetadataWithPrefix(prefix string) XAddValuesMarshaller {
	return func(msg *message.Message) map[string]interface{} {
		values := make(map[string]interface{}, len(msg.Metadata))
		for k, v := range msg.Metadata {
			values[prefix+k] = v
		}
		return values
	}
}

// PrefixMetadataExtractor is a function that extracts all values where the key starts with a prefix and use
// them as metadata.
func PrefixMetadataExtractor(prefix string) func(wrapper MessageWrapper) map[string]string {
	return func(wrapper MessageWrapper) map[string]string {
		metadata := make(map[string]string, len(wrapper.msg.Values))
		for k, v := range wrapper.msg.Values {
			if strings.HasPrefix(k, prefix) {
				if s, ok := v.(string); ok {
					metadata[strings.TrimPrefix(k, prefix)] = s
				}
			}
		}
		return metadata
	}
}

// PrefixPayloadExtractor is a function that extracts all values where the key starts with a prefix and use
// them as payload.
func PrefixPayloadExtractor(prefix string) func(wrapper MessageWrapper) map[string]interface{} {
	return func(wrapper MessageWrapper) map[string]interface{} {
		payload := make(map[string]interface{}, len(wrapper.msg.Values))
		for k, v := range wrapper.msg.Values {
			if strings.HasPrefix(k, prefix) {
				payload[strings.TrimPrefix(k, prefix)] = v
			}
		}
		return payload
	}
}

// NonPrefixPayloadExtractor is a function that extracts all values where the key doesn't starts with a prefix and use
// them as payload.
func NonPrefixPayloadExtractor(prefix string) func(wrapper MessageWrapper) map[string]interface{} {
	return func(wrapper MessageWrapper) map[string]interface{} {
		payload := map[string]interface{}{}
		for k, v := range wrapper.msg.Values {
			if !strings.HasPrefix(k, prefix) {
				payload[k] = v
			}
		}
		return payload
	}
}
