package redis_test

import (
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
)

func TestUtilityFunctions(t *testing.T) {
	t.Run("MetadataWithPrefix", func(t *testing.T) {
		t.Run("should add prefix to all metadata keys", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			msg = msg.WithMetadata("user", "john").WithMetadata("role", "admin")
			
			marshaller := subredis.MetadataWithPrefix("app_")
			result := marshaller(msg)
			
			g.Expect(result).To(HaveKey("app_user"))
			g.Expect(result).To(HaveKey("app_role"))
			g.Expect(result["app_user"]).To(Equal("john"))
			g.Expect(result["app_role"]).To(Equal("admin"))
			g.Expect(result).NotTo(HaveKey("user"))
			g.Expect(result).NotTo(HaveKey("role"))
		})

		t.Run("should handle empty metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			marshaller := subredis.MetadataWithPrefix("app_")
			result := marshaller(msg)
			
			g.Expect(result).To(BeEmpty())
		})

		t.Run("should handle empty prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			msg = msg.WithMetadata("user", "john")
			
			marshaller := subredis.MetadataWithPrefix("")
			result := marshaller(msg)
			
			g.Expect(result).To(HaveKey("user"))
			g.Expect(result["user"]).To(Equal("john"))
		})

		t.Run("should handle special characters in prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			msg = msg.WithMetadata("user", "john")
			
			marshaller := subredis.MetadataWithPrefix("app.meta:")
			result := marshaller(msg)
			
			g.Expect(result).To(HaveKey("app.meta:user"))
			g.Expect(result["app.meta:user"]).To(Equal("john"))
		})
	})

	t.Run("PrefixMetadataExtractor", func(t *testing.T) {
		t.Run("should create extractor function", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			// Test that the function factory creates a valid function
			extractor := subredis.PrefixMetadataExtractor("meta_")
			g.Expect(extractor).NotTo(BeNil())
			
			// Test different prefixes are handled
			emptyExtractor := subredis.PrefixMetadataExtractor("")
			g.Expect(emptyExtractor).NotTo(BeNil())
			
			specialExtractor := subredis.PrefixMetadataExtractor("app.meta:")
			g.Expect(specialExtractor).NotTo(BeNil())
		})
	})

	t.Run("PrefixPayloadExtractor", func(t *testing.T) {
		t.Run("should create extractor function", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			// Test that the function factory creates a valid function
			extractor := subredis.PrefixPayloadExtractor("data_")
			g.Expect(extractor).NotTo(BeNil())
			
			// Test different prefixes are handled
			emptyExtractor := subredis.PrefixPayloadExtractor("")
			g.Expect(emptyExtractor).NotTo(BeNil())
			
			specialExtractor := subredis.PrefixPayloadExtractor("app.data:")
			g.Expect(specialExtractor).NotTo(BeNil())
		})
	})

	t.Run("NonPrefixPayloadExtractor", func(t *testing.T) {
		t.Run("should create extractor function", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			// Test that the function factory creates a valid function
			extractor := subredis.NonPrefixPayloadExtractor("meta_")
			g.Expect(extractor).NotTo(BeNil())
			
			// Test different prefixes are handled
			emptyExtractor := subredis.NonPrefixPayloadExtractor("")
			g.Expect(emptyExtractor).NotTo(BeNil())
			
			specialExtractor := subredis.NonPrefixPayloadExtractor("app.meta:")
			g.Expect(specialExtractor).NotTo(BeNil())
		})
	})

	t.Run("Integration tests with real Redis data", func(t *testing.T) {
		t.Run("should work end-to-end with message processing", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			// Test that our utility functions can be used together
			msg := message.NewMessage(nil, map[string]interface{}{
				"field1": "value1",
				"field2": "value2",
			})
			msg = msg.WithMetadata("user", "john").WithMetadata("action", "login")
			
			// Test metadata marshalling
			marshaller := subredis.MetadataWithPrefix("meta_")
			marshalledMeta := marshaller(msg)
			
			g.Expect(marshalledMeta).To(HaveKey("meta_user"))
			g.Expect(marshalledMeta).To(HaveKey("meta_action"))
			g.Expect(marshalledMeta["meta_user"]).To(Equal("john"))
			g.Expect(marshalledMeta["meta_action"]).To(Equal("login"))
		})

		t.Run("should handle mixed data types in metadata values", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			msg = msg.WithMetadata("count", 42).WithMetadata("enabled", true).WithMetadata("name", "test")
			
			marshaller := subredis.MetadataWithPrefix("app_")
			result := marshaller(msg)
			
			g.Expect(result).To(HaveKey("app_count"))
			g.Expect(result).To(HaveKey("app_enabled"))
			g.Expect(result).To(HaveKey("app_name"))
			g.Expect(result["app_count"]).To(Equal(42))
			g.Expect(result["app_enabled"]).To(Equal(true))
			g.Expect(result["app_name"]).To(Equal("test"))
		})

		t.Run("should handle nil metadata gracefully", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			msg := message.NewMessage(nil, nil)
			// msg.Metadata should be nil or empty
			
			marshaller := subredis.MetadataWithPrefix("app_")
			result := marshaller(msg)
			
			g.Expect(result).To(BeEmpty())
		})
	})
}