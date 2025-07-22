package redis_test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
)

type TestStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestJSONUtilities(t *testing.T) {
	t.Run("MarshallPayloadToJsonMap", func(t *testing.T) {
		t.Run("should marshal payload to JSON in specified map key", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := TestStruct{
				Name:  "test",
				Value: 42,
			}
			msg := message.NewMessage(context.Background(), payload)

			marshaller := subredis.MarshallPayloadToJsonMap("data")
			result := marshaller(msg)

			g.Expect(result).To(HaveKey("data"))
			g.Expect(result["data"]).To(BeAssignableToTypeOf([]byte{}))
			
			// Verify it's valid JSON
			jsonData := result["data"].([]byte)
			g.Expect(string(jsonData)).To(ContainSubstring("\"name\":\"test\""))
			g.Expect(string(jsonData)).To(ContainSubstring("\"value\":42"))
		})

		t.Run("should handle simple data types", func(t *testing.T) {
			g := NewGomegaWithT(t)

			msg := message.NewMessage(context.Background(), "simple string")
			marshaller := subredis.MarshallPayloadToJsonMap("data")
			result := marshaller(msg)

			g.Expect(result).To(HaveKey("data"))
			jsonData := result["data"].([]byte)
			g.Expect(string(jsonData)).To(Equal("\"simple string\""))
		})

		t.Run("should handle nil payload", func(t *testing.T) {
			g := NewGomegaWithT(t)

			msg := message.NewMessage(context.Background(), nil)
			marshaller := subredis.MarshallPayloadToJsonMap("data")
			result := marshaller(msg)

			g.Expect(result).To(HaveKey("data"))
			jsonData := result["data"].([]byte)
			g.Expect(string(jsonData)).To(Equal("null"))
		})

		t.Run("should handle map payload", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := map[string]interface{}{
				"key1": "value1",
				"key2": 123,
			}
			msg := message.NewMessage(context.Background(), payload)
			marshaller := subredis.MarshallPayloadToJsonMap("json_data")
			result := marshaller(msg)

			g.Expect(result).To(HaveKey("json_data"))
			jsonData := result["json_data"].([]byte)
			g.Expect(string(jsonData)).To(ContainSubstring("\"key1\":\"value1\""))
			g.Expect(string(jsonData)).To(ContainSubstring("\"key2\":123"))
		})

		t.Run("should use custom map key", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := "test data"
			msg := message.NewMessage(context.Background(), payload)
			marshaller := subredis.MarshallPayloadToJsonMap("custom_key")
			result := marshaller(msg)

			g.Expect(result).To(HaveKey("custom_key"))
			g.Expect(result).NotTo(HaveKey("data"))
		})
	})

	t.Run("UnmarshallMapPayloadFromJson", func(t *testing.T) {
		t.Run("should unmarshal JSON from map key to struct", func(t *testing.T) {
			g := NewGomegaWithT(t)

			jsonData := `{"name":"test","value":42}`
			payload := map[string]interface{}{
				"json_payload": jsonData,
				"other_field":  "other_value",
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handlerCalled := false
			
			handler := func(msg *message.Message) error {
				handlerCalled = true
				// Verify payload was unmarshalled to struct
				structPayload, ok := msg.Payload.(TestStruct)
				g.Expect(ok).To(BeTrue())
				g.Expect(structPayload.Name).To(Equal("test"))
				g.Expect(structPayload.Value).To(Equal(42))
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handlerCalled).To(BeTrue())
		})

		t.Run("should handle JSON as byte array", func(t *testing.T) {
			g := NewGomegaWithT(t)

			jsonData := []byte(`{"name":"test","value":42}`)
			payload := map[string]interface{}{
				"json_payload": jsonData,
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handlerCalled := false
			
			handler := func(msg *message.Message) error {
				handlerCalled = true
				structPayload := msg.Payload.(TestStruct)
				g.Expect(structPayload.Name).To(Equal("test"))
				g.Expect(structPayload.Value).To(Equal(42))
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handlerCalled).To(BeTrue())
		})

		t.Run("should return error for non-map payload", func(t *testing.T) {
			g := NewGomegaWithT(t)

			msg := message.NewMessage(context.Background(), "not a map")

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handler := func(msg *message.Message) error {
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).To(HaveOccurred())
			g.Expect(err).To(MatchError("payload is not a map[string]interface{}"))
		})

		t.Run("should return error for missing map key", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := map[string]interface{}{
				"other_field": "value",
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("missing_key", TestStruct{})
			handler := func(msg *message.Message) error {
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("payload key missing_key not found"))
		})

		t.Run("should return error for invalid data type in map key", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := map[string]interface{}{
				"json_payload": 123, // Invalid type - should be string or []byte
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handler := func(msg *message.Message) error {
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("payload key json_payload is of type"))
			g.Expect(err.Error()).To(ContainSubstring("It must be either a byte array or string"))
		})

		t.Run("should return error for invalid JSON", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := map[string]interface{}{
				"json_payload": "invalid json {",
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handler := func(msg *message.Message) error {
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("invalid character"))
		})

		t.Run("should unmarshal to primitive types", func(t *testing.T) {
			g := NewGomegaWithT(t)

			payload := map[string]interface{}{
				"json_payload": `"simple string"`,
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", "")
			handlerCalled := false
			
			handler := func(msg *message.Message) error {
				handlerCalled = true
				stringPayload, ok := msg.Payload.(string)
				g.Expect(ok).To(BeTrue())
				g.Expect(stringPayload).To(Equal("simple string"))
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handlerCalled).To(BeTrue())
		})

		t.Run("should pass through handler errors", func(t *testing.T) {
			g := NewGomegaWithT(t)

			jsonData := `{"name":"test","value":42}`
			payload := map[string]interface{}{
				"json_payload": jsonData,
			}
			msg := message.NewMessage(context.Background(), payload)

			middleware := subredis.UnmarshallMapPayloadFromJson("json_payload", TestStruct{})
			handlerError := fmt.Errorf("handler error")
			
			handler := func(msg *message.Message) error {
				return handlerError
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msg)

			g.Expect(err).To(Equal(handlerError))
		})
	})

	t.Run("Integration tests", func(t *testing.T) {
		t.Run("should work round-trip marshal and unmarshal", func(t *testing.T) {
			g := NewGomegaWithT(t)

			originalPayload := TestStruct{
				Name:  "integration test",
				Value: 999,
			}
			msg := message.NewMessage(context.Background(), originalPayload)

			// Marshal to JSON map
			marshaller := subredis.MarshallPayloadToJsonMap("data")
			marshalledMap := marshaller(msg)

			// Create new message with marshalled data
			msgWithMap := message.NewMessage(context.Background(), marshalledMap)

			// Unmarshal back to struct
			middleware := subredis.UnmarshallMapPayloadFromJson("data", TestStruct{})
			handler := func(msg *message.Message) error {
				// Verify round-trip worked
				resultPayload, ok := msg.Payload.(TestStruct)
				g.Expect(ok).To(BeTrue())
				g.Expect(resultPayload.Name).To(Equal("integration test"))
				g.Expect(resultPayload.Value).To(Equal(999))
				return nil
			}

			wrappedHandler := middleware(handler)
			err := wrappedHandler(msgWithMap)
			g.Expect(err).NotTo(HaveOccurred())
		})
	})
}