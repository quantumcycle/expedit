package unmarshaller

import (
	"encoding/json"
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	"reflect"
	"strings"
)

type JSONUnmarshaller struct {
	declaredTypes map[string]reflect.Type
}

func (m *JSONUnmarshaller) AddType(name string, t any) {
	m.declaredTypes[name] = reflect.TypeOf(t)
}

func (m *JSONUnmarshaller) CreateBytesUnmarshaller() func(name string, payload []byte) (message.Payload, error) {
	return func(name string, payload []byte) (message.Payload, error) {
		t, ok := m.declaredTypes[name]
		if !ok {
			var possibleTypes []string
			for t, _ := range m.declaredTypes {
				possibleTypes = append(possibleTypes, t)
			}
			return nil, fmt.Errorf("unregistered unmarshalling type %q. possible values are [%s]",
				name, strings.Join(possibleTypes, ","))
		}
		v := reflect.New(t).Interface()
		if err := json.Unmarshal(payload, v); err != nil {
			return nil, err
		}
		return v, nil
	}
}
