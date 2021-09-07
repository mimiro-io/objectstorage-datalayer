package encoder

import (
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"testing"
)

func TestDecodeLine(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The toEntityBytes function", func() {
		g.It("Should return unstripped entities unmodified", func() {
			input := `{"id":"a:1", "refs":{}, "props":{"a:id": "a:1", "a:name": "Hank"}}`
			expected := input
			backend := conf.StorageBackend{StripProps: false}
			result, err := toEntityBytes([]byte(input), backend)
			g.Assert(err).IsNil()
			g.Assert(string(result)).Eql(expected)
		})
		g.It("Should return stripped entities with configured mappings in place", func() {
			input := `{"id": "1", "name": "Hank"}`
			expected := `{"id":"a:1", "deleted": false, "refs":{}, "props":{"a:id": "a:1", "a:name": "Hank"}}`
			backend := conf.StorageBackend{StripProps: true, DecodeConfig: &conf.DecodeConfig{
				Namespaces:       nil,
				PropertyPrefixes: map[string]string{"id": "a:a", "name": "a"},
				IdProperty:       "id",
			}}
			result, err := toEntityBytes([]byte(input), backend)
			var resultMap map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
		g.It("Should support prefix-free ids", func() {
			input := `{"id": "a:1", "name": "Hank", "hobby": "reading"}`
			expected := `{"id":"a:1","deleted":false, "refs":{}, "props":{"a:id": "a:1", "b:name": "Hank", "hobby": "reading"}}`
			backend := conf.StorageBackend{StripProps: true, DecodeConfig: &conf.DecodeConfig{
				Namespaces:       nil,
				PropertyPrefixes: map[string]string{"id": "a", "name": "b", "hobby": ""},
				IdProperty:       "id",
			}}
			result, err := toEntityBytes([]byte(input), backend)
			var resultMap map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
		g.It("Should put some props in refs if configured", func() {
			input := `{"id": "a:1", "name": "Hank", "hobby": "17"}`
			expected := `{"id":"a:1","deleted":false ,"refs":{"c:hobby":"b:17"}, "props":{"a:id": "a:1", "b:name": "Hank"}}`
			backend := conf.StorageBackend{StripProps: true, DecodeConfig: &conf.DecodeConfig{
				Namespaces:       nil,
				PropertyPrefixes: map[string]string{"id": "a", "name": "b", "hobby": "c:b"},
				IdProperty:       "id",
				Refs:             []string{"hobby"},
			}}
			result, err := toEntityBytes([]byte(input), backend)
			var resultMap map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
	})
}
