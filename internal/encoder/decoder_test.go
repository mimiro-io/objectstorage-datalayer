package encoder

import (
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"io"
	"testing"
)

func TestDecodeLine(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The toEntityBytes function", func() {
		g.It("Should return stripped entities with configured mappings in place", func() {
			input := `{"id": "1", "name": "Hank"}`
			expected := `{"id":"a:1", "deleted": false, "refs":{}, "props":{"a:id": "a:1", "a:name": "Hank"}}`
			backend := conf.StorageBackend{StripProps: true, DecodeConfig: &conf.DecodeConfig{
				Namespaces:       nil,
				PropertyPrefixes: map[string]string{"id": "a:a", "name": "a"},
				IdProperty:       "id",
			}}
			var m map[string]interface{}
			json.Unmarshal([]byte(input), &m)
			result, err := toEntityBytes(m, backend)
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
			var m map[string]interface{}
			json.Unmarshal([]byte(input), &m)
			result, err := toEntityBytes(m, backend)
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
			var m map[string]interface{}
			json.Unmarshal([]byte(input), &m)
			result, err := toEntityBytes(m, backend)
			var resultMap map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
	})
	g.Describe("The FlatFile decoder", func() {
		g.It("Should produce a valid UDA entity from fixed width flat file line", func() {
			input := `JOHNSMITH01021990987654321`
			expected := `{"deleted":false,"id":"987654321","props":{"_:born":"01021990","_:firstname":"JOHN","_:lastname":"SMITH","_:phone":"987654321"},"refs":{}}`
			config := `{"flatFile":{"fields":{"phone":{"substring":[[17,26]]},"firstname":{"substring":[[0,4]]},"lastname":{"substring":[[4,9]]},"born":{"substring":[[9,17]]}}},"decode":{"defaultNamespace":"_","namespaces":{"_":"http://test.example.io/person/info/"},"idProperty":"phone"}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)
			reader, _ := io.Pipe()
			decoder := &FlatFileDecoder{backend: backend, reader: reader, logger: nil, since: ""}
			m, err := decoder.ParseLine(input, backend.FlatFileConfig)
			g.Assert(err).IsNil()

			result, err := toEntityBytes(m, backend)
			var resultMap map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
	})
}
