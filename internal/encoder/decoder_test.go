package encoder

import (
	"encoding/csv"
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"io"
	"strings"
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
	g.Describe("The csv Decoder", func() {
		g.It("Should produce json entity from csv input", func() {
			reader, _ := io.Pipe()
			result := make([]byte, 0)
			backend := conf.StorageBackend{CsvConfig: &conf.CsvConfig{
				Header:         true,
				Encoding:       "UTF-8",
				Separator:      ",",
				Order:          []string{"id", "key"},
				SkipRows:       2,
				ValidateFields: false,
			}}
			decoder := &CsvDecoder{backend: backend, reader: reader, logger: nil, since: ""}
			//var input []string
			data := "1\n2\nID,Type,Name,Date,Thing\n991234,1,Tester,,Yes\n88456,abc,Fester,,asd\n"

			csvRead := csv.NewReader(strings.NewReader(data))
			if !backend.CsvConfig.ValidateFields {
				csvRead.FieldsPerRecord = -1
			}

			csvRead.Comma = rune(backend.CsvConfig.Separator[0])
			skip := 0
			for skip < backend.CsvConfig.SkipRows {
				if _, err := csvRead.Read(); err != nil {
					panic(err)
				}
				skip++
			}
			headerLine, err := csvRead.Read()
			records, err := csvRead.ReadAll()
			if err != nil {
				panic(err)
			}
			expected := `[{"id":"@context","namespaces":{"_":"http://example.io/foo/"}},{"deleted":false,"id":"991234","props":{"_:Date":"", "_:ID":"991234","_:Type":"1","_:Name":"Tester","_:Thing":"Yes"},"refs":{}},{"deleted":false,"id":"88456","props":{"_:Date":"","_:ID":"88456","_:Type":"abc","_:Name":"Fester", "_:Thing":"asd"},"refs":{}},{"id":"@continuation","token":""}]`
			config := `{"decode":{"defaultNamespace":"_","namespaces":{"_":"http://example.io/foo/"},"propertyPrefixes":{},"refs":[],"idProperty":"ID"}}`
			json.Unmarshal([]byte(config), &backend)
			result = append(result, []byte("[")...)
			result = append(result, []byte(buildContext(backend.DecodeConfig.Namespaces))...)
			for _, record := range records {
				var entityProps = make(map[string]interface{})
				entityProps, err := decoder.parseRecord(record, headerLine)
				if err != nil {
					return
				}
				var entityBytes []byte
				entityBytes, err = toEntityBytes(entityProps, backend)
				if err != nil {
					return
				}
				result = append(result, append([]byte(","), entityBytes...)...)
			}
			token := ""
			// Add continuation token
			entity := map[string]interface{}{
				"id":    "@continuation",
				"token": token,
			}
			sinceBytes, err := json.Marshal(entity)
			result = append(result, append([]byte(","), sinceBytes...)...)
			result = append(result, []byte("]")...)
			g.Assert(err).IsNil()
			var resultMap []map[string]interface{}
			json.Unmarshal(result, &resultMap)
			var expectedMap []map[string]interface{}
			json.Unmarshal([]byte(expected), &expectedMap)
			g.Assert(err).IsNil()
			g.Assert(resultMap).Eql(expectedMap)
		})
	})
}
