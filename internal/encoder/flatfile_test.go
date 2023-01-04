package encoder_test

import (
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"io/ioutil"
	"testing"
)

func TestFlatFile(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The FlatFile Encoder", func() {
		g.It("Should produce a complete fixed width flatfile from single batch", func() {
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:foo": "99", "a:bar": "aaa"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:foo": "88", "a:bar": "bbb"}},
			}
			expected := []byte("99aaa\n88bbb\n")

			config := `{"flatFile":{"fieldOrder":["foo","bar"],"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,5]]}}}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			readResult, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(readResult)).Eql(string(expected))
		})
		g.It("Should add spaces when field config allocates more character positions than the input value", func() {
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:foo": "9", "a:bar": "aaa"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:foo": "88", "a:bar": "bbb"}},
			}
			expected := []byte("9 aaa\n88bbb\n")

			config := `{"flatFile":{"fieldOrder":["foo","bar"],"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,5]]}}}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			readResult, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(readResult)).Eql(string(expected))
		})
		g.It("Should return substring of value according to field config if value is longer than allocated characters", func() {
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:foo": "99", "a:bar": "aaa"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:foo": "88", "a:bar": "bbb"}},
			}
			expected := []byte("99aa\n88bb\n")

			config := `{"flatFile":{"fieldOrder":["foo","bar"],"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,4]]}}}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			readResult, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(readResult)).Eql(string(expected))
		})
		g.It("Should count number of utf8 characters in string fields", func() {
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:foo": "99", "a:bår": "aååå"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:foo": "88", "a:bår": "Ñ¢a"}},
				{ID: "a:3", Properties: map[string]interface{}{"a:foo": "77", "a:bår": "§"}},
			}
			expected := "99aå\n88Ñ¢\n77§ \n"

			config := `{"flatFile":{"fieldOrder":["foo","bår"],"fields":{"foo":{"substring":[[0,2]]},"bår":{"substring":[[2,4]]}}}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			readResult, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(readResult)).Eql(expected)
		})
		g.It("Should produce timestamp according to date layout in field config", func() {
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:foo": "99", "a:bar": "aaa", "a:date": "2021-11-05T00:00:00Z"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:foo": "88", "a:bar": "bbb", "a:date": "2021-12-05T00:00:00Z"}},
			}
			expected := []byte("99aaa20211105\n88bbb20211205\n")

			config := `{"flatFile":{"fieldOrder":["foo","bar","date"],"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,5]]},"date":{"substring":[[5,13]],"type":"date","dateLayout":"20060102"}}}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			readResult, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(readResult)).Eql(string(expected))
		})
	})
	g.Describe("The FlatFile Decoder", func() {
		g.It("Should produce integer value in json entity according to field config", func() {
			entities := []byte("99123\n88456\n")

			expected := `[{"id":"@context","namespaces":{"_":"http://example.io/foo/"}},{"deleted":false,"id":"99","props":{"_:bar":123,"_:foo":"99"},"refs":{}},{"deleted":false,"id":"88","props":{"_:bar":456,"_:foo":"88"},"refs":{}},{"id":"@continuation","token":""}]`

			config := `{"flatFile":{"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,5]],"type":"integer"}}},"decode":{"defaultNamespace":"_","namespaces":{"_":"http://example.io/foo/"},"propertyPrefixes":{},"refs":[],"idProperty":"foo"}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			reader, err := decodeOnce(backend, entities)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(243)
			g.Assert(string(all)).Eql(expected)
		})
		g.It("Should produce float value in json entity according to field config", func() {
			entities := []byte("99123\n88456\n")

			expected := `[{"id":"@context","namespaces":{"_":"http://example.io/foo/"}},{"deleted":false,"id":"99","props":{"_:bar":1.23,"_:foo":"99"},"refs":{}},{"deleted":false,"id":"88","props":{"_:bar":4.56,"_:foo":"88"},"refs":{}},{"id":"@continuation","token":""}]`

			config := `{"flatFile":{"fields":{"foo":{"substring":[[0,2]]},"bar":{"substring":[[2,5]],"type":"float", "decimals": 2}}},"decode":{"defaultNamespace":"_","namespaces":{"_":"http://example.io/foo/"},"propertyPrefixes":{},"refs":[],"idProperty":"foo"}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			reader, err := decodeOnce(backend, entities)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(245)
			g.Assert(string(all)).Eql(expected)
		})
		g.It("Should produce date value in json entity according to field config", func() {
			entities := []byte("9920211105\n8820211205\n")

			expected := `[{"id":"@context","namespaces":{"_":"http://example.io/foo/"}},{"deleted":false,"id":"99","props":{"_:date":"2021-11-05T00:00:00Z","_:foo":"99"},"refs":{}},{"deleted":false,"id":"88","props":{"_:date":"2021-12-05T00:00:00Z","_:foo":"88"},"refs":{}},{"id":"@continuation","token":""}]`

			config := `{"flatFile":{"fields":{"foo":{"substring":[[0,2]]},"date":{"substring":[[2,10]],"type":"date","dateLayout":"20060102"}}},"decode":{"defaultNamespace":"_","namespaces":{"_":"http://example.io/foo/"},"propertyPrefixes":{},"refs":[],"idProperty":"foo"}}`
			var backend conf.StorageBackend
			json.Unmarshal([]byte(config), &backend)

			reader, err := decodeOnce(backend, entities)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(283)
			g.Assert(string(all)).Eql(expected)
		})
	})
}
