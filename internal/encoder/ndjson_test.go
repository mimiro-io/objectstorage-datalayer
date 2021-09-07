package encoder_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"io/ioutil"
	"testing"
)

func TestNDJson(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The NDJson Encoder", func() {
		g.It("Should produce ndjson with complete entities from single batch", func() {
			backend := conf.StorageBackend{AthenaCompatible: true}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "value 2"}},
			}
			readResult, err := encodeOnce(backend, entities)
			g.Assert(err).IsNil()
			scanner := bufio.NewScanner(bytes.NewReader(readResult))
			var result []map[string]interface{}
			for scanner.Scan() {
				var m map[string]interface{}
				err = json.Unmarshal(scanner.Bytes(), &m)
				result = append(result, m)
			}
			g.Assert(result).IsNotNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(result[0]["id"]).Eql("a:1")
			g.Assert(result[1]["props"].(map[string]interface{})["a:key"]).Eql("value 2")
		})

		g.It("Should produce ndjson with props only if asked to", func() {
			backend := conf.StorageBackend{AthenaCompatible: true, StripProps: true}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "value 2"}},
			}
			readResult, err := encodeOnce(backend, entities)
			g.Assert(err).IsNil()
			scanner := bufio.NewScanner(bytes.NewReader(readResult))
			var result []map[string]interface{}
			for scanner.Scan() {
				var m map[string]interface{}
				err = json.Unmarshal(scanner.Bytes(), &m)
				result = append(result, m)
			}
			g.Assert(result).IsNotNil()
			g.Assert(len(result)).Eql(2)
			g.Assert(len(result[0])).Eql(1, "expect only one property: key")
			g.Assert(result[0]["key"]).Eql("value 1")
			g.Assert(result[1]["key"]).Eql("value 2")
		})

		g.It("Should produce ndjson from multiple batches", func() {
			backend := conf.StorageBackend{AthenaCompatible: true, StripProps: true}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "value 2"}},
			}
			readResult, err := encodeTwice(backend, entities)
			g.Assert(err).IsNil()
			scanner := bufio.NewScanner(bytes.NewReader(readResult))
			var result []map[string]interface{}
			for scanner.Scan() {
				var m map[string]interface{}
				err = json.Unmarshal(scanner.Bytes(), &m)
				result = append(result, m)
			}
			g.Assert(result).IsNotNil()
			g.Assert(len(result)).Eql(4)
			g.Assert(len(result[0])).Eql(1, "expect only one property: key")
			g.Assert(result[0]["key"]).Eql("value 1")
			g.Assert(result[1]["key"]).Eql("value 2")
			g.Assert(result[2]["key"]).Eql("value 1")
			g.Assert(result[3]["key"]).Eql("value 2")
		})
	})

	g.Describe("The ndjson decoder", func() {
		g.It("Should decode unstripped files", func() {
			fileBytes, err := ioutil.ReadFile("../../resources/test/data/unstripped.ndjson")
			g.Assert(err).IsNil()
			backend := conf.StorageBackend{
				AthenaCompatible: true,
				StripProps:       false,
				DecodeConfig: &conf.DecodeConfig{
					Namespaces: map[string]string{"a": "http://domain/a", "b": "https://domain/b"}}}
			reader, err := decodeOnce(backend, fileBytes)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(537)
		})

		g.It("Should decode stripped files", func() {
			fileBytes, err := ioutil.ReadFile("../../resources/test/data/stripped.ndjson")
			g.Assert(err).IsNil()
			backend := conf.StorageBackend{
				AthenaCompatible: true,
				StripProps:       true,
				DecodeConfig: &conf.DecodeConfig{
					Namespaces: map[string]string{
						"a": "http://domain/a",
						"b": "https://domain/b"},
					PropertyPrefixes: map[string]string{
						"id":         "a",
						"surname":    "a",
						"vaccinated": "a",
						"firstname":  "a",
						"address":    "b",
						"age":        "a"},
					IdProperty: "id",
					Refs:       []string{"address"}}}
			reader, err := decodeOnce(backend, fileBytes)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(537)
		})
	})
}
