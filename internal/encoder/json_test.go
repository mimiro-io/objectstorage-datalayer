package encoder_test

import (
	"encoding/json"
	"github.com/franela/goblin"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"testing"
)

func TestJson(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The Json Encoder", func() {
		g.It("Should extract properties only if stripprops is set", func() {
			backend := conf.StorageBackend{StripProps: true}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "value 2"}},
			}
			result, err := encodeOnce(backend, entities)
			g.Assert(err).IsNil()
			var m []map[string]interface{}
			err = json.Unmarshal(result, &m)
			g.Assert(err).IsNil()
			g.Assert(len(m)).Eql(2)
			g.Assert(m[1]["key"]).Eql("value 2")
			g.Assert(m[1]["id"]).IsNil()
		})

		g.It("Should write complete entities when stripprops is off", func() {
			backend := conf.StorageBackend{StripProps: false}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "v1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "v2"}},
			}
			result, err := encodeOnce(backend, entities)
			g.Assert(err).IsNil()
			var m []map[string]interface{}
			err = json.Unmarshal(result, &m)
			g.Assert(err).IsNil()
			g.Assert(len(m)).Eql(2)
			g.Assert(m[1]["id"]).Eql("a:2")
			g.Assert(m[0]["props"].(map[string]interface{})["a:key"]).Eql("v1")
		})

		g.It("Should write valid json when combining multiple batches", func() {
			backend := conf.StorageBackend{StripProps: false}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"a:key": "v1"}},
				{ID: "a:2", Properties: map[string]interface{}{"a:key": "v2"}},
			}
			result, err := encodeTwice(backend, entities)
			g.Assert(err).IsNil()
			var m []map[string]interface{}
			err = json.Unmarshal(result, &m)
			g.Assert(err).IsNil()
			g.Assert(len(m)).Eql(4)
			g.Assert(m[1]["id"]).Eql("a:2")
			g.Assert(m[2]["props"].(map[string]interface{})["a:key"]).Eql("v1")
		})
	})
}
