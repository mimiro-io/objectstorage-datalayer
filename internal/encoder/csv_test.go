package encoder_test

import (
	"github.com/franela/goblin"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"testing"
)

func TestCSV(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The CSV encoder", func() {
		g.It("Should produce csv from single batch", func() {
			backend := conf.StorageBackend{CsvConfig: &conf.CsvConfig{
				Header:    true,
				Encoding:  "UTF-8",
				Separator: ",",
				Order:     []string{"id", "key"},
			}}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": "1", "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": "2", "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(result)).Eql("id,key\n1,value 1\n2,value 2\n")
		})

		g.It("Should omit csv header if asked to", func() {
			backend := conf.StorageBackend{CsvConfig: &conf.CsvConfig{
				Header:    false,
				Encoding:  "UTF-8",
				Separator: "\t",
				Order:     []string{"id", "key"},
			}}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": "1", "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": "2", "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(result)).Eql("1\tvalue 1\n2\tvalue 2\n")
		})

		g.It("Should produce csv from multiple batches", func() {
			backend := conf.StorageBackend{CsvConfig: &conf.CsvConfig{
				Header:    true,
				Encoding:  "UTF-8",
				Separator: "|",
				Order:     []string{"id", "key"},
			}}
			entities := []*entity.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": "1", "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": "2", "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeTwice(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(string(result)).Eql("id|key\n1|value 1\n2|value 2\n1|value 1\n2|value 2\n")
		})
	})
}
