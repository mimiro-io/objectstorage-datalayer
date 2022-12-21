package entity

import (
	"bytes"
	"fmt"
	"github.com/franela/goblin"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"os"
	"strings"
	"testing"
)

func TestParseStream(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The stream parser", func() {
		g.It("Should parse json into entities", func() {
			file, err := os.Open("../../resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			var recorded []*Entity
			var recordCnt int
			err = ParseStream(file, func(entities []*Entity, entityContext *uda.Context) error {
				recorded = append(recorded, entities...)
				recordCnt++
				return nil
			}, 1000, false)
			g.Assert(err).IsNil()
			g.Assert(recordCnt).Eql(1)
			g.Assert(len(recorded)).Eql(3)
			g.Assert(recorded[0].ID).Eql("a:1")
		})
		g.It("Should parse in multiple batches", func() {
			file, err := os.Open("../../resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			var recorded []*Entity
			var recordCnt int
			err = ParseStream(file, func(entities []*Entity, entityContext *uda.Context) error {
				recorded = append(recorded, entities...)
				recordCnt++
				return nil
			}, 2, true)
			g.Assert(err).IsNil()
			g.Assert(recordCnt).Eql(2, "Expected to add two batches of entities because batchsize was smaller than entity count")
			g.Assert(len(recorded)).Eql(3)
			g.Assert(recorded[0].ID).Eql("a:1")
		})
		g.It("Should handle empty input", func() {
			var recorded []*Entity
			err := ParseStream(bytes.NewReader(nil), func(entities []*Entity, entityContext *uda.Context) error {
				recorded = append(recorded, entities...)
				return nil
			}, 1000, false)
			g.Assert(err).IsNil()
			g.Assert(len(recorded)).Eql(0)
		})

		g.Describe("Should accept and sanitize invalid entities", func() {
			tpl := `[ {
				"id": "@context",
				"namespaces": {
					"_": "http://global/"
				}
			} %v ]`
			g.It("unexpected fields", func() {
				var parsed *Entity
				var seen bool
				err := ParseStream(strings.NewReader(fmt.Sprintf(tpl,
					`,{"id": "1", "foo": "bar"}`,
				)), func(entities []*Entity, entityContext *uda.Context) error {
					parsed = entities[0]
					seen = true
					return nil
				}, 1, true)
				g.Assert(err).IsNil()
				g.Assert(seen).IsTrue("Expected ParseStream to emit one enitity. but none was emitted")
				g.Assert(parsed.ID).Eql("1")
				g.Assert(parsed.IsDeleted).Eql(false)
				g.Assert(parsed.Recorded).IsZero()
				g.Assert(len(parsed.Properties)).Eql(0)
				g.Assert(len(parsed.References)).Eql(0)
			})
			g.It("unexpected nested fields", func() {
				var parsed *Entity
				var seen bool
				err := ParseStream(strings.NewReader(fmt.Sprintf(tpl,
					`,{"id": "1",
                           "props": {
                               "foo": { "bar": null},
                               "ok": "value"
                           }}`,
				)), func(entities []*Entity, entityContext *uda.Context) error {
					parsed = entities[0]
					seen = true
					return nil
				}, 1, true)
				g.Assert(err).IsNil()
				g.Assert(seen).IsTrue("Expected ParseStream to emit one enitity. but none was emitted")
				g.Assert(parsed.ID).Eql("1")
				g.Assert(parsed.IsDeleted).Eql(false)
				g.Assert(parsed.Recorded).IsZero()
				g.Assert(len(parsed.Properties)).Eql(2)
				g.Assert(parsed.Properties["ok"]).Eql("value")
				g.Assert(parsed.Properties["foo"]).Eql(map[string]interface{}{"bar": nil}, "Should simple forward invalid nested structures. up to encoders to deal with it")
				g.Assert(len(parsed.References)).Eql(0)
			})
		})
	})
}
