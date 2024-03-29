package encoder_test

import (
	"bytes"
	"github.com/franela/goblin"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"io/ioutil"
	"testing"
	"time"
)

func TestParquet(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The Parquet Encoder", func() {
		g.It("Should produce parquet file from single batch", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required int64 id;
					required binary key (STRING);
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": 1, "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": 2, "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(348)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(2))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(1))
			g.Assert(row["key"]).Eql([]byte("value 1"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(2))
			g.Assert(row["key"]).Eql([]byte("value 2"))
		})

		g.It("Should process files with missing column values if field is optional", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required int64 id;
					required binary key (STRING);
					optional binary name (STRING);
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": 1, "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": 2, "a:key": "value 2", "a:name": "Bob"}},
				{ID: "a:3", Properties: map[string]interface{}{"b:id": 3, "a:key": "value 3", "a:name": nil}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(473)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key", "name")
			g.Assert(err).IsNil()

			g.Assert(pqReader.NumRows()).Eql(int64(3))
			row, _ := pqReader.NextRow()
			g.Assert(len(row)).Eql(2)
			g.Assert(row["id"]).Eql(int64(1))
			g.Assert(row["key"]).Eql([]byte("value 1"))

			row, _ = pqReader.NextRow()
			g.Assert(len(row)).Eql(3)
			g.Assert(row["id"]).Eql(int64(2))
			g.Assert(row["key"]).Eql([]byte("value 2"))
			g.Assert(row["name"]).Eql([]byte("Bob"))

			row, _ = pqReader.NextRow()
			g.Assert(len(row)).Eql(2)
			g.Assert(row["id"]).Eql(int64(3))
			g.Assert(row["key"]).Eql([]byte("value 3"))

		})

		g.It("Should produce parquet file from multi batch", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required int64 id;
					required binary key (STRING);
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": 1, "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": 2, "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeTwice(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(348)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(4))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(1))
			g.Assert(row["key"]).Eql([]byte("value 1"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(2))
			g.Assert(row["key"]).Eql([]byte("value 2"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(1))
			g.Assert(row["key"]).Eql([]byte("value 1"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(2))
			g.Assert(row["key"]).Eql([]byte("value 2"))
		})

		g.It("Should support a selection of parquet datatypes", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required boolean abool;
					required int32 aint32;
					required int64 aint64;
					required float afloat;
					required double adouble;
					required binary abytes;
					required binary astring (STRING);
					required int32 adate (DATE);
					required int64 atime (TIME(NANOS,true));
				}`,
			}}
			/*
					required fixed_len_byte_array(5) five_bytes;

					required int96 aint96;
				required fixed_len_byte_array(16) uuid (UUID);
				required binary list (LIST);
				required binary map (MAP);
				required binary enum (ENUM);
				required binary decimal (DECIMAL);
				required int64 i (INT);
			*/
			oslo, _ := time.LoadLocation("Europe/Oslo")
			date := time.Date(2021, 12, 31, 23, 30, 59, 50, oslo)
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{
					"b:abool":  true,
					"a:aint32": 1000,
					"a:aint64": 2000,
					//"a:aint96":      3000,
					"a:afloat":  3.0,
					"a:adouble": 6.0,
					"a:abytes":  []byte("a bunch of bytes"),
					//"a:afive_bytes": []byte{byte(1),byte(2),byte(3),byte(4),byte(5)},
					"a:astring": "a string",
					"a:adate":   date,

					"a:atime": date.Local(),
					/*	"a:uuid": uuid.MustParse("ef346158-c858-11eb-b8bc-0242ac130003"),
						"a:list": []string{"e1", "e2"},
						"a:map": map[string]int{"e1":1, "e2":2},
						"a:enum": [2]string{"a1", "a2"},
						"a:bson": "{\"key\":\"val\"}",
						"a:decimal": "2,14",
						"a:i": 5000,*/
				}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(1239)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "abool", "aint32", "aint64", "afloat", "adouble", "abytes",
				"five_bytes", "astring", "adate", "atime")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(1))
			row, _ := pqReader.NextRow()
			//t.Log(row)
			g.Assert(row["abool"]).Eql(true)
			g.Assert(row["aint32"]).Eql(int32(1000))
			g.Assert(row["aint64"]).Eql(int64(2000))
			g.Assert(row["afloat"]).Eql(float32(3))
			g.Assert(row["adouble"]).Eql(float64(6))
			g.Assert(row["abytes"]).Eql([]byte("a bunch of bytes"))
			g.Assert(row["astring"]).Eql([]byte("a string"))
			daysSince1970 := row["adate"].(int32)
			dur := -1 * 24 * time.Hour * time.Duration(daysSince1970)
			g.Assert(date.Truncate(24 * time.Hour).Add(dur).Unix()).Eql(int64(0))
			g.Assert(row["atime"]).Eql(date.UnixNano())
		})

		g.It("Should flush after threshold is reached", func() {
			// 1000 byte threshold should result in single collected rowgroup - therefore compact file
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				FlushThreshold: 1000,
				SchemaDefinition: `message test_schema {
					required int64 id;
					required binary key (STRING);
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": 1, "a:key": "AAAAAAALKJASLDKJDLAKJSLDJALSDJALSKDJLAJSDLKJAALSKDJALSDJLASJKDADLKJALSJKDLKAJSD"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": 2, "a:key": "asodfij askdjf aølsdji føalskjdf ølaskjd følaksjd følkjas dløfj elkajweløkfja slødjkf"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			res, err := encodeTwice(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(res)).Eql(489)

			// 100 byte threshold should result in separate rowgroup per write - therefore larger file
			backend.ParquetConfig.FlushThreshold = 100
			res, err = encodeTwice(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(res)).Eql(892)
		})

		g.It("Should produce parquet file with resolved refs", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required int64 id;
					required binary key (STRING);
					required binary ref (STRING);
				}`,
			},
				ResolveNamespace: true}
			entities := []*uda.Entity{
				{ID: "a:1",
					Properties: map[string]interface{}{"b:id": 1, "a:key": "value 1"},
					References: map[string]interface{}{"a:ref": "ns10:123"}},
				{ID: "a:2",
					Properties: map[string]interface{}{"b:id": 2, "a:key": "value 2"},
					References: map[string]interface{}{"a:ref": "ns10:456"}},
			}
			entityContext := uda.Context{ID: "@context",
				Namespaces: map[string]string{"ns10": "http://data.example.io/"}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(488)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key", "ref")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(2))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(1))
			g.Assert(row["key"]).Eql([]byte("value 1"))
			g.Assert(row["ref"]).Eql([]byte("http://data.example.io/123"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql(int64(2))
			g.Assert(row["key"]).Eql([]byte("value 2"))
			g.Assert(row["ref"]).Eql([]byte("http://data.example.io/456"))
		})

		g.It("Should produce parquet file with resolved id", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required binary id (STRING);
					required binary key (STRING);
					required binary ref (STRING);
				}`,
			},
				ResolveNamespace: true}
			entities := []*uda.Entity{
				{ID: "ns10:1",
					Properties: map[string]interface{}{"a:key": "value 1"},
					References: map[string]interface{}{"a:ref": "ns10:123"}},
				{ID: "ns10:2",
					Properties: map[string]interface{}{"a:key": "value 2"},
					References: map[string]interface{}{"a:ref": "ns10:456"}},
			}
			entityContext := uda.Context{ID: "@context",
				Namespaces: map[string]string{"ns10": "http://data.example.io/"}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(471)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key", "ref")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(2))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("http://data.example.io/1"))
			g.Assert(row["key"]).Eql([]byte("value 1"))
			g.Assert(row["ref"]).Eql([]byte("http://data.example.io/123"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("http://data.example.io/2"))
			g.Assert(row["key"]).Eql([]byte("value 2"))
			g.Assert(row["ref"]).Eql([]byte("http://data.example.io/456"))
		})

		g.It("Should produce parquet file with delete flag", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required binary id (STRING);
					required binary key (STRING);
					required boolean deleted;
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "ns10:1",
					Properties: map[string]interface{}{"a:key": "value 1"},
					IsDeleted:  false,
				},
				{ID: "ns10:2",
					Properties: map[string]interface{}{"a:key": "value 2"},
					IsDeleted:  true,
				},
			}
			entityContext := uda.Context{ID: "@context",
				Namespaces: map[string]string{"ns10": "http://data.example.io/"}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(399)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key", "deleted")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(2))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("ns10:1"))
			g.Assert(row["key"]).Eql([]byte("value 1"))
			g.Assert(row["deleted"]).Eql(false)

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("ns10:2"))
			g.Assert(row["key"]).Eql([]byte("value 2"))
			g.Assert(row["deleted"]).Eql(true)
		})

		g.It("Should produce parquet file with recorded timestamp", func() {
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required binary id (STRING);
					required binary key (STRING);
					required binary recorded (STRING);
				}`,
			}}
			entities := []*uda.Entity{
				{ID: "ns10:1",
					Properties: map[string]interface{}{"a:key": "value 1"},
					Recorded:   "1666349374523225600",
				},
				{ID: "ns10:2",
					Properties: map[string]interface{}{"a:key": "value 2"},
					Recorded:   "1660804179145791488",
				},
			}
			entityContext := uda.Context{ID: "@context",
				Namespaces: map[string]string{"ns10": "http://data.example.io/"}}
			result, err := encodeOnce(backend, entities, &entityContext)
			g.Assert(err).IsNil()
			g.Assert(len(result)).Eql(473)
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(result), "id", "key", "recorded")
			g.Assert(err).IsNil()
			//t.Logf("Schema: %s", pqReader.GetSchemaDefinition())

			g.Assert(pqReader.NumRows()).Eql(int64(2))
			row, _ := pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("ns10:1"))
			g.Assert(row["key"]).Eql([]byte("value 1"))
			g.Assert(row["recorded"]).Eql([]byte("1666349374523225600"))

			row, _ = pqReader.NextRow()
			g.Assert(row["id"]).Eql([]byte("ns10:2"))
			g.Assert(row["key"]).Eql([]byte("value 2"))
			g.Assert(row["recorded"]).Eql([]byte("1660804179145791488"))
		})
	})
	g.Describe("The Parquet Decoder", func() {
		g.It("Should produce a complete fixed width parquet", func() {
			expected := `[{"id":"@context","namespaces":{"_":"http://example.io/foo/"}},{"deleted":false,"id":"1","props":{"_:id":"1","_:key":"value 1"},"refs":{}},{"deleted":false,"id":"2","props":{"_:id":"2","_:key":"value 2"},"refs":{}},{"id":"@continuation","token":""}]`
			backend := conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				SchemaDefinition: `message test_schema {
					required binary id (STRING);
					required binary key (STRING);
				}`,
			},
				DecodeConfig: &conf.DecodeConfig{
					IdProperty:       "id",
					DefaultNamespace: "_",
					Namespaces:       map[string]string{"_": "http://example.io/foo/"}}}

			entities := []*uda.Entity{
				{ID: "a:1", Properties: map[string]interface{}{"b:id": "1", "a:key": "value 1"}},
				{ID: "a:2", Properties: map[string]interface{}{"b:id": "2", "a:key": "value 2"}},
			}
			entityContext := uda.Context{ID: "@context", Namespaces: map[string]string{}}
			result, err := encodeOnce(backend, entities, &entityContext)
			reader, err := decodeOnce(backend, result)
			g.Assert(err).IsNil()
			all, err := ioutil.ReadAll(reader)
			g.Assert(err).IsNil()
			g.Assert(len(all)).Eql(249)
			g.Assert(string(all)).Eql(expected)
		})
	})
}
