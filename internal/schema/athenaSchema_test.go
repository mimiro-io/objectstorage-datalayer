package schema

import (
	"github.com/franela/goblin"
	"testing"
)

func TestParquetToAthena(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Given a textual parquet schema, the ParquetToAthena function", func() {
		g.It("Should produce valid athena sql", func() {
			expected := "CREATE EXTERNAL TABLE `a_new_table` (\n" +
				"  `abool` boolean,\n" +
				"  `aint32` int,\n" +
				"  `aint64` bigint,\n" +
				"  `afloat` float,\n" +
				"  `adouble` double,\n" +
				"  `abytes` binary,\n" +
				"  `astring` string,\n" +
				"  `adate` date,\n" +
				"  `atime` timestamp )\n" +
				"STORED AS PARQUET\n" +
				"LOCATION\n" +
				"  's3://bucket/folder/'\n" +
				"TBLPROPERTIES (\n" +
				"  'parquet.compression'='SNAPPY')"

			schemaString := `message test_schema {
					required boolean abool;
					required int32 aint32;
					required int64 aint64;
					required float afloat;
					required double adouble;
					required binary abytes;
					required binary astring (STRING);
					required int32 adate (DATE);
					required int64 atime (TIME(NANOS,true));
			}`
			athenaGenerator, err := NewParquetAthenaSqlBuilder(
				"a.new-table",
				schemaString,
				"s3://bucket/folder/")
			g.Assert(err).IsNil()
			result, err := athenaGenerator.WithSnappyCompression().Build()
			g.Assert(err).IsNil()
			g.Assert(result).Eql(expected)
		})

		g.It("Should add partition block to sql if configured", func() {
			expected := "CREATE EXTERNAL TABLE `a_new_table` (\n" +
				"  `id` int )\n" +
				"PARTITIONED BY (\n" +
				"  year STRING,\n" +
				"  month STRING,\n" +
				"  day STRING)\n" +
				"STORED AS PARQUET\n" +
				"LOCATION\n" +
				"  's3://bucket/folder/'\n" +
				"TBLPROPERTIES (\n" +
				"  'parquet.compression'='SNAPPY')"

			schemaString := `message test_schema {
					required int32 id;
			}`
			athenaGenerator, err := NewParquetAthenaSqlBuilder(
				"a.new-table",
				schemaString,
				"s3://bucket/folder/")
			g.Assert(err).IsNil()
			result, err := athenaGenerator.
				WithSnappyCompression().
				WithPartitioning("year", "month", "day").
				Build()
			g.Assert(err).IsNil()
			g.Assert(result).Eql(expected)
		})
	})
}
