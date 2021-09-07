package schema

import (
	"errors"
	"fmt"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"regexp"
	"strings"
)

// https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html
type parquetToAthenaBuilder struct {
	schema          *parquetschema.SchemaDefinition
	TableProperties []string
	PartitionFields []string
	name            string
	location        string
}

func NewParquetAthenaSqlBuilder(tableName string, schema string, targetLocation string) (*parquetToAthenaBuilder, error) {
	schemaDef, err := parquetschema.ParseSchemaDefinition(schema)
	if err != nil {
		return nil, err
	}
	return &parquetToAthenaBuilder{
		name:     athenaName(tableName),
		schema:   schemaDef,
		location: targetLocation,
	}, nil
}

func athenaName(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]`)
	return strings.ToLower(re.ReplaceAllString(name, "_"))
}

func (g *parquetToAthenaBuilder) WithSnappyCompression() *parquetToAthenaBuilder {
	g.TableProperties = append(g.TableProperties, "'parquet.compression'='SNAPPY'")
	return g
}

func (g *parquetToAthenaBuilder) Build() (result string, err error) {
	var sb strings.Builder
	// header line
	_, err = fmt.Fprintf(&sb, "CREATE EXTERNAL TABLE `%v`", g.name)

	// columns
	if len(g.schema.RootColumn.Children) > 0 {
		sb.WriteString(" (")
		for i, se := range g.schema.RootColumn.Children {
			colType, err2 := parqetTypeToAthenaType(se.SchemaElement)
			if err2 != nil {
				return "", err2
			}
			colName := se.SchemaElement.Name
			_, err = fmt.Fprintf(&sb, "%v`%v` %v", delimForRow(i), colName, colType)
		}
		sb.WriteString(" )")
	}
	//partition fields
	if len(g.PartitionFields) > 0 {
		sb.WriteString("\nPARTITIONED BY (")
		for i, pf := range g.PartitionFields {
			_, err = fmt.Fprintf(&sb, "%v%v STRING", delimForRow(i), pf)
		}
		sb.WriteString(")")

	}

	// tail block
	sb.WriteString("\nSTORED AS PARQUET")

	if g.location != "" {
		_, err = fmt.Fprintf(&sb, "\nLOCATION\n  '%v'", g.location)
	} else {
		err = errors.New("location required")
	}

	if len(g.TableProperties) > 0 {
		sb.WriteString("\nTBLPROPERTIES (")
		for i, tp := range g.TableProperties {
			_, err = fmt.Fprintf(&sb, "%v%v", delimForRow(i), tp)
		}
		sb.WriteString(")")

	}
	result = sb.String()
	return
}

func delimForRow(i int) string {
	delim := ",\n  "
	if i == 0 {
		delim = "\n  "
	}
	return delim
}

func parqetTypeToAthenaType(s *parquet.SchemaElement) (string, error) {
	if s.IsSetLogicalType() {
		if s.LogicalType.IsSetSTRING() {
			return "string", nil
		}
		if s.LogicalType.IsSetDATE() {
			return "date", nil
		}
		if s.LogicalType.IsSetTIME() {
			return "timestamp", nil
		}
		return "", errors.New(fmt.Sprintf("unsupported parquet type: %+v", s))
	}
	if *s.Type == parquet.Type_BOOLEAN {
		return "boolean", nil
	}
	if *s.Type == parquet.Type_INT64 {
		return "bigint", nil
	}
	if *s.Type == parquet.Type_INT32 {
		return "int", nil
	}
	if *s.Type == parquet.Type_FLOAT {
		return "float", nil
	}
	if *s.Type == parquet.Type_DOUBLE {
		return "double", nil
	}
	if *s.Type == parquet.Type_BYTE_ARRAY {
		return "binary", nil
	}
	return "", errors.New(fmt.Sprintf("unsupported parquet type: %+v", s))
}

func (g *parquetToAthenaBuilder) WithPartitioning(partitionFields ...string) *parquetToAthenaBuilder {
	g.PartitionFields = partitionFields
	return g
}
