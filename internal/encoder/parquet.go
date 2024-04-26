package encoder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"io"
	"strings"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
)

type ParquetEncoder struct {
	backend        conf.StorageBackend
	writer         *io.PipeWriter
	open           bool
	pqWriter       *goparquet.FileWriter
	schemaDef      *parquetschema.SchemaDefinition
	flushThreshold int64
	logger         *zap.SugaredLogger
}

func (enc *ParquetEncoder) Close() error {
	if !enc.open {
		return fmt.Errorf("nothing was added to file. Cannot write empty parquet file")
	}
	size := enc.pqWriter.CurrentRowGroupSize()
	enc.logger.Debugf("Finalizing parquet stream. flushing remaining rows with size: %v", size)
	err := enc.pqWriter.Close()
	if err != nil {
		err2 := enc.writer.CloseWithError(err)
		if err2 != nil {
			return err2
		}
		return err
	}
	enc.logger.Infof("Closed parquet stream. total flushed: %v", enc.pqWriter.CurrentFileSize())

	return enc.writer.Close()
}

func (enc *ParquetEncoder) Write(entities []*uda.Entity) (int, error) {
	if !enc.open {
		err := enc.Open()
		if err != nil {
			return 0, err
		}
	}

	for _, e := range entities {
		props := uda.StripProps(e)
		refs := uda.StripRefs(e)
		row := make(map[string]interface{})
		for _, c := range enc.schemaDef.RootColumn.Children {
			if c.SchemaElement.Name == "deleted" {
				i, err := convertType(e.IsDeleted, c.SchemaElement.Type, c.SchemaElement.LogicalType)
				if err != nil {
					return 0, err
				}
				row[c.SchemaElement.Name] = i
			}

			if c.SchemaElement.Name == "recorded" {
				i, err := convertType(e.Recorded, c.SchemaElement.Type, c.SchemaElement.LogicalType)
				if err != nil {
					return 0, err
				}
				row[c.SchemaElement.Name] = i
			}

			val, ok := props[c.SchemaElement.Name]
			if !ok {
				val, ok = refs[c.SchemaElement.Name]
				if ok && val != nil {
					val, _ = concatStringSlice(val)
				}
			}
			if ok && val != nil {
				i, err := convertType(val, c.SchemaElement.Type, c.SchemaElement.LogicalType)
				if err != nil {
					return 0, err
				}
				row[c.SchemaElement.Name] = i
			}

			_, ok = row[c.SchemaElement.Name]
			if c.SchemaElement.Name == "id" && !ok { // Allows for overriding entity id with id in props
				i, err := convertType(e.ID, c.SchemaElement.Type, c.SchemaElement.LogicalType)
				if err != nil {
					return 0, err
				}
				row[c.SchemaElement.Name] = i
			}
		}
		if err := enc.pqWriter.AddData(row); err != nil {
			return 0, err
		}
	}
	written := enc.pqWriter.CurrentRowGroupSize()
	if written > enc.flushThreshold {
		err := enc.pqWriter.FlushRowGroup()
		if err != nil {
			return 0, err
		}
		flushed := written - enc.pqWriter.CurrentRowGroupSize()
		enc.logger.Debugf("Flushed %v parquet bytes to underlying writer. flushed in total: %v", flushed, enc.pqWriter.CurrentFileSize())
		return int(flushed), nil
	}
	return 0, nil
}

func concatStringSlice(value any) (string, bool) {
	var output string
	success := true
	var values []string
	switch value.(type) {
	case []string:
		for _, val := range value.([]string) {
			values = append(values, val)
		}
		output = strings.Join(values, ",")
	case []any:
		for _, val := range value.([]any) {
			values = append(values, val.(string))
		}
		output = strings.Join(values, ",")
	default:
		output, success = value.(string)
	}
	return output, success
}

func convertType(val interface{}, t *parquet.Type, logicalType *parquet.LogicalType) (interface{}, error) {
	switch *t {
	case parquet.Type_BOOLEAN:
		r, ok := val.(bool)
		if !ok {
			return nil, errors.New(fmt.Sprintf("could not convert %+v to bool", val))
		}
		return r, nil
	case parquet.Type_INT32:
		if logicalType == nil {
			return int32(val.(int)), nil
		}
		if logicalType.IsSetDATE() {
			d := val.(time.Time)
			duration := d.Sub(time.Unix(0, 0))
			return int32(duration.Hours() / 24), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	case parquet.Type_INT64:
		if logicalType == nil {
			switch val.(type) {
			case float64:
				return int64(val.(float64)), nil
			default:
				return int64(val.(int)), nil
			}
		}
		if logicalType.IsSetTIME() {
			d := val.(time.Time)
			return d.UnixNano(), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	case parquet.Type_FLOAT:
		return float32(val.(float64)), nil
	case parquet.Type_DOUBLE:
		return val.(float64), nil
	case parquet.Type_BYTE_ARRAY:
		if logicalType == nil {
			return val.([]byte), nil
		}
		if logicalType.IsSetSTRING() {
			r, ok := concatStringSlice(val)
			if !ok {
				return nil, errors.New(fmt.Sprintf("could not convert %+v to string", val))
			}
			return []byte(r), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	default:
		return nil, errors.New(fmt.Sprintf("unsupported datatype: %+v", t))
	}
}

func (enc *ParquetEncoder) CloseWithError(err error) error {
	if enc.pqWriter != nil {
		_ = enc.pqWriter.Close()
	}
	return enc.writer.CloseWithError(err)
}

func (enc *ParquetEncoder) Open() error {
	schemaDef, err := parquetschema.ParseSchemaDefinition(enc.backend.ParquetConfig.SchemaDefinition)
	if err != nil {
		enc.logger.Errorf("Failed to parse parquet schema: %s", err)
		return err
	}

	enc.flushThreshold = 1 * 1024 * 1024 //1MB conservative default. For optimal parquet files 512-1024MB is recommended but this will blow memory for us
	if enc.backend.ParquetConfig.FlushThreshold > 0 {
		enc.flushThreshold = enc.backend.ParquetConfig.FlushThreshold
	}
	enc.logger.Infof("writing parquet files with flushThreshold %v", enc.flushThreshold)
	enc.schemaDef = schemaDef

	enc.pqWriter = goparquet.NewFileWriter(enc.writer, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.WithSchemaDefinition(schemaDef), goparquet.WithCreator("objectstorage-datalayer"))

	enc.open = true

	return nil
}

//#######################################################################//
//--------------------------------READ-----------------------------------//
//#######################################################################//

// ParquetDecoder ********************** DECODER ****************************************/
type ParquetDecoder struct {
	backend  conf.StorageBackend
	logger   *zap.SugaredLogger
	reader   *io.PipeReader
	scanner  *bufio.Scanner
	open     bool
	closed   bool
	overhang []byte
	since    string
	fullSync bool
	pqReader *goparquet.FileReader
}

func (d *ParquetDecoder) Read(p []byte) (n int, err error) {
	buf := make([]byte, 0, len(p))
	var done bool
	if len(d.overhang) > 0 {
		buf = append(buf, d.overhang...)
		d.overhang = nil
	}

	if !d.open {
		d.open = true
		allBytes, err := io.ReadAll(d.reader)
		if err != nil {
			return n, err
		}
		readSeeker := bytes.NewReader(allBytes)
		d.pqReader, err = goparquet.NewFileReader(readSeeker)
		if err != nil {
			return n, err
		}
		//start json array and add context as first entity
		buf = append(buf, []byte("[")...)
		if n, err, done = d.flush(p, buf); done {
			return n, err
		}
		buf = append(buf, []byte(buildContext(d.backend.DecodeConfig.Namespaces))...)
		if n, err, done = d.flush(p, buf); done {
			return n, err
		}
	}

	// append one entity per line, comma separated
	count := 0
	for {
		row, err := d.pqReader.NextRow()
		var entityProps = row

		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return n, err

		}

		var entityBytes []byte
		entityProps, err = d.ParseLine(entityProps)
		entityBytes, err = toEntityBytes(entityProps, d.backend)
		if err != nil {
			return n, err
		}

		buf = append(buf, append([]byte(","), entityBytes...)...)
		if n, err, done = d.flush(p, buf); done {
			return n, err
		}

		count++
	}
	var token string
	if d.fullSync {
		token = ""
	} else {
		token = d.since
	}

	// close json array
	if !d.closed {
		// Add continuation token
		continueEntity := map[string]interface{}{
			"id":    "@continuation",
			"token": token,
		}
		sinceBytes, _ := json.Marshal(continueEntity)
		buf = append(buf, append([]byte(","), sinceBytes...)...)
		buf = append(buf, []byte("]")...)
		d.closed = true
		if n, err, done = d.flush(p, buf); done {
			return
		}
	}
	n = copy(p, buf)
	return n, io.EOF
}

func (d *ParquetDecoder) flush(p []byte, buf []byte) (int, error, bool) {
	if len(buf) >= len(p) {
		n := copy(p, buf)
		d.overhang = buf[n:]
		return n, nil, true
	}
	return 0, nil, false
}

func (d *ParquetDecoder) Close() error {
	return d.reader.Close()
}

func (d *ParquetDecoder) ParseLine(line map[string]interface{}) (map[string]interface{}, error) {
	var entityProps = make(map[string]interface{}, 0)
	for key, field := range line {
		for _, v := range d.pqReader.GetSchemaDefinition().RootColumn.Children {
			if key == v.SchemaElement.Name {
				if v.SchemaElement.LogicalType != nil {
					value := fmt.Sprintf("%s", field)
					entityProps[key] = value
				} else {
					value := field
					entityProps[key] = value
				}
			}
		}
	}
	return entityProps, nil
}
