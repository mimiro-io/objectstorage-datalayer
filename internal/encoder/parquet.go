package encoder

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
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

func (enc *ParquetEncoder) Write(entities []*entity.Entity) (int, error) {
	if !enc.open {
		err := enc.Open()
		if err != nil {
			return 0, err
		}
	}

	for _, e := range entities {
		props := propStripper(e)
		row := make(map[string]interface{})
		for _, c := range enc.schemaDef.RootColumn.Children {
			val, ok := props[c.SchemaElement.Name]
			if ok && val != nil {
				i, err := convertType(val, c.SchemaElement.Type, c.SchemaElement.LogicalType)
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
		enc.logger.Debugf("Flushed %v parquet bytes to underlying writer. flushed in total: %v",
			flushed, enc.pqWriter.CurrentFileSize())
		return int(flushed), nil
	}
	return 0, nil
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
			r, ok := val.(string)
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
	_ = enc.pqWriter.Close()
	return enc.writer.CloseWithError(err)
}

func (enc *ParquetEncoder) Open() error {
	schemaDef, err := parquetschema.ParseSchemaDefinition(enc.backend.ParquetConfig.SchemaDefinition)
	if err != nil {
		return err
	}

	enc.flushThreshold = 1 * 1024 * 1024 //1MB conservative default. For optimal parquet files 512-1024MB is recommended but this will blow memory for us
	if enc.backend.ParquetConfig.FlushThreshold > 0 {
		enc.flushThreshold = enc.backend.ParquetConfig.FlushThreshold
	}
	enc.logger.Infof("writing parquet files with flushThreshold %v", enc.flushThreshold)
	enc.schemaDef = schemaDef

	enc.pqWriter = goparquet.NewFileWriter(enc.writer,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("objectstorage-datalayer"),
	)

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
		d.scanner = bufio.NewScanner(d.reader)
		//start json array and add context as first entity
		buf = append(buf, []byte("[")...)
		if n, err, done = d.flush(p, buf); done {
			return
		}
		buf = append(buf, []byte(buildContext(d.backend.DecodeConfig.Namespaces))...)
		if n, err, done = d.flush(p, buf); done {
			return
		}
	}
	// append one entity per line, comma separated
	for d.scanner.Scan() {

		line := d.scanner.Text()
		//d.logger.Debugf("Got line : '%s'", line)
		var entityProps map[string]interface{}
		if err != nil {
			d.logger.Errorf("Failed to parse line: '%s'", line)
			if d.backend.ParquetConfig.ContinueOnParseError {
				continue
			} else {
				return
			}
		}

		var entityBytes []byte
		entityBytes, err = toEntityBytes(entityProps, d.backend)
		if err != nil {
			return
		}
		buf = append(buf, append([]byte(","), entityBytes...)...)
		if n, err, done = d.flush(p, buf); done {
			return
		}
	}
	var token string
	if d.fullSync {
		token = ""
	} else {
		token = d.since
	}
	// Add continuation token
	entity := map[string]interface{}{
		"id":    "@continuation",
		"token": token,
	}
	sinceBytes, err := json.Marshal(entity)
	buf = append(buf, append([]byte(","), sinceBytes...)...)

	// close json array
	if !d.closed {
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

func (d *ParquetDecoder) convertType(value string, fieldConfig conf.FlatFileField) (interface{}, error) {
	switch fieldConfig.Type {
	case "integer":
		return strconv.Atoi(value)
	case "float":
		index := fieldConfig.Decimals
		if index == 0 {
			return value, errors.New("no decimals defined for type float in flat file field config")
		}
		withComma := value[:len(value)-index] + "." + value[len(value)-index:]
		asFloat, err := strconv.ParseFloat(withComma, 64)
		if err != nil {
			return value, err
		}
		return asFloat, nil
	case "date":
		layout := fieldConfig.DateLayout
		if layout == "" {
			return value, errors.New("no date layout defined for type date in flat file field config")
		}
		timestamp, err := time.Parse(layout, value)
		if err != nil {
			return value, err
		}
		return timestamp.Format(time.RFC3339), nil
	default:
		return value, nil
	}

}
