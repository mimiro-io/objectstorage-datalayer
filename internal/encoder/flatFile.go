package encoder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"io"
	"strconv"
	"strings"
	"time"
)

// FlatFileEncoder ********************** ENCODER ****************************************/
type FlatFileEncoder struct {
	backend conf.StorageBackend
	writer  *io.PipeWriter
	logger  *zap.SugaredLogger
}

func (enc *FlatFileEncoder) Write(entities []*entity.Entity) (int, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	data, err := enc.encode(entities)
	if err != nil {
		return 0, err
	}

	return enc.writer.Write(data)
}

func (enc *FlatFileEncoder) Close() error {
	return enc.writer.Close()
}

func (enc *FlatFileEncoder) CloseWithError(err error) error {
	return enc.writer.CloseWithError(err)
}

func (enc *FlatFileEncoder) encode(entities []*entity.Entity) ([]byte, error) {
	buf := new(bytes.Buffer)
	fields := enc.backend.FlatFileConfig.Fields
	if fields == nil {
		return nil, fmt.Errorf("missing field config for flat file")
	}
	fieldOrder := enc.backend.FlatFileConfig.FieldOrder
	if fieldOrder == nil {
		return nil, fmt.Errorf("missing fieldOrder config for flat file write operation")
	}
	for _, e := range entities {
		var line []string
		fieldsWithData := 0
		for _, fieldName := range fieldOrder {
			fieldConfig, exist := fields[fieldName]
			if exist == false {
				return nil, fmt.Errorf("missing fieldConfig for required field in fieldOrder")
			}
			var preppedValue string
			var fieldValue interface{}
			for key, val := range e.Properties {
				if stripNamespace(key) == fieldName {
					fieldValue = val
					break
				}
			}
			for key, val := range e.References {
				if stripNamespace(key) == fieldName {
					fieldValue = stripNamespace(val.(string))
					break
				}
			}
			fieldSize := 0
			for _, sub := range fieldConfig.Substring {
				fieldSize += sub[1] - sub[0]
			}
			if fieldValue == nil {
				//	Need to add spaces according to field substring config
				preppedValue = appendSpaces(preppedValue, fieldSize)
			} else {
				//	cast to string, then cut or append spaces to value according to substring config
				var value string
				switch fieldConfig.Type {
				case "date":
					dt, _ := time.Parse(time.RFC3339, fieldValue.(string))
					value = dt.Format(fieldConfig.DateLayout)
				case "float":
					f := strconv.FormatFloat(fieldValue.(float64), 'f', fieldConfig.Decimals, 64)
					value = strings.Replace(f, ".", "", -1)
				case "integer":
					value = fmt.Sprintf("%d", int(fieldValue.(float64)))
				default:
					value = fmt.Sprintf("%s", fieldValue)
				}
				valueLength := len(value)
				if valueLength < fieldSize {
					diff := fieldSize - valueLength
					if fieldConfig.Type == "integer" {
						preppedValue = prependZeros(value, diff)
					} else {
						preppedValue = appendSpaces(value, diff)
					}
				} else if valueLength > fieldSize {
					preppedValue = value[:fieldSize]
				} else {
					preppedValue = value
				}
				fieldsWithData += 1

			}
			line = append(line, preppedValue)
		}
		if fieldsWithData != 0 {
			buf.WriteString(fmt.Sprintf("%s\n", strings.Join(line, "")))
		}
	}

	return buf.Bytes(), nil
}

func appendSpaces(value string, amount int) string {
	for i := 0; i < amount; i++ {
		value += " "
	}
	return value
}

func prependZeros(value string, amount int) string {
	prefix := ""
	for i := 0; i < amount; i++ {
		prefix += "0"
	}
	return prefix + value
}

func stripNamespace(prop string) string {
	if strings.Contains(prop, ":") {
		parts := strings.Split(prop, ":")
		return parts[len(parts)-1]
	}
	return prop
}

// FlatFileDecoder ********************** DECODER ****************************************/
type FlatFileDecoder struct {
	backend  conf.StorageBackend
	logger   *zap.SugaredLogger
	reader   *io.PipeReader
	scanner  *bufio.Scanner
	open     bool
	closed   bool
	overhang []byte
	since    string
}

func (d *FlatFileDecoder) Read(p []byte) (n int, err error) {
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
		entityProps, err = d.ParseLine(line, d.backend.FlatFileConfig)
		if err != nil {
			return
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

	// Add continuation token
	entity := map[string]interface{}{
		"id":    "@continuation",
		"token": d.since,
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

func (d *FlatFileDecoder) flush(p []byte, buf []byte) (int, error, bool) {
	if len(buf) >= len(p) {
		n := copy(p, buf)
		d.overhang = buf[n:]
		return n, nil, true
	}
	return 0, nil, false
}

func (d *FlatFileDecoder) Close() error {
	return d.reader.Close()
}

func (d *FlatFileDecoder) convertType(value string, fieldConfig conf.FlatFileField) (interface{}, error) {
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

func (d *FlatFileDecoder) ParseLine(line string, config *conf.FlatFileConfig) (map[string]interface{}, error) {
	var entityProps = make(map[string]interface{}, 0)
	for key, field := range config.Fields {
		value := ""
		for _, sub := range field.Substring {
			value += line[sub[0]:sub[1]]
		}
		valueWithType, err := d.convertType(value, field)
		if err != nil {
			return nil, err
		}
		entityProps[key] = valueWithType

	}
	return entityProps, nil
}
