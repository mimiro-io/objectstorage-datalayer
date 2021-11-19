package encoder

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"io"
	"strconv"
	"time"
)

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
		entityProps, err := d.ParseLine(line, d.backend.FlatFileConfig)
		if err != nil {
			return 0, err
		}

		var entityBytes []byte
		entityBytes, err = toEntityBytes(entityProps, d.backend)
		if err != nil {
			return 0, err
		}
		buf = append(buf, append([]byte(","), entityBytes...)...)
		if n, err, done = d.flush(p, buf); done {
			return 0, err
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
