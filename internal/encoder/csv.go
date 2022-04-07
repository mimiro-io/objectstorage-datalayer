package encoder

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"io"
	"strconv"
)

// CsvFileEncoder ********************** ENCODER ****************************************/
type CsvEncoder struct {
	backend conf.StorageBackend
	writer  *io.PipeWriter
	logger  *zap.SugaredLogger
	open    bool
}

func (enc *CsvEncoder) Close() error {
	return enc.writer.Close()
}

func (enc *CsvEncoder) Write(entities []*entity.Entity) (int, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	if !enc.open {
		err := enc.Open()
		if err != nil {
			return 0, err
		}
	}

	written, err := enc.encode(entities)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func (enc *CsvEncoder) Open() error {
	enc.open = true
	config := enc.backend.CsvConfig // we come here, this should never be nil

	if config.Header {
		writer := csv.NewWriter(enc.writer)
		if enc.backend.CsvConfig.Separator != "" {
			writer.Comma = rune(enc.backend.CsvConfig.Separator[0])
		}

		var headerLine []string
		headerLine = append(headerLine, config.Order...)

		err := writer.Write(headerLine)
		if err != nil {
			return err
		}

		writer.Flush()
	}

	return nil
}

func (enc *CsvEncoder) CloseWithError(err error) error {
	return enc.writer.CloseWithError(err)
}

func (enc *CsvEncoder) encode(entities []*entity.Entity) (int, error) {
	writer := csv.NewWriter(enc.writer)
	config := enc.backend.CsvConfig // we come here, this should never be nil

	if enc.backend.CsvConfig.Separator != "" {
		writer.Comma = rune(enc.backend.CsvConfig.Separator[0])
	}

	written := 0
	for _, ent := range entities {
		var r []string

		row := propStripper(ent)

		for _, h := range config.Order {
			if _, ok := row[h]; ok {
				switch v := row[h].(type) {
				case float64:
					r = append(r, strconv.FormatFloat(v, 'f', 0, 64))
				case string:
					r = append(r, v)
				case bool:
					r = append(r, strconv.FormatBool(v))
				}
			} else {
				r = append(r, "")
			}
		}
		for _, col := range r {
			written += len([]byte(col))
		}
		err := writer.Write(r)
		if err != nil {
			return 0, err
		}
	}

	writer.Flush()

	return written, nil
}

// CsvFileDecoder ********************** DECODER ****************************************/

type CsvDecoder struct {
	backend  conf.StorageBackend
	reader   *io.PipeReader
	logger   *zap.SugaredLogger
	open     bool
	closed   bool
	since    string
	fullSync bool
}

func (dec *CsvDecoder) Close() error {
	return dec.Close()
}

func (dec *CsvDecoder) Read(p []byte) (n int, err error) {
	buf := make([]byte, 0, len(p))

	if !dec.open {
		dec.open = true
		buf = append(buf, []byte("[")...)
		buf = append(buf, []byte(buildContext(dec.backend.DecodeConfig.Namespaces))...)
		if err != nil {
			return
		}
	}
	config := dec.backend.CsvConfig // we come here, this should never be nil
	reader := csv.NewReader(dec.reader)
	if config.Separator != "" {
		reader.Comma = rune(config.Separator[0])
	}
	reader.FieldsPerRecord = -1

	skip := 0
	for skip < dec.backend.CsvConfig.SkipRows {
		var skipRec []string
		if _, err := reader.Read(); err != nil {
			panic(err)
		}
		fmt.Sprintf("skipped rec %s", skipRec)
		skip++
	}
	headerLine, err := reader.Read()
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	// append one entity per line, comma separated
	buf = append(buf, []byte("[")...)
	buf = append(buf, []byte(buildContext(dec.backend.DecodeConfig.Namespaces))...)
	for _, record := range records {
		var entityProps = make(map[string]interface{})
		entityProps, err := dec.parseRecord(record, headerLine)
		if err != nil {
			panic(err)
		}
		var entityBytes []byte
		entityBytes, err = toEntityBytes(entityProps, dec.backend)
		if err != nil {
			panic(err)
		}
		buf = append(buf, append([]byte(","), entityBytes...)...)
	}
	var token string
	if dec.fullSync {
		token = ""
	} else {
		token = dec.since
	}
	// Add continuation token
	entity := map[string]interface{}{
		"id":    "@continuation",
		"token": token,
	}
	sinceBytes, err := json.Marshal(entity)
	buf = append(buf, append([]byte(","), sinceBytes...)...)

	// close json array
	if !dec.closed {
		buf = append(buf, []byte("]")...)
		dec.closed = true
	}
	n = copy(p, buf)
	return n, io.EOF
}
func (dec *CsvDecoder) parseRecord(data []string, header []string) (map[string]interface{}, error) {
	// convert csv lines to array of structs
	var entityProps = make(map[string]interface{}, 0)
	for j, key := range header {
		entityProps[key] = data[j]

	}
	return entityProps, nil
}
