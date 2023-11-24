package encoder

import (
	"encoding/csv"
	"encoding/json"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
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

func (enc *CsvEncoder) Write(entities []*uda.Entity) (int, error) {
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

func (enc *CsvEncoder) encode(entities []*uda.Entity) (int, error) {
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
	backend    conf.StorageBackend
	reader     *io.PipeReader
	logger     *zap.SugaredLogger
	csvreader  *csv.Reader
	headerline []string
	open       bool
	closed     bool
	since      string
	overhang   []byte
	fullSync   bool
}

func (dec *CsvDecoder) Read(p []byte) (n int, err error) {
	buf := make([]byte, 0, len(p))
	var done bool
	if len(dec.overhang) > 0 {
		buf = append(buf, dec.overhang...)
		dec.overhang = nil
	}
	if !dec.open {
		dec.open = true
		dec.csvreader = csv.NewReader(dec.reader)
		buf = append(buf, []byte("[")...)
		if n, err, done = dec.flush(p, buf); done {
			return
		}
		buf = append(buf, []byte(buildContext(dec.backend.DecodeConfig.Namespaces))...)
		config := dec.backend.CsvConfig // we come here, this should never be nil
		//csvreader := csv.NewReader(dec.reader)
		if config.Separator != "" {
			dec.csvreader.Comma = rune(config.Separator[0])
		}
		dec.csvreader.FieldsPerRecord = -1
		dec.skipRows()
		if n, err, done = dec.flush(p, buf); done {
			return
		}
	}

	var headerLine []string
	var record []string
	// create headerline from config or from read.
	if dec.headerline == nil {
		if dec.backend.CsvConfig.Header {
			headerLine, err = dec.csvreader.Read()
			if err == nil {
				dec.headerline = headerLine
			}
		} else if !dec.backend.CsvConfig.Header {
			if dec.backend.CsvConfig.Order != nil {
				dec.headerline = dec.backend.CsvConfig.Order
			}
		} else {
			dec.logger.Errorf("No strategy chosen for headers chosen, please change config")
		}
	}

	//streaming doesnt work with ReadAll()
	// append one entity per line, comma separated
	for {
		record, err = dec.csvreader.Read()

		if err != nil || len(record) == 0 {
			break
		}
		if len(record) < len(dec.headerline) {
			dec.skipRows()
			continue
		}

		var entityProps = make(map[string]interface{})
		entityProps, err = dec.parseRecord(record, dec.headerline)
		if err != nil {
			return
		}
		var entityBytes []byte
		entityBytes, err = toEntityBytes(entityProps, dec.backend)
		if err != nil {
			return
		}
		if entityBytes == nil {
			continue
		}
		buf = append(buf, append([]byte(","), entityBytes...)...)

		if n, err, done = dec.flush(p, buf); done {
			return
		}

	}

	// close json array
	var sinceBytes []byte
	if !dec.closed {
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
		sinceBytes, err = json.Marshal(entity)
		if err != nil {
			panic(err)
		}
		buf = append(buf, append([]byte(","), sinceBytes...)...)
		buf = append(buf, []byte("]")...)
		dec.closed = true
		if n, err, done = dec.flush(p, buf); done {
			return
		}
	}
	n = copy(p, buf)

	return n, io.EOF
}
func (dec *CsvDecoder) skipRows() {
	skip := 0
	for skip < dec.backend.CsvConfig.SkipRows {
		_, err := dec.csvreader.Read()
		if err != nil {
			panic(err)
		}
		skip++
	}
}

func (dec *CsvDecoder) flush(p []byte, buf []byte) (int, error, bool) {
	// p grows unexpectedly so 512 is set as hard byte cap.
	if len(buf) >= 512 {
		n := copy(p, buf)
		dec.overhang = buf[n:]
		return n, nil, true
	}
	return 0, nil, false
}

func (dec *CsvDecoder) Close() error {
	return dec.Close()
}

func (dec *CsvDecoder) parseRecord(data []string, header []string) (map[string]interface{}, error) {
	// convert csv lines to array of structs
	var entityProps = make(map[string]interface{}, 0)
	for j, key := range header {
		entityProps[key] = data[j]

	}
	return entityProps, nil
}
