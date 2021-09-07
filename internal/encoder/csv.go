package encoder

import (
	"encoding/csv"
	"go.uber.org/zap"
	"io"
	"strconv"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
)

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
