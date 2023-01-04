package encoder

import (
	"encoding/json"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"io"
)

type JSONEncoder struct {
	backend            conf.StorageBackend
	writer             *io.PipeWriter
	logger             *zap.SugaredLogger
	open               bool
	firstEntityWritten bool
}

func (enc *JSONEncoder) Close() error {
	_, err := enc.writer.Write([]byte("]"))
	if err != nil {
		return err
	}

	return enc.writer.Close()
}

func (enc *JSONEncoder) Write(entities []*uda.Entity) (int, error) {
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

func (enc *JSONEncoder) CloseWithError(err error) error {
	return enc.writer.CloseWithError(err)
}

func (enc *JSONEncoder) encode(entities []*uda.Entity) (int, error) {
	written := 0
	for _, e := range entities {
		if enc.firstEntityWritten {
			w, err := enc.writer.Write([]byte(","))
			if err != nil {
				return 0, err
			}
			written += w
		} else {
			enc.firstEntityWritten = true
		}

		var err error
		var bytes []byte
		if enc.backend.StripProps {
			bytes, err = json.Marshal(propStripper(e))
		} else {
			bytes, err = json.Marshal(e)
		}
		var w int
		w, err = enc.writer.Write(bytes)
		if err != nil {
			return 0, err
		}

		written += w
	}

	return written, nil
}

func (enc *JSONEncoder) Open() error {
	enc.open = true

	_, err := enc.writer.Write([]byte("["))
	if err != nil {
		return err
	}

	return nil
}
