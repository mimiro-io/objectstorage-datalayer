package encoder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"io"
)

// NDJsonEncoder ********************** ENCODER ****************************************/
type NDJsonEncoder struct {
	backend conf.StorageBackend
	writer  *io.PipeWriter
	logger  *zap.SugaredLogger
}

func (enc *NDJsonEncoder) Write(entities []*entity.Entity) (int, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	data, err := enc.encode(entities)
	if err != nil {
		return 0, err
	}

	return enc.writer.Write(data)
}

func (enc *NDJsonEncoder) Close() error {
	return enc.writer.Close()
}

func (enc *NDJsonEncoder) CloseWithError(err error) error {
	return enc.writer.CloseWithError(err)
}

func (enc *NDJsonEncoder) encode(entities []*entity.Entity) ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)

	for _, e := range entities {
		if enc.backend.StripProps {
			_ = encoder.Encode(propStripper(e))
		} else {
			_ = encoder.Encode(e)
		}
	}

	return buf.Bytes(), nil
}

// NDJsonDecoder ********************** DECODER ****************************************/
type NDJsonDecoder struct {
	backend  conf.StorageBackend
	logger   *zap.SugaredLogger
	reader   *io.PipeReader
	scanner  *bufio.Scanner
	open     bool
	closed   bool
	overhang []byte
}

func (d *NDJsonDecoder) Read(p []byte) (n int, err error) {
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
		var entityBytes []byte
		entityBytes, err = toEntityBytes(d.scanner.Bytes(), d.backend)
		if err != nil {
			return
		}
		buf = append(buf, append([]byte(","), entityBytes...)...)
		if n, err, done = d.flush(p, buf); done {
			return
		}
	}

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

func (d *NDJsonDecoder) flush(p []byte, buf []byte) (int, error, bool) {
	if len(buf) >= len(p) {
		n := copy(p, buf)
		d.overhang = buf[n:]
		return n, nil, true
	}
	return 0, nil, false
}

func (d *NDJsonDecoder) Close() error {
	return d.reader.Close()
}
