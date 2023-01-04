package encoder

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"io"
	"strings"
)

type EncodingEntityWriter interface {
	io.Closer
	Write(entities []*uda.Entity) (int, error)
	CloseWithError(err error) error
	//encode(entities []*entity.Entity) ([]byte, error)
}

func NewEntityEncoder(backend conf.StorageBackend, writer *io.PipeWriter, logger *zap.SugaredLogger) EncodingEntityWriter {
	if backend.ParquetConfig != nil {
		return &ParquetEncoder{backend: backend, writer: writer, logger: logger}
	}

	if backend.CsvConfig != nil {
		return &CsvEncoder{backend: backend, writer: writer, logger: logger}
	}

	if backend.AthenaCompatible {
		return &NDJsonEncoder{backend: backend, writer: writer, logger: logger}
	}

	if backend.FlatFileConfig != nil {
		return &FlatFileEncoder{backend: backend, writer: writer, logger: logger}
	}

	return &JSONEncoder{backend: backend, writer: writer, logger: logger}
}

func propStripper(entity *uda.Entity) map[string]interface{} {
	var singleMap = make(map[string]interface{})
	for k := range entity.Properties {
		singleMap[strings.SplitAfter(k, ":")[1]] = entity.Properties[k]
	}

	return singleMap
}
