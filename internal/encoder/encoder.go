package encoder

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"io"
	"strings"
)

type EncodingEntityWriter interface {
	io.Closer
	Write(entities []*entity.Entity, entityContext *uda.Context) (int, error)
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

func propStripper(entity *entity.Entity) map[string]interface{} {
	var singleMap = make(map[string]interface{})
	for k := range entity.Properties {
		singleMap[strings.SplitAfter(k, ":")[1]] = entity.Properties[k]
	}

	return singleMap
}

func keyStripper(entity *entity.Entity, keyType string) map[string]interface{} {
	var singleMap = make(map[string]interface{})
	var keys map[string]interface{}
	switch keyType {
	case "props":
		keys = entity.Properties
	case "refs":
		keys = entity.References
	}
	for k := range keys {
		key := k
		parts := strings.SplitAfter(k, ":")
		if len(parts) > 1 {
			key = parts[1]
		}
		singleMap[key] = keys[k]
	}

	return singleMap
}
