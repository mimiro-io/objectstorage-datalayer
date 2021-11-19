package encoder

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"io"
	"strings"
)

type EncodingEntityReader interface {
	io.Closer
	io.Reader
}

func NewEntityDecoder(backend conf.StorageBackend, reader *io.PipeReader, since string, logger *zap.SugaredLogger) (EncodingEntityReader, error) {
	if backend.AthenaCompatible {
		return &NDJsonDecoder{backend: backend, reader: reader, logger: logger}, nil
	}

	if backend.FlatFileConfig != nil {
		return &FlatFileDecoder{backend: backend, reader: reader, logger: logger, since: since}, nil
	}

	return nil, errors.New("this dataset has no decoder")
}

func toEntityBytes(line map[string]interface{}, backend conf.StorageBackend) ([]byte, error) {

	id, err := extractID(backend, line)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	result["id"] = id
	newProps := map[string]interface{}{}
	newRefs := map[string]interface{}{}

	for k, v := range line {
		if isRef(backend, k) {
			withPrefix(newRefs, backend, k, v)
		} else {
			withPrefix(newProps, backend, k, v)
		}
	}

	result["props"] = newProps
	result["refs"] = newRefs
	result["deleted"] = false

	bytes, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func isRef(backend conf.StorageBackend, k string) bool {
	if backend.DecodeConfig != nil && len(backend.DecodeConfig.Refs) > 0 {
		for _, r := range backend.DecodeConfig.Refs {
			if r == k {
				return true
			}
		}
	}

	return false
}

func withPrefix(m map[string]interface{}, backend conf.StorageBackend, k string, v interface{}) string {
	if backend.DecodeConfig != nil {
		prefixConfig, exist := backend.DecodeConfig.PropertyPrefixes[k]
		if exist {
			keyPrefix, valuePrefix := prefixValues(prefixConfig)
			m[fmt.Sprintf("%v", wrap(k, keyPrefix))] = wrap(v, valuePrefix)
		} else {
			m[fmt.Sprintf("%v", wrap(k, backend.DecodeConfig.DefaultNamespace))] = v
		}
	}

	return k
}

func wrap(value interface{}, prefix string) interface{} {
	if prefix == "" {
		return value
	}
	return fmt.Sprintf("%v:%v", prefix, value)
}

func extractID(backend conf.StorageBackend, m map[string]interface{}) (string, error) {
	id := ""

	if len(backend.DecodeConfig.IdProperty) > 0 {
		value := m[backend.DecodeConfig.IdProperty]
		prefix := backend.DecodeConfig.PropertyPrefixes[backend.DecodeConfig.IdProperty]
		_, valuePrefix := prefixValues(prefix)
		if valuePrefix == "" {
			id = fmt.Sprintf("%v", value)
		} else {
			id = fmt.Sprintf("%v:%v", valuePrefix, value)
		}
	} else {
		return "", errors.New("decode.idProperty configuration required for stripped datasets")
	}

	return id, nil
}

func prefixValues(prefixConfig string) (string, string) {
	tokens := strings.Split(prefixConfig, ":")
	if len(tokens) > 1 {
		return tokens[0], tokens[1]
	}
	return tokens[0], ""
}

// build context entity
func buildContext(namespaces map[string]string) string {
	res := "{\"id\":\"@context\",\"namespaces\":{"
	firstNs := true

	for k, v := range namespaces {
		if !firstNs {
			res += ","
		}

		res = res + "\"" + k + "\":\"" + v + "\""
		firstNs = false
	}

	return res + "}}"
}
