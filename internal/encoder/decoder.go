package encoder

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EncodingEntityReader interface {
	io.Closer
	io.Reader
}

func NewEntityDecoder(backend conf.StorageBackend, reader *io.PipeReader, since string, logger *zap.SugaredLogger, fullSync bool) (EncodingEntityReader, error) {
	if backend.AthenaCompatible {
		return &NDJsonDecoder{backend: backend, reader: reader, logger: logger}, nil
	}

	if backend.FlatFileConfig != nil {
		return &FlatFileDecoder{backend: backend, reader: reader, logger: logger, since: since, fullSync: fullSync}, nil
	}
	if backend.CsvConfig != nil {
		return &CsvDecoder{backend: backend, reader: reader, logger: logger, since: since, fullSync: fullSync}, nil
	}
	if backend.ParquetConfig != nil {
		return &ParquetDecoder{backend: backend, reader: reader, logger: logger, since: since, fullSync: fullSync}, nil
	}
	return nil, errors.New("this dataset has no decoder")
}

func toEntityBytes(line map[string]interface{}, backend conf.StorageBackend) ([]byte, error) {

	id, err := extractID(backend, line)
	if err != nil || id == "" {
		return nil, err
	}

	result := make(map[string]interface{})
	result["id"] = id
	newProps := map[string]interface{}{}
	newRefs := map[string]interface{}{}

	// add defaults if defined - this overwrites any existing values
	if backend.DecodeConfig != nil && backend.DecodeConfig.Defaults != nil {
		for k, v := range backend.DecodeConfig.Defaults {
			line[k] = v
		}
	}

	// iterate the concat columns and concat them into new fields
	if backend.DecodeConfig != nil && backend.DecodeConfig.ConcatColumns != nil {
		for k, v := range backend.DecodeConfig.ConcatColumns {
			var sb strings.Builder
			first := true
			for _, col := range v {
				if val, ok := line[col]; ok {
					if !first {
						sb.WriteString(",")
					}
					first = false
					sb.WriteString(val.(string))
				}
			}
			line[k] = sb.String()
		}
	}

	ignoreColums := backend.DecodeConfig.IgnoreColumns
	for k, v := range line {
		if slices.Contains(ignoreColums, k) {
			continue
		}
		if isRef(backend, k) {
			_, err = withPrefix(newRefs, backend, k, v)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = withPrefix(newProps, backend, k, v)
			if err != nil {
				return nil, err
			}
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

func withPrefix(m map[string]interface{}, backend conf.StorageBackend, k string, v interface{}) (string, error) {

	if backend.DecodeConfig != nil {
		if backend.DecodeConfig.ColumnMappings != nil {
			if mapped, ok := backend.DecodeConfig.ColumnMappings[k]; ok {
				k = mapped
			}
		}

		// if the value is multivalue process that first
		isMultiValue := false
		if backend.DecodeConfig.ListValueColumns != nil {
			if mapped, ok := backend.DecodeConfig.ListValueColumns[k]; ok {
				sv := v.(string)
				if sv != "" {
					tv := strings.Split(sv, mapped)
					vs := make([]string, 0)
					for _, s := range tv {
						vs = append(vs, strings.TrimSpace(s))
					}
					v = vs
				}
				isMultiValue = true
			}
		}

		// convert the value to the correct type - if list then apply to all values
		if backend.DecodeConfig.ColumnTypes != nil {
			if mapped, ok := backend.DecodeConfig.ColumnTypes[k]; ok {
				if isMultiValue {
					sv := v.([]string)
					if mapped == "int" {
						iv := make([]int, 0)
						for _, s := range sv {
							vv, _ := strconv.Atoi(s)
							iv = append(iv, vv)
						}
						v = iv
					} else if mapped == "float" {
						iv := make([]float64, 0)
						for _, s := range sv {
							vv, _ := strconv.ParseFloat(s, 64)
							iv = append(iv, vv)
						}
						v = iv
					} else if mapped == "bool" {
						iv := make([]bool, 0)
						for _, s := range sv {
							vv, _ := strconv.ParseBool(s)
							iv = append(iv, vv)
						}
						v = iv
					} else {
						return "", errors.New(fmt.Sprintf("Unsupported type %v for column %v", mapped, k))
					}
				} else {
					sv := v.(string)
					switch mapped {
					case "int":
						v, _ = strconv.Atoi(sv)
					case "float":
						v, _ = strconv.ParseFloat(sv, 64)
					case "bool":
						v, _ = strconv.ParseBool(sv)
					default:
						return "", errors.New(fmt.Sprintf("Unsupported type %v for column %v", mapped, k))
					}
				}
			}
		}

		prefixConfig, exist := backend.DecodeConfig.PropertyPrefixes[k]
		if exist {
			keyPrefix, valuePrefix := prefixValues(prefixConfig)
			m[fmt.Sprintf("%v", wrap(k, keyPrefix))] = wrap(v, valuePrefix)
		} else {
			m[fmt.Sprintf("%v", wrap(k, backend.DecodeConfig.DefaultNamespace))] = v
		}
	}

	return k, nil
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
		if value == nil {
			return "", fmt.Errorf("could not extract id from entity")
		}
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
