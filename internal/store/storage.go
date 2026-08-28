package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/mimiro-io/datahub-client-sdk-go"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"go.uber.org/zap"
)

type FullSyncState struct {
	Id    string
	Start bool
	End   bool
}

type StorageInterface interface {
	GetConfig() conf.StorageBackend
	DeliverOnceClientInit() (datahub.Client, error)
	DeliverOnceVariableCheck() error
	DeliverOnce(entities []*uda.Entity, client datahub.Client) error
	// StoreEntities writes entities to the backend and returns the subset of entities
	// that were actually stored (some backends may drop individual invalid rows rather
	// than failing the whole batch). Callers that trigger DeliverOnce should use the
	// returned entities, not the input, so dropped rows aren't reported as delivered.
	StoreEntities(entities []*uda.Entity) ([]*uda.Entity, error)
	StoreEntitiesFullSync(state FullSyncState, entities []*uda.Entity) error
	GetEntities() (io.Reader, error)
	GetChanges(since string) (io.Reader, error)
}

func GenerateContent(entities []*uda.Entity, config conf.StorageBackend, logger *zap.SugaredLogger) ([]byte, error) {
	reader, writer := io.Pipe()
	entEnc := encoder.NewEntityEncoder(config, writer, logger)
	go func() {
		_, err := entEnc.Write(entities)
		if err != nil {
			_ = entEnc.CloseWithError(err)
		}
		_ = entEnc.Close()
	}()
	return ioutil.ReadAll(reader)
}

func OrderContent(entities []byte, config conf.StorageBackend, logger *zap.SugaredLogger) ([]byte, error) {
	acceptedSortingTypes := []string{"desc", "asc"}
	if !slices.Contains(acceptedSortingTypes, config.OrderType) {
		logger.Info("No valid orderType defined. Defaulting to ascending order")
	}

	data := strings.Split(string(entities[:len(entities)-1]), "\n")
	errs := []error{}
	sort.Slice(data, func(i, j int) bool {
		var partsI, partsJ int
		for _, x := range config.OrderBy {
			partsI, err := extractParts(data[i], x)
			if err != nil {
				errs = append(errs, err)
				logger.Error(fmt.Sprintf("Unable to parse position %v in line %v as an integer", i, x))
			}
			partsJ, err = extractParts(data[j], x)
			if err != nil {
				errs = append(errs, err)
				logger.Error(fmt.Sprintf("Unable to parse position %v in line %v as an integer", i, x))
			}
			// Comparison of parts
			if partsI != partsJ {
				if config.OrderType == "desc" {
					return partsI > partsJ
				} else {
					return partsI < partsJ
				}
			}

		}
		if config.OrderType == "desc" {
			return partsI > partsJ
		} else {
			return partsI < partsJ
		}
	})
	if len(errs) > 0 {
		logger.Error("Unable to parse data")
		return nil, errs[0]
	}
	// Reconstruct the entities in sorted order
	var sortedData []byte
	for _, s := range data {
		sortedData = append(sortedData, []byte(s+"\n")...)
	}
	return sortedData, nil
}

func GenerateAndOrderFlatFileContent(entities []*uda.Entity, config conf.StorageBackend, logger *zap.SugaredLogger) ([]byte, []*uda.Entity, error) {
	type keyedLine struct {
		line   []byte
		keys   []int
		entity *uda.Entity
	}

	acceptedSortingTypes := []string{"desc", "asc"}
	if !slices.Contains(acceptedSortingTypes, config.OrderType) {
		logger.Info("No valid orderType defined. Defaulting to ascending order")
	}

	var kept []keyedLine
	for _, e := range entities {
		lineBytes, ok := safeEncodeFlatFileEntity(e, config, logger)
		if !ok {
			continue
		}
		if len(lineBytes) == 0 {
			// entity produced no output line (e.g. no fields matched); nothing to order
			continue
		}
		trimmed := strings.TrimSuffix(string(lineBytes), "\n")

		keys := make([]int, len(config.OrderBy))
		badRow := false
		for i, rng := range config.OrderBy {
			v, err := extractParts(trimmed, rng)
			if err != nil {
				logger.Errorw("Unable to parse orderBy value for entity. Dropping row.",
					"id", e.ID, "position", rng, "error", err)
				badRow = true
				break
			}
			keys[i] = v
		}
		if badRow {
			continue
		}
		kept = append(kept, keyedLine{line: []byte(trimmed), keys: keys, entity: e})
	}

	sort.Slice(kept, func(i, j int) bool {
		for idx := range kept[i].keys {
			a, b := kept[i].keys[idx], kept[j].keys[idx]
			if a != b {
				if config.OrderType == "desc" {
					return a > b
				}
				return a < b
			}
		}
		return false
	})

	var out []byte
	storedEntities := make([]*uda.Entity, 0, len(kept))
	for _, k := range kept {
		out = append(out, k.line...)
		out = append(out, '\n')
		storedEntities = append(storedEntities, k.entity)
	}
	return out, storedEntities, nil
}

func safeEncodeFlatFileEntity(e *uda.Entity, config conf.StorageBackend, logger *zap.SugaredLogger) (line []byte, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorw("Panic while encoding entity for flat file. Dropping row.", "id", e.ID, "error", r)
			line = nil
			ok = false
		}
	}()

	lineBytes, err := encoder.EncodeFlatFileEntities([]*uda.Entity{e}, config)
	if err != nil {
		logger.Errorw("Failed to encode entity for flat file. Dropping row.", "id", e.ID, "error", err)
		return nil, false
	}
	return lineBytes, true
}

func extractParts(s string, i []int) (int, error) {
	numPart, err := strconv.Atoi(s[i[0]:i[1]])
	if err != nil {
		return 0, err
	}
	return numPart, nil
}
