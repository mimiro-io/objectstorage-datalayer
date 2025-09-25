package store

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
)

type FullSyncState struct {
	Id    string
	Start bool
	End   bool
}

type StorageInterface interface {
	GetConfig() conf.StorageBackend
	StoreEntities(entities []*uda.Entity) error
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
	if config.OrderType != "desc" || config.OrderType != "asc" {
		logger.Info("orderType in config not of supported format. Defaulting to ascending order")
	}
	if config.OrderBy == "" {
		logger.Error("No orderby specified in config")
	}
	// Parse the orderby string
	orderBy, err := extractIndexSlice(config.OrderBy)
	if err != nil {
		logger.Error("Unable to parse orderby string")
		return entities, err
	}
	data := strings.Split(string(entities[:len(entities)-1]), "\n")

	sort.Slice(data, func(i, j int) bool {
		var partsI, partsJ int
		for _, x := range orderBy {
			partsI, _ = extractParts(data[i], x)
			partsJ, _ = extractParts(data[j], x)

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

	// Reconstruct the entities in sorted order
	var sortedData []byte
	for _, s := range data {
		sortedData = append(sortedData, []byte(s+"\n")...)
	}
	return sortedData, nil
}
func extractParts(s string, i []int) (int, error) {
	numPart, err := strconv.Atoi(s[i[0]:i[1]])
	if err != nil {
		return 0, err
	}
	return numPart, nil
}
func extractIndexSlice(s string) ([][]int, error) {
	parts := strings.Split(s, ":")
	var orderByInt = make([][]int, len(parts))
	for index, o := range parts {
		order := strings.Split(o, ",")
		i, err := strconv.Atoi(order[0])
		if err != nil {
			return nil, err
		}
		j, err := strconv.Atoi(order[1])
		if err != nil {
			return nil, err
		}
		orderByInt[index] = []int{i, j}
	}
	return orderByInt, nil
}
