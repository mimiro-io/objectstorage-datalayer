package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"slices"
	"sort"
	"strconv"
	"strings"

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
func extractParts(s string, i []int) (int, error) {
	numPart, err := strconv.Atoi(s[i[0]:i[1]])
	if err != nil {
		return 0, err
	}
	return numPart, nil
}
