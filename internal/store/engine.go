package store

import (
	"errors"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
	"strings"
)

type StorageEngine struct {
	statsd   statsd.ClientInterface
	logger   *zap.SugaredLogger
	storages map[string]storageState
	mngr     *conf.ConfigurationManager
	env      *conf.Env
}

type storageState struct {
	isRunning bool
	storage   StorageInterface
}

func NewStorageEngine(logger *zap.SugaredLogger, config *conf.ConfigurationManager, env *conf.Env, statsd statsd.ClientInterface) *StorageEngine {
	return &StorageEngine{
		statsd:   statsd,
		mngr:     config,
		logger:   logger.Named("storage"),
		env:      env,
		storages: make(map[string]storageState),
	}
}

// Storage returns a configured storage from the configured storages, or it returns an error
// if not found
func (engine *StorageEngine) Storage(datasetName string) (StorageInterface, error) {
	if _, ok := engine.mngr.Datalayer.StorageMapping[datasetName]; !ok {
		return nil, errors.New("dataset not found")
	}

	var state storageState
	if s, ok := engine.storages[datasetName]; ok {
		s.isRunning = true
		state = s
	} else {
		storage, err := engine.initBackend(engine.mngr.Datalayer.StorageMapping[datasetName])
		if err != nil {
			return nil, err
		}
		s := storageState{
			isRunning: true,
			storage:   storage,
		}
		engine.storages[datasetName] = s
		state = s
	}

	return state.storage, nil
}

// Close handles cleanup of storage engines, if needed
func (engine *StorageEngine) Close(datasetName string) {
	if s, ok := engine.storages[datasetName]; ok {
		s.isRunning = false
	}
}

func (engine *StorageEngine) initBackend(backend conf.StorageBackend) (StorageInterface, error) {
	switch strings.ToLower(backend.StorageType) {
	case "azure":
		return NewAzureStorage(engine.logger, engine.env, backend, engine.statsd, backend.Dataset), nil
	case "s3":
		return NewS3Storage(engine.logger, engine.env, backend, engine.statsd, backend.Dataset)
	default:
		return &ConsoleStorage{
			Logger: engine.logger.Named("console-store"),
		}, nil
	}
}