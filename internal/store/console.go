package store

import (
	"errors"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"io"
)

type ConsoleStorage struct {
	Logger *zap.SugaredLogger
	env    *conf.Env
	config conf.StorageBackend
}

func (consoleStorage *ConsoleStorage) GetEntities() (io.Reader, error) {
	return nil, errors.New("GetEntities not supported for ConsoleStorage")
}

func (consoleStorage *ConsoleStorage) GetChanges(since string) (io.Reader, error) {
	return nil, errors.New("GetChanges not supported for ConsoleStorage")
}

func (consoleStorage *ConsoleStorage) GetConfig() conf.StorageBackend {
	return consoleStorage.config
}

func (consoleStorage *ConsoleStorage) StoreEntities(entities []*entity.Entity, entityContext *uda.Context) error {
	consoleStorage.Logger.Info("Console stores")
	consoleStorage.Logger.Infof("Got: %d entities", len(entities))
	return nil
}

func (consoleStorage *ConsoleStorage) StoreEntitiesFullSync(state FullSyncState, entities []*entity.Entity, entityContext *uda.Context) error {
	return errors.New("fullsync not supported to console")
}
