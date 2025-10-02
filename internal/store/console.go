package store

import (
	"errors"
	"io"

	"github.com/mimiro-io/datahub-client-sdk-go"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
)

type ConsoleStorage struct {
	Logger *zap.SugaredLogger
	env    *conf.Env
	config conf.StorageBackend
}

func (consoleStorage *ConsoleStorage) DeliverOnceClientInit() (datahub.Client, error) {
	return datahub.Client{}, errors.New("DeliverOnceClientInit not supported for ConsoleStorage")
}

func (consoleStorage *ConsoleStorage) DeliverOnceVariableCheck() error {
	return errors.New("DeliverOnceVariableCheck not supported for ConsoleStorage")
}

func (consoleStorage *ConsoleStorage) DeliverOnce(entities []*uda.Entity, client datahub.Client) error {
	return errors.New("DeliverOnce not supported for ConsoleStorage")
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

func (consoleStorage *ConsoleStorage) StoreEntities(entities []*uda.Entity) error {
	consoleStorage.Logger.Info("Console stores")
	consoleStorage.Logger.Infof("Got: %d entities", len(entities))
	return nil
}

func (consoleStorage *ConsoleStorage) StoreEntitiesFullSync(state FullSyncState, entities []*uda.Entity) error {
	return errors.New("fullsync not supported to console")
}
