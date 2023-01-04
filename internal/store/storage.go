package store

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
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
