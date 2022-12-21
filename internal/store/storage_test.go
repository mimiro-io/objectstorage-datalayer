package store

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"go.uber.org/zap"
	"testing"
)

func TestConsoleStorage_StoreEntities(t *testing.T) {
	entities := []*entity.Entity{
		entity.NewEntity(),
	}

	entityContext := uda.Context{
		ID:         "@context",
		Namespaces: map[string]string{},
	}

	env := &conf.Env{
		Logger:          nil,
		Env:             "",
		Port:            "",
		ConfigLocation:  "",
		RefreshInterval: "",
		ServiceName:     "",
	}

	//datalayer := conf.Datalayer{
	//	StorageMapping: storeM,
	//	Datalayers:     nil,
	//}

	//configurationManager := conf.ConfigurationManager{
	//	Datalayer:      &datalayer,
	//	state:          conf.state{},
	//	TokenProviders: nil,
	//}

	consoleStorage := ConsoleStorage{
		Logger: zap.NewNop().Sugar(),
		env:    env,
		config: conf.StorageBackend{},
	}

	err := consoleStorage.StoreEntities(entities, &entityContext)
	if err != nil {
		t.Error(err)
	}

}
