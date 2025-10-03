package store

import (
	"strings"
	"testing"

	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"go.uber.org/zap"
)

func TestConsoleStorage_StoreEntities(t *testing.T) {
	entities := []*uda.Entity{
		uda.NewEntity(),
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

	err := consoleStorage.StoreEntities(entities)
	if err != nil {
		t.Error(err)
	}

}
func TestOrderContent(t *testing.T) {
	data := []byte("10000000100002\n10000000200001\n10000000200002\n20000000100002\n20000000100001\n20000000200001\n20000000200002\n10000000100001\n")

	var testDataSorted = []byte("10000000100001\n10000000100002\n10000000200001\n10000000200002\n20000000100001\n20000000100002\n20000000200001\n20000000200002\n")
	var testDataSortedString string
	for _, d := range strings.Split(string(testDataSorted[:len(testDataSorted)-1]), "\n") {
		testDataSortedString += d + "\n"
	}
	orderBy := [][]int{
		{0, 8},
		{8, 12},
		{12, 14}}
	config := conf.StorageBackend{
		OrderBy: orderBy,
	}
	sortedData, err := OrderContent(data, config, zap.NewNop().Sugar())
	if err != nil {
		t.Error(err)
	}
	var dataSortedString string
	for _, d := range strings.Split(string(sortedData[:len(sortedData)-1]), "\n") {
		dataSortedString += d + "\n"
	}

	if testDataSortedString != dataSortedString {
		t.Error("Sorting is not correct")
	}
}

func TestOrderContentNoIntError(t *testing.T) {
	data := []byte("1000wer000100002\n10000000200001\n10000000200002\n20000000100002\n20000000100001\n20000000200001\n20000000200002\n10000000100001\n")
	orderBy := [][]int{
		{0, 8},
		{8, 12},
		{12, 14}}
	config := conf.StorageBackend{
		OrderBy: orderBy,
	}
	_, err := OrderContent(data, config, zap.NewNop().Sugar())
	if err == nil {
		t.Error("Expected error, got none")
	}
}
func TestDeliverOnceVariableCheckMissingVariable(t *testing.T) {
	var env string = "local"
	storage := S3Storage{
		logger: zap.NewNop().Sugar(),
		env:    &conf.Env{Env: env},
		config: conf.StorageBackend{
			DeliverOnceConfig: conf.DeliverOnceConfig{
				Enabled:          true,
				Dataset:          "",
				IdNamespace:      "http://data.mimiro.io/e360/milk_control_labels/",
				DefaultNamespace: "http://data.mimiro.io/s3/",
				BaseUrl:          "http://localhost:8080"},
		},
	}
	err := storage.DeliverOnceVariableCheck()
	if err == nil {
		t.Error(err)
	}
}
func TestDeliverOnceVariableCheckAllVariables(t *testing.T) {
	var env string = "local"
	storage := S3Storage{
		logger: zap.NewNop().Sugar(),
		env:    &conf.Env{Env: env},
		config: conf.StorageBackend{
			DeliverOnceConfig: conf.DeliverOnceConfig{
				Enabled:          true,
				Dataset:          "foo",
				IdNamespace:      "http://data.mimiro.io/e360/milk_control_labels/",
				DefaultNamespace: "http://data.mimiro.io/s3/",
				BaseUrl:          "http://localhost:8080"},
		},
	}
	err := storage.DeliverOnceVariableCheck()
	if err != nil {
		t.Error(err)
	}
}
