package store

import (
	"encoding/json"
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

	_, err := consoleStorage.StoreEntities(entities)
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
		{8, 10},
		{10, 14}}
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
		{8, 10},
		{10, 14}}
	config := conf.StorageBackend{
		OrderBy: orderBy,
	}
	_, err := OrderContent(data, config, zap.NewNop().Sugar())
	if err == nil {
		t.Error("Expected error, got none")
	}
}
func TestGenerateAndOrderFlatFileContent_DropsBadRowKeepsRest(t *testing.T) {
	configJSON := `{
		"orderBy": [[0,8]],
		"flatFile": {
			"fieldOrder": ["FieldA", "FieldB"],
			"fields": {
				"FieldA": {"substring": [[0, 8]]},
				"FieldB": {"substring": [[8, 10]]}
			}
		}
	}`
	var backend conf.StorageBackend
	if err := json.Unmarshal([]byte(configJSON), &backend); err != nil {
		t.Fatal(err)
	}

	entities := []*uda.Entity{
		{ID: "a:1", Properties: map[string]interface{}{"a:FieldA": "22222222", "a:FieldB": "02"}},
		{ID: "a:2", Properties: map[string]interface{}{"a:FieldB": "01"}}, // missing FieldA -> bad row
		{ID: "a:3", Properties: map[string]interface{}{"a:FieldA": "11111111", "a:FieldB": "03"}},
	}

	content, stored, err := GenerateAndOrderFlatFileContent(entities, backend, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("expected no error, batch should continue despite bad row: %v", err)
	}

	got := string(content)
	if strings.Contains(got, "  ") {
		t.Errorf("expected bad row to be dropped, got content containing blank FieldA: %q", got)
	}
	if !strings.Contains(got, "1111111103") || !strings.Contains(got, "2222222202") {
		t.Errorf("expected the two valid rows to be present and ordered, got: %q", got)
	}
	// valid rows should be sorted ascending by FieldA: 11111111 before 22222222
	if strings.Index(got, "11111111") > strings.Index(got, "22222222") {
		t.Errorf("expected rows to be ordered ascending by FieldA, got: %q", got)
	}

	if len(stored) != 2 {
		t.Fatalf("expected 2 stored entities, got %d", len(stored))
	}
	for _, id := range []string{"a:2"} {
		for _, e := range stored {
			if e.ID == id {
				t.Errorf("expected dropped entity %q to be excluded from stored entities", id)
			}
		}
	}
}

func TestGenerateAndOrderFlatFileContent_DropsPanickingRowKeepsRest(t *testing.T) {
	// A field typed as "integer" expects a float64 property value (as real json.Unmarshal
	// numbers would produce). Supplying a string here triggers a failed type assertion
	// panic deep in the flat file encoder. This must be recovered and the row dropped,
	// not allowed to crash/fail the whole batch.
	configJSON := `{
		"orderBy": [[0,8]],
		"flatFile": {
			"fieldOrder": ["FieldA", "FieldC"],
			"fields": {
				"FieldA": {"substring": [[0, 8]]},
				"FieldC": {"substring": [[8, 10]], "type": "integer"}
			}
		}
	}`
	var backend conf.StorageBackend
	if err := json.Unmarshal([]byte(configJSON), &backend); err != nil {
		t.Fatal(err)
	}

	entities := []*uda.Entity{
		{ID: "a:1", Properties: map[string]interface{}{"a:FieldA": "11111111", "a:FieldC": float64(2)}},
		{ID: "a:2", Properties: map[string]interface{}{"a:FieldA": "22222222", "a:FieldC": "not-a-number"}}, // wrong type -> panics
		{ID: "a:3", Properties: map[string]interface{}{"a:FieldA": "33333333", "a:FieldC": float64(3)}},
	}

	content, stored, err := GenerateAndOrderFlatFileContent(entities, backend, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("expected no error, batch should continue despite panicking row: %v", err)
	}

	got := string(content)
	if strings.Contains(got, "22222222") {
		t.Errorf("expected panicking row to be dropped, but it appeared in content: %q", got)
	}
	if !strings.Contains(got, "11111111") || !strings.Contains(got, "33333333") {
		t.Errorf("expected good rows to be present, got: %q", got)
	}

	if len(stored) != 2 {
		t.Fatalf("expected 2 stored entities, got %d", len(stored))
	}
	for _, e := range stored {
		if e.ID == "a:2" {
			t.Errorf("expected panicking entity %q to be excluded from stored entities", e.ID)
		}
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
				Dataset:          "<Dataset>",
				IdNamespace:      "<IdNamespace>",
				DefaultNamespace: "<DefaultNamespace>"},
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
		logger:            zap.NewNop().Sugar(),
		env:               &conf.Env{Env: env},
		datahubAuthConfig: conf.DatahubAuthConfig{Audience: "audience"},
		config: conf.StorageBackend{
			DeliverOnceConfig: conf.DeliverOnceConfig{
				Enabled:          true,
				Dataset:          "<Dataset>",
				IdNamespace:      "<IdNamespace>",
				DefaultNamespace: "<DefaultNamespace>"},
		},
	}
	err := storage.DeliverOnceVariableCheck()
	if err != nil {
		t.Error(err)
	}
}
