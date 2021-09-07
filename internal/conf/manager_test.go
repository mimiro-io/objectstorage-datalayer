package conf

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mimiro-io/objectstorage-datalayer/internal/security"
	"go.uber.org/zap"
)

func TestLoadFile(t *testing.T) {

	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}

	_, err := cmgr.loadFile("file://../../resources/test/test-config.json")
	if err != nil {
		t.FailNow()
	}

}

func TestLoadUrl(t *testing.T) {
	srv := serverMock()
	defer srv.Close()

	cmgr := ConfigurationManager{
		logger:         zap.NewNop().Sugar(),
		TokenProviders: security.NoOpTokenProviders(),
	}

	_, err := cmgr.loadUrl(fmt.Sprintf("%s/test/config.json", srv.URL))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestParse(t *testing.T) {
	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}

	res, err := cmgr.loadFile("file://../../resources/test/test-config.json")
	if err != nil {
		t.FailNow()
	}

	_, err = cmgr.parse(res)
	if err != nil {
		t.FailNow()
	}

}

func serverMock() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/test/config.json", configMock)

	srv := httptest.NewServer(handler)

	return srv
}

func configMock(w http.ResponseWriter, r *http.Request) {
	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}
	res, _ := cmgr.loadFile("file://../../resources/test/test-config.json")
	_, _ = w.Write(res)
}
