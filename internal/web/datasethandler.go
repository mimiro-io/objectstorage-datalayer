package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"github.com/mimiro-io/objectstorage-datalayer/internal/store"
)

type datasetHandler struct {
	logger   *zap.SugaredLogger
	storages *store.StorageEngine
	config   *conf.ConfigurationManager
	env      *conf.Env
}

type DatasetName struct {
	Name string   `json:"name"`
	Type []string `json:"type"`
}

func NewDatasetHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, storages *store.StorageEngine, config *conf.ConfigurationManager, env *conf.Env) {
	log := logger.Named("web")
	dh := &datasetHandler{
		logger: log,
		config: config,
		env:    env,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.GET("/datasets", dh.listDatasetsHandler, mw.authorizer(log, "datahub:r"))
			e.POST("/datasets/:dataset/entities", dh.datasetHandler, mw.authorizer(log, "datahub:w"))
			e.GET("/datasets/:dataset/entities", dh.getDatasetHandler, mw.authorizer(log, "datahub:r"))
			e.GET("/datasets/:dataset/changes", dh.getChangesHandler, mw.authorizer(log, "datahub:r"))
			dh.storages = storages
			return nil
		},
	})
}
func (dh *datasetHandler) getChangesHandler(c echo.Context) error {
	c.Set("changes", true)
	return dh.getDatasetHandler(c)
}

func (dh *datasetHandler) getDatasetHandler(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))

	// grab the storage backend
	storage, err := dh.storages.Storage(datasetName)
	if err != nil {
		dh.logger.Warnw(err.Error(), "dataset", datasetName)
		return echo.ErrNotFound
	}
	var reader io.Reader
	if c.Get("changes") == true {
		reader, err = storage.GetChanges()
	} else {
		reader, err = storage.GetEntities()
	}
	_, err = io.Copy(c.Response().Writer, reader)
	if err != nil {
		dh.logger.Warnw(err.Error(), "dataset", datasetName)
		return echo.ErrInternalServerError
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().Flush()
	return nil
}

func (dh *datasetHandler) listDatasetsHandler(c echo.Context) error {
	datasets := make([]DatasetName, 0)

	for _, v := range dh.config.Datalayer.StorageMapping {
		datasets = append(datasets, DatasetName{Name: v.Dataset, Type: []string{"POST"}})
	}
	return c.JSON(http.StatusOK, datasets)
}

func extractState(request *http.Request) store.FullSyncState {
	id := request.Header.Get("universal-data-api-full-sync-id")
	start := request.Header.Get("universal-data-api-full-sync-start")
	end := request.Header.Get("universal-data-api-full-sync-end")

	return store.FullSyncState{
		Id:    id,
		Start: start == "true",
		End:   end == "true",
	}
}

func (dh *datasetHandler) datasetHandler(c echo.Context) error {
	if c.Request().Header.Get("universal-data-api-full-sync-id") != "" {
		return dh.datasetStoreFullSync(c)
	}
	return dh.datasetStore(c)
}
func (dh *datasetHandler) datasetStoreFullSync(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))

	finalState := extractState(c.Request())
	if finalState.Start {
		dh.logger.Infow(fmt.Sprintf("Incoming new fullsync request for %v, id %s", datasetName, finalState.Id),
			"dataset", datasetName)
	}
	if finalState.End {
		dh.logger.Infow(fmt.Sprintf("Incoming finalize fullsync request for %v, id %s", datasetName, finalState.Id),
			"dataset", datasetName)
	}
	// we need the "end" flag only in the last step, for everything before in our parse loop we use an un-ended state
	state := store.FullSyncState{Id: finalState.Id, Start: finalState.Start}

	// grab the storage backend
	storage, err := dh.storages.Storage(datasetName)
	if err != nil {
		dh.logger.Warnw(err.Error(), "dataset", datasetName)
		return echo.ErrNotFound
	}

	storeConfig := storage.GetConfig()

	if !strings.HasPrefix(strings.ToLower(storeConfig.StorageType), "s3") {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("full sync not supported on dataset type").Error())
	}

	// parse it
	batchSize := 10000
	requestedBatchSize := c.QueryParam("batchSize")
	if requestedBatchSize != "" {
		batchSize, err = strconv.Atoi(requestedBatchSize)
	}

	err = entity.ParseStream(c.Request().Body, func(entities []*entity.Entity) error {
		err2 := storage.StoreEntitiesFullSync(state, entities)
		if err2 != nil {
			return err2
		}
		state.Start = false      //only start once
		finalState.Start = false //only start once

		return nil
	}, batchSize, storeConfig.StoreDeleted)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("could not parse the or process json payload").Error())
	}

	if finalState.End {
		defer dh.storages.Close(datasetName)
		err := storage.StoreEntitiesFullSync(finalState, nil)
		if err != nil {
			dh.logger.Errorw(err.Error(), "err", err, "dataset", datasetName)
			return echo.NewHTTPError(http.StatusBadRequest, errors.New("error in StoreEntitiesFullSync").Error())
		}
	}

	return c.NoContent(http.StatusOK)
}

func (dh *datasetHandler) datasetStore(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))

	dh.logger.Infow(fmt.Sprintf("Got dataset %s", datasetName), "dataset", datasetName)

	// grab the storage backend
	storage, err := dh.storages.Storage(datasetName)
	if err != nil {
		dh.logger.Warnw(err.Error(), "dataset", datasetName)
		return echo.ErrNotFound
	}
	defer dh.storages.Close(datasetName)

	storeConfig := storage.GetConfig()

	// parse it
	batchSize := 10000

	err = entity.ParseStream(c.Request().Body, func(entities []*entity.Entity) error {
		// filter if storeDeleted is false
		err2 := storage.StoreEntities(entities)

		if err2 != nil {
			return err2
		}
		return nil
	}, batchSize, storeConfig.StoreDeleted)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("could not parse the json payload").Error())
	}

	return c.NoContent(http.StatusOK)
}
