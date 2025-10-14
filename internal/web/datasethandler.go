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
	"github.com/mimiro-io/datahub-client-sdk-go"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/internal-go-util/pkg/uda"

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

	since := c.QueryParam("since")
	if since != "" {
		s, _ := url.QueryUnescape(since)
		c.Set("since", s)
	}

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
		since := c.Get("since")
		reader, err = storage.GetChanges(fmt.Sprintf("%s", since))
	} else {
		reader, err = storage.GetEntities()
	}
	if err != nil {
		dh.logger.Errorw(err.Error(), "dataset", datasetName)
		return echo.ErrInternalServerError
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
		dh.logger.Debugw(fmt.Sprintf("Incoming new fullsync request for %v, id %s", datasetName, finalState.Id),
			"dataset", datasetName)
	}
	if finalState.End {
		dh.logger.Debugw(fmt.Sprintf("Incoming finalize fullsync request for %v, id %s", datasetName, finalState.Id),
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

	if storeConfig.DeliverOnceConfig.Enabled {
		dh.logger.Errorf("full sync not supported with deliver once")
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("full sync not supported with deliver once").Error())
	}
	if !strings.HasPrefix(strings.ToLower(storeConfig.StorageType), "s3") {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("full sync not supported on dataset type").Error())
	}

	// parse it
	batchSize := 10000
	requestedBatchSize := c.QueryParam("batchSize")
	if requestedBatchSize != "" {
		batchSize, err = strconv.Atoi(requestedBatchSize)
	}

	err = entity.ParseStream(c.Request().Body, func(entities []*uda.Entity, entityContext *uda.Context) error {
		if storeConfig.ResolveNamespace {
			entities = uda.ExpandUris(entities, entityContext)
		}
		err2 := storage.StoreEntitiesFullSync(state, entities)
		if err2 != nil {
			return err2
		}
		state.Start = false      //only start once
		finalState.Start = false //only start once

		return nil
	}, batchSize, storeConfig.StoreDeleted)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New(fmt.Sprintf("could not process the json payload: %s", err.Error())).Error())
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
	foo := conf.StorageConfig{}
	fmt.Sprintf("%+v", foo)

	// parse it
	batchSize := 10000

	err = entity.ParseStream(c.Request().Body, func(entities []*uda.Entity, entityContext *uda.Context) error {
		// filter if storeDeleted is false
		if storeConfig.ResolveNamespace {
			entities = uda.ExpandUris(entities, entityContext)
		}
		var deliverOnceClient datahub.Client
		if storeConfig.DeliverOnceConfig.Enabled {
			err := storage.DeliverOnceVariableCheck()

			if err != nil {
				return err
			}
			client, err := storage.DeliverOnceClientInit()
			if err != nil {
				return err
			}
			deliverOnceClient = client
		}
		err2 := storage.StoreEntities(entities)

		if storeConfig.DeliverOnceConfig.Enabled {
			err := storage.DeliverOnce(entities, deliverOnceClient)
			if err != nil {
				return err
			}
		}

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
