package app

import (
	"context"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/security"
	"github.com/mimiro-io/objectstorage-datalayer/internal/store"
	"github.com/mimiro-io/objectstorage-datalayer/internal/web"
	"go.uber.org/fx"
)

func wire() *fx.App {
	app := fx.New(
		fx.Provide(
			conf.NewEnv,
			conf.NewStatsd,
			conf.NewLogger,
			security.NewTokenProviders,
			store.NewStorageEngine,
			conf.NewConfigurationManager,
			web.NewWebServer,
			web.NewMiddleware,
		),
		fx.Invoke(
			web.Register,
			web.NewDatasetHandler,
		),
	)
	return app
}

func Run() {
	wire().Run()
}

func Start(ctx context.Context) (*fx.App, error) {
	app := wire()
	err := app.Start(ctx)
	return app, err
}
