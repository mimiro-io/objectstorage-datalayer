package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
)

type AzureStorage struct {
	logger  *zap.SugaredLogger
	env     *conf.Env
	config  conf.StorageBackend
	statsd  statsd.ClientInterface
	dataset string
}

func (azStorage *AzureStorage) GetEntities() (io.Reader, error) {
	return nil, errors.New("GetEntities not supported for AzureStorage")
}

func (azStorage *AzureStorage) GetChanges() (io.Reader, error) {
	return nil, errors.New("GetChanges not supported for AzureStorage")
}

func NewAzureStorage(logger *zap.SugaredLogger, env *conf.Env, config conf.StorageBackend, statsd statsd.ClientInterface, dataset string) *AzureStorage {
	return &AzureStorage{
		logger:  logger.Named("azure-store"),
		env:     env,
		config:  config,
		dataset: dataset,
		statsd:  statsd,
	}
}

func (azStorage *AzureStorage) GetConfig() conf.StorageBackend {
	return azStorage.config
}

func (azStorage *AzureStorage) StoreEntities(entities []*entity.Entity) error {
	azStorage.logger.Debugf("Got: %d entities", len(entities))
	tags := []string{
		"datalayer",
		"azure",
		azStorage.dataset,
	}

	start := time.Now()
	defer func() {
		timed := time.Since(start)
		_ = azStorage.statsd.Timing("storage.time", timed, tags, 1)
	}()

	azUrl, err := azStorage.createURL(entities)
	if err != nil {
		azStorage.logger.Errorf("Unable to construct url with error: " + err.Error())
	}

	credential, err := azStorage.azureBlobCredentials()
	if err != nil {
		azStorage.logger.Errorf("Invalid credentials with error: " + err.Error())
		return err
	}

	content, err := GenerateContent(entities, azStorage.config, azStorage.logger)
	if err != nil {
		azStorage.logger.Errorf("Unable to create stores content")
		return err
	}

	err = azStorage.upload(content, azUrl, credential)

	return err
}

func (azStorage *AzureStorage) StoreEntitiesFullSync(state FullSyncState, entities []*entity.Entity) error {
	return errors.New("fullsync not supported for Azure")
}

func (azStorage *AzureStorage) azureBlobCredentials() (azblob.Credential, error) {
	config := azStorage.config.Properties

	if config.AuthType != nil && *config.AuthType == "SAS" {
		credentials := azblob.NewAnonymousCredential()
		return credentials, nil
	} else {
		credentials, err := azblob.NewSharedKeyCredential(*config.Key, *config.Secret)
		if err != nil {
			log.Fatal("Invalid credentials with error: " + err.Error())
		}
		return credentials, err
	}
}

func (azStorage *AzureStorage) upload(content []byte, url *url.URL, credential azblob.Credential) error {

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	blobURL := azblob.NewBlockBlobURL(*url, p)
	ctx := context.Background()

	_, err := azblob.UploadBufferToBlockBlob(ctx, content, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				azStorage.logger.Infof("Received 409. Container already exists")
				return serr
			}
			azStorage.logger.Warn(serr)
			return serr
		}
		azStorage.logger.Warn(err)
	}
	return err
}

func (azStorage *AzureStorage) createURL(entities []*entity.Entity) (*url.URL, error) {
	config := azStorage.config.Properties

	var rootFolder string
	if config.RootFolder != nil && *config.RootFolder != "" {
		rootFolder = *config.RootFolder
	} else {
		rootFolder = azStorage.dataset
	}

	year, month, day := time.Now().Date()

	prefix := ""
	if config.FilePrefix != nil && *config.FilePrefix != "" {
		prefix = *config.FilePrefix
	}

	if entities[0].Recorded != "" {
		prefix = prefix + entities[0].Recorded + "-"
	}

	filename := fmt.Sprintf("%s%s.json", prefix, uuid.New().String())
	blobname := fmt.Sprintf("%s/%s/%d/%d/%d/%s", azStorage.env.Env, rootFolder, year, int(month), day, filename)
	urlString := fmt.Sprintf("%s/%s/%s", config.Endpoint, *config.ResourceName, blobname)
	if config.AuthType != nil && *config.AuthType == "SAS" {
		urlString = fmt.Sprintf("%s?%s", urlString, *config.Secret)
	}
	return url.Parse(urlString)
}
