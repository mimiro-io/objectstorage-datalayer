package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"io"
	"log"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
)

type AzureStorage struct {
	logger  *zap.SugaredLogger
	env     *conf.Env
	config  conf.StorageBackend
	statsd  statsd.ClientInterface
	dataset string
}

func (azStorage *AzureStorage) GetEntities() (io.Reader, error) {
	azStorage.logger.Debugf("Getting entities from container: %s", azStorage.config.Properties.ResourceName)
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
	config := azStorage.config.Properties

	// create url and client
	azUrl := azStorage.readURL()
	if azUrl == "" {
		azStorage.logger.Errorf("url is empty, something is missing ")
	}
	client, err := azStorage.createClient(azUrl)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	blobList, err2 := azStorage.generateBlobList(*client)
	ctx := context.Background()
	containerName := *config.Bucket
	go func() {
		defer func() {
			_ = writer.Close()
		}()
		for _, blob := range blobList {
			readTotal, err := client.DownloadStream(ctx, containerName, blob, nil)
			//retry method, look in to it
			/*
					downloadedData := bytes.Buffer{}
					retryReader := get.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
					_, err = downloadedData.ReadFrom(retryReader)
					if err != nil {
						return err
					}
					err = retryReader.Close()
					if err != nil {
						return err
					}
					// Print the contents of the blob we created
					fmt.Println("Blob contents:")
					fmt.Println(downloadedData.String())
				}
			*/
			azStorage.logger.Infof("read %v bytes total from blob %v", readTotal, blob)
			if err != nil {
				azStorage.logger.Error(err)
				_ = reader.CloseWithError(err)
				break
			}
		}
	}()
	if err2 != nil {
		return reader, err2
	}

	// filter out correct endings, or according to spec.

	// create uda entities from files
	return encoder.NewEntityDecoder(azStorage.config, reader, "", azStorage.logger, false)
}

func (azStorage *AzureStorage) GetChanges(since string) (io.Reader, error) {

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

func (azStorage *AzureStorage) StoreEntities(entities []*uda.Entity) error {
	azStorage.logger.Debugf("Got: %d entities", len(entities))
	config := azStorage.config.Properties

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

	azUrl, blobName, err := azStorage.createURL(entities)
	if err != nil {
		azStorage.logger.Errorf("Unable to construct url with error: " + err.Error())
	}
	client, err := azStorage.createClient(azUrl)

	content, err := GenerateContent(entities, azStorage.config, azStorage.logger)
	if err != nil {
		azStorage.logger.Errorf("Unable to create stores content")
		return err
	}

	ctx := context.Background()
	containerName := *config.Bucket
	_, err = client.UploadBuffer(ctx, containerName, blobName, content, &azblob.UploadBufferOptions{
		BlockSize:   4 * 1024 * 1024,
		Concurrency: 16,
	})
	//remove this?
	//err = azStorage.upload(content, azUrl, credential)
	return err
}

func (azStorage *AzureStorage) StoreEntitiesFullSync(state FullSyncState, entities []*uda.Entity) error {
	return errors.New("fullsync not supported for Azure")
}

func (azStorage *AzureStorage) azureBlobCredentials() (*azblob.SharedKeyCredential, error) {
	config := azStorage.config.Properties

	credentials, err := azblob.NewSharedKeyCredential(*config.Key, *config.Secret)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())

	}
	return credentials, err
}

/*
func (azStorage *AzureStorage) upload(content []byte, url string, credential azblob.Credential) error {

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
*/
func (azStorage *AzureStorage) createURL(entities []*uda.Entity) (url string, blobName string, err error) {
	config := azStorage.config.Properties

	var rootFolder string
	if config.RootFolder != nil && *config.RootFolder != "" {
		rootFolder = *config.RootFolder
	} else {
		rootFolder = azStorage.dataset
	}

	prefix := ""
	if config.FilePrefix != nil && *config.FilePrefix != "" {
		prefix = *config.FilePrefix
	}

	if entities[0].Recorded != "" {
		prefix = prefix + entities[0].Recorded + "-"
	}

	ending := "json"
	if azStorage.config.CsvConfig != nil {
		ending = "csv"
	}
	if azStorage.config.FlatFileConfig != nil {
		ending = "txt"
	}
	if azStorage.config.ParquetConfig != nil {
		ending = "parquet"
	}

	filename := fmt.Sprintf("%s%s.%s", prefix, uuid.New().String(), ending)
	blobname := fmt.Sprintf("%s/%s", rootFolder, filename)
	if config.FolderStructure != nil && strings.ToLower(*config.FolderStructure) == "dated" {
		year, month, day := time.Now().Date()
		blobname = fmt.Sprintf("%s/%d/%d/%d/%s", rootFolder, year, int(month), day, filename)
		filename = blobname
	}
	urlString := fmt.Sprintf("%s/%s", config.Endpoint, *config.ResourceName)
	//urlString := config.Endpoint
	if config.AuthType != nil && *config.AuthType == "SAS" {
		urlString = fmt.Sprintf("%s?%s", urlString, *config.Secret)
	}
	return urlString, filename, nil
}
func (azStorage *AzureStorage) createClient(azUrl string) (*azblob.Client, error) {
	//supports SAS and blobcredentials
	config := azStorage.config.Properties
	client := &azblob.Client{}
	//err := errors.New("Error cannot create client")
	if config.AuthType != nil && *config.AuthType == "SAS" {
		client, err := azblob.NewClientWithNoCredential(azUrl, nil)
		if err != nil {
			azStorage.logger.Errorf("Invalid credentials with error: " + err.Error())
			return nil, err
		}
		return client, nil
	} else {
		credential, err := azStorage.azureBlobCredentials()
		if err != nil {
			azStorage.logger.Errorf("Invalid credentials with error: " + err.Error())
			return nil, err
		}
		client, err = azblob.NewClientWithSharedKeyCredential(azUrl, credential, nil)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (azStorage *AzureStorage) readURL() string {
	config := azStorage.config.Properties

	//maybe not needed
	/*	var rootFolder string
		if config.RootFolder != nil && *config.RootFolder != "" {
			rootFolder = *config.RootFolder
		} else {
			rootFolder = azStorage.dataset
		}*/

	urlString := fmt.Sprintf("%s/%s", config.Endpoint, *config.ResourceName)
	if config.AuthType != nil && *config.AuthType == "SAS" {
		urlString = fmt.Sprintf("%s?%s", urlString, *config.Secret)
	}
	return urlString

}

func (azStorage *AzureStorage) generateBlobList(client azblob.Client) ([]string, error) {
	containerName := azStorage.config.Properties.Bucket
	fileList := make([]string, 0)
	pager := client.NewListBlobsFlatPager(*containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})

	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			fileList = append(fileList, "empty")
			return fileList, err
		}

		for _, blob := range resp.Segment.BlobItems {
			fileList = append(fileList, *blob.Name)
			fmt.Println(*blob.Name)
		}
	}

	return fileList, nil
}
