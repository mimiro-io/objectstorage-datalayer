package store

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/google/uuid"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"github.com/spf13/cast"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
)

type LocalStorage struct {
	logger         *zap.SugaredLogger
	env            *conf.Env
	config         conf.StorageBackend
	dataset        string
	statsd         statsd.ClientInterface
	writer         encoder.EncodingEntityWriter
	reader         *io.PipeReader
	fullsyncId     string
	waitGroup      sync.WaitGroup
	cancelFunc     context.CancelFunc
	fullsyncTimout *time.Timer
}

type FileInfo struct {
	FilePath     string
	FileSize     int64
	LastModified time.Time
}

func GetAllFiles(path string) []FileInfo {

	fileList := make([]FileInfo, 0)
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}
		if !info.IsDir() {
			file := FileInfo{FilePath: path, FileSize: info.Size(), LastModified: info.ModTime()}
			fileList = append(fileList, file)
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
	return fileList
}

func NewLocalStorage(logger *zap.SugaredLogger, env *conf.Env, statsd statsd.ClientInterface, config conf.StorageBackend, dataset string) *LocalStorage {
	s := &LocalStorage{
		logger:  logger.Named("local-store").With("dataset", dataset),
		env:     env,
		config:  config,
		dataset: dataset,
		statsd:  statsd,
	}
	return s
}

func (ls *LocalStorage) GetConfig() conf.StorageBackend {
	return ls.config
}

func (ls *LocalStorage) StoreEntities(entities []*entity.Entity) error {
	if len(entities) == 0 {
		return nil
	}
	content, err := GenerateContent(entities, ls.config, ls.logger)
	if err != nil {
		ls.logger.Error("Unable to create store content")
	}
	ls.logger.Infof("Encoded %d entities into %v bytes", len(entities), len(content))

	key := ls.createKey(entities, false)
	properties := ls.config.Properties
	//test logging
	ls.logger.Debug(key, properties)
	//result.Location should be in the info log down below.
	ls.logger.Info("Successfully uploaded to testingnotworking")
	return nil
}
func (ls *LocalStorage) StoreEntitiesFullSync(state FullSyncState, entities []*entity.Entity) error {
	if len(entities) == 0 {
		return nil
	}
	content, err := GenerateContent(entities, ls.config, ls.logger)
	if err != nil {
		ls.logger.Error("Unable to create store content")
	}
	ls.logger.Infof("Encoded %d entities into %v bytes", len(entities), len(content))

	key := ls.createKey(entities, false)
	properties := ls.config.Properties
	//test logging
	ls.logger.Debug(key, properties)
	//result.Location should be in the info log down below.
	ls.logger.Info("Successfully uploaded to testingnotworking")
	return nil
}

func (ls *LocalStorage) createKey(entities []*entity.Entity, fullSync bool) string {
	t := "changes"
	if fullSync {
		t = "entities"
	}

	recorded := ""
	if len(entities) > 0 {
		recorded = entities[0].Recorded
		if recorded != "" {
			recorded = recorded + "-"
		}
	}

	ending := "json"
	if ls.config.CsvConfig != nil {
		ending = "csv"
	}
	if ls.config.FlatFileConfig != nil {
		ending = "txt"
	}
	if ls.config.ParquetConfig != nil {
		ending = "parquet"
		if !fullSync {
			for _, p := range ls.config.ParquetConfig.Partitioning {
				year, month, day := time.Now().Date()
				pVal := ""
				if p == "year" {
					pVal = strconv.Itoa(year)
				} else if p == "month" {
					pVal = strconv.Itoa(int(month))
				} else if p == "day" {
					pVal = strconv.Itoa(day)
				}
				if pVal != "" {
					t = fmt.Sprintf("%v/%v=%v", t, p, pVal)
				}
			}
		}
	}

	filename := fmt.Sprintf("%s%s.%s", recorded, uuid.New().String(), ending)
	return fmt.Sprintf("datasets/%s/%s/%s", ls.dataset, t, filename)
}

func (ls *LocalStorage) GetEntities() (io.Reader, error) {
	properties := ls.config.LocalFileConfig
	if properties.RootFolder != "" {
		//key = fmt.Sprintf("%s", *properties.ResourceName)
		fmt.Sprintf("Working on folder: %s", properties.RootFolder)
	} else {
		ls.logger.Error("No folder specified, exiting")
		os.Exit(1)
	}
	reader, writer := io.Pipe()

	files, err := ls.findObjects(properties.RootFolder)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !strings.HasSuffix(file.FilePath, ls.config.LocalFileConfig.FileSuffix) {
			continue
		}
	}

	go func() {
		defer func() {
			_ = writer.Close()
		}()
		for _, fileObj := range files {
			file, err := os.Open(fileObj.FilePath)
			defer file.Close()
			//choose a chunk size
			io.TeeReader(file, writer)

			ls.logger.Infof("read bytes from local file %v", fileObj.FilePath)
			if err != nil {
				ls.logger.Error(err)
				_ = reader.CloseWithError(err)
				break
			}
		}
	}()
	return encoder.NewEntityDecoder(ls.config, reader, "", ls.logger, true)
}

func (ls *LocalStorage) GetChanges(since string) (io.Reader, error) {
	properties := ls.config.LocalFileConfig
	if properties.RootFolder != "" {
		//key = fmt.Sprintf("%s", *properties.ResourceName)
		fmt.Sprintf("Working on folder: %s", properties.RootFolder)
	} else {
		ls.logger.Error("No folder specified, exiting")
		os.Exit(1)
	}
	reader, writer := io.Pipe()
	files, err := ls.findObjects(properties.RootFolder)
	if err != nil {
		return nil, err
	}
	ls.logger.Debugf("Files found:\n%s", files)
	if err != nil {
		return nil, err
	}
	var latestLastModified = time.Now()
	for _, file := range files {
		if strings.HasSuffix(file.FilePath, ls.config.LocalFileConfig.FileSuffix) {
			if !file.LastModified.Before(latestLastModified) {
				latestLastModified = file.LastModified
			}
		}
	}
	go func() {
		defer func() {
			_ = writer.Close()
		}()
		for _, fileObj := range files {
			ls.logger.Debugf("Comparing since values - file lastmodified: %v , request since: %v", fileObj.LastModified, since)
			if fileObj.LastModified.Before(cast.ToTime(since)) {
				latestLastModified = fileObj.LastModified
				//option 1
				//readTotal, err := ioutil.ReadFile(fileObj.FilePath)
				//option 2
				file, err := os.Open(fileObj.FilePath)
				defer file.Close()
				//choose a chunk size
				io.TeeReader(file, writer)

				ls.logger.Infof("read bytes from local file %v", fileObj.FilePath)
				if err != nil {
					ls.logger.Error(err)
					_ = reader.CloseWithError(err)
					break
				}
			}
		}
	}()
	return encoder.NewEntityDecoder(ls.config, reader, cast.ToString(latestLastModified), ls.logger, false)
}

func (ls *LocalStorage) findObjects(folder string) ([]FileInfo, error) {
	var path string
	if folder == "" {
		path = "/"
	} else {
		path = folder
	}
	//get file objects in here, format TBD
	var err error
	result := GetAllFiles(path)
	if err != nil {
		return nil, err
	}
	return result, err
}
