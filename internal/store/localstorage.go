package store

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"io"
	"os"
	"path/filepath"
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

func GetFolderStructure(rootPath string) []string{
	var fileList []string
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}

		fileList = append(fileList, path)

		//fmt.Printf("dir: %v: name: %s\n", info.IsDir(), path)
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
	return fileList
}

func GetAllFiles(config conf.StorageBackend) []string {

	//get all folders and subfolders
	// get all files with any give extension
	//read files in to entities based on folderstructure
	//readFolders := io.ReadAll(folderstructure)

	fileList := GetFolderStructure(config.LocalFileConfig.RootFolder)
	for count, file := range fileList {
		if strings.Contains(file, "csv"){
			//read file
			fmt.Print(count, "files in folder")
		}

	}
	return fileList
}


func NewLocalStorage(logger *zap.SugaredLogger, env *conf.Env, statsd statsd.ClientInterface, config conf.StorageBackend) (StorageInterface, error) {
	test := statsd
	fullList := GetAllFiles(config)
	logger.Debug(fullList, test, env)
	return nil, nil
}
