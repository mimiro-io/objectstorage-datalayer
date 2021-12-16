package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
	"github.com/mimiro-io/objectstorage-datalayer/internal/schema"
	"go.uber.org/zap"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"
)

type S3Storage struct {
	logger         *zap.SugaredLogger
	env            *conf.Env
	config         conf.StorageBackend
	dataset        string
	statsd         statsd.ClientInterface
	uploader       *s3manager.Uploader
	downloader     *s3manager.Downloader
	writer         encoder.EncodingEntityWriter
	reader         *io.PipeReader
	fullsyncId     string
	waitGroup      sync.WaitGroup
	cancelFunc     context.CancelFunc
	fullsyncTimout *time.Timer
}
type sequentialWriter struct {
	w io.Writer
}

func (sw sequentialWriter) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return sw.w.Write(p)
}

func (s3s *S3Storage) GetEntities() (io.Reader, error) {
	reader, writer := io.Pipe()
	properties := s3s.config.Properties
	//var key string
	var files []string
	if properties.ResourceName != nil {
		if properties.CustomResourcePath != nil && *properties.CustomResourcePath {
			//key = fmt.Sprintf("%s", *properties.ResourceName)
			fileObjs, err := s3s.findObjects("entities")
			if err != nil {
				return nil, err
			}
			for _, f := range fileObjs {
				files = append(files, f.FilePath)
			}
		} else {
			//key = s3s.fullSyncFixedKey()
			files = append(files, s3s.fullSyncFixedKey())
		}
	} else {
		keyPointer, err := s3s.findNewestKey("entities")
		if err != nil {
			return nil, err
		}
		//key = *keyPointer
		files = append(files, *keyPointer)
	}
	go func() {
		defer func() {
			_ = writer.Close()
		}()
		for _, file := range files {
			readTotal, err := s3s.downloader.Download(
				sequentialWriter{writer}, &s3.GetObjectInput{
					Bucket: aws.String(*properties.Bucket),
					Key:    aws.String(file),
				},
			)
			s3s.logger.Infof("read %v bytes total from s3 file %v", readTotal, file)
			if err != nil {
				s3s.logger.Error(err)
				_ = reader.CloseWithError(err)
				break
			}
		}
	}()
	return encoder.NewEntityDecoder(s3s.config, reader, "", s3s.logger, true)
}

func (s3s *S3Storage) GetChanges(since string) (io.Reader, error) {
	reader, writer := io.Pipe()
	properties := s3s.config.Properties

	files, err := s3s.findObjects("changes")
	s3s.logger.Debugf("Files found:\n%s", files)
	if err != nil {
		return nil, err
	}
	latestLastModified := ""
	for _, file := range files {
		if file.LastModified > latestLastModified {
			latestLastModified = file.LastModified
		}
	}
	go func() {
		defer func() {
			_ = writer.Close()
		}()
		w := sequentialWriter{writer}
		for _, fileObj := range files {
			s3s.logger.Debugf("Comparing since values - file lastmodified: %v , request since: %v", fileObj.LastModified, since)
			if fileObj.LastModified > since {
				latestLastModified = fileObj.LastModified
				readTotal, err := s3s.downloader.Download(
					w, &s3.GetObjectInput{
						Bucket: aws.String(*properties.Bucket),
						Key:    aws.String(fileObj.FilePath),
					},
				)
				s3s.logger.Infof("read %v bytes total from s3 file %v", readTotal, fileObj.FilePath)
				if err != nil {
					s3s.logger.Error(err)
					_ = reader.CloseWithError(err)
					break
				}
			}
		}
	}()
	return encoder.NewEntityDecoder(s3s.config, reader, latestLastModified, s3s.logger, false)
}

func (s3s *S3Storage) ExportSchema() error {
	if s3s.config.ParquetConfig != nil {
		for _, folder := range []string{"changes", "latest"} {
			targetLocation := fmt.Sprintf("s3://%v/datasets/%v/%v/",
				*s3s.config.Properties.Bucket,
				s3s.config.Dataset,
				folder)
			gen, err := schema.NewParquetAthenaSqlBuilder(s3s.config.Dataset,
				s3s.config.ParquetConfig.SchemaDefinition,
				targetLocation)
			if err != nil {
				return err
			}
			gen.WithSnappyCompression()
			if folder == "changes" {
				gen.WithPartitioning(s3s.config.ParquetConfig.Partitioning...)
			}
			ddl, err := gen.Build()
			if err != nil {
				return err
			}
			schemaLocation := fmt.Sprintf("schemas/%v-%v.sql", s3s.config.Dataset, folder)
			uploadResult, err := s3s.uploader.Upload(&s3manager.UploadInput{
				Body:   bytes.NewReader([]byte(ddl)),
				Bucket: aws.String(*s3s.config.Properties.Bucket),
				Key:    aws.String(schemaLocation),
			})
			if err != nil {
				s3s.logger.Error("Failed to upload ", err)
				return err
			}
			s3s.logger.Info("Successfully uploaded athena schema to ", uploadResult.Location)
		}
	}
	return nil
}

func NewS3Storage(logger *zap.SugaredLogger, env *conf.Env, config conf.StorageBackend, statsd statsd.ClientInterface, dataset string) (*S3Storage, error) {
	uploader, downloader, err := initS3(config, env)
	downloader.Concurrency = 1 // disable parallel download of chunks, we need sequential streaming
	if err != nil {
		return nil, err
	}

	s := &S3Storage{
		logger:     logger.Named("s3-store").With("dataset", dataset),
		env:        env,
		config:     config,
		dataset:    dataset,
		statsd:     statsd,
		uploader:   uploader,
		downloader: downloader,
	}

	err = s.ExportSchema()
	if err == nil {
		logger.Info("exported schema for dataset ", dataset)
	}

	return s, err
}

func initS3(config conf.StorageBackend, env *conf.Env) (*s3manager.Uploader, *s3manager.Downloader, error) {
	if env.Env == "local" {
		sess, err := session.NewSession(&aws.Config{
			Credentials:      credentials.NewStaticCredentials(*config.Properties.Key, *config.Properties.Secret, ""),
			S3ForcePathStyle: aws.Bool(true),
			Region:           aws.String(*config.Properties.Region),
			Endpoint:         aws.String(config.Properties.Endpoint),
		})
		if err != nil {
			return nil, nil, err
		}
		return s3manager.NewUploader(sess), s3manager.NewDownloader(sess), nil
	} else {

		//TODO:: verify if key, secret, region is set.. If so it's an external bucket....
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("eu-west-1"),
		})
		if err != nil {
			return nil, nil, err
		}
		return s3manager.NewUploader(sess), s3manager.NewDownloader(sess), nil
	}
}

func (s3s *S3Storage) GetConfig() conf.StorageBackend {
	return s3s.config
}

func (s3s *S3Storage) StoreEntities(entities []*entity.Entity) error {
	if len(entities) == 0 {
		return nil
	}
	content, err := GenerateContent(entities, s3s.config, s3s.logger)
	if err != nil {
		s3s.logger.Error("Unable to create store content")
	}
	s3s.logger.Infof("Encoded %d entities into %v bytes", len(entities), len(content))

	key := s3s.createKey(entities, false)
	properties := s3s.config.Properties
	result, err := s3s.uploader.Upload(&s3manager.UploadInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(*properties.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		s3s.logger.Error("Failed to upload ", err)
		return err
	}
	s3s.logger.Info("Successfully uploaded to ", result.Location)
	return nil
}

func (s3s *S3Storage) createKey(entities []*entity.Entity, fullSync bool) string {
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
	if s3s.config.CsvConfig != nil {
		ending = "csv"
	}
	if s3s.config.FlatFileConfig != nil {
		ending = "txt"
	}
	if s3s.config.ParquetConfig != nil {
		ending = "parquet"
		if !fullSync {
			for _, p := range s3s.config.ParquetConfig.Partitioning {
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
	return fmt.Sprintf("datasets/%s/%s/%s", s3s.dataset, t, filename)
}

var fullsyncTimeoutDuration = 30 * time.Minute

func (s3s *S3Storage) StoreEntitiesFullSync(state FullSyncState, entities []*entity.Entity) error {
	if state.Start {
		s3s.fullsyncId = state.Id
		var pipeWriter *io.PipeWriter
		//each fullsync keeps a pipe in memory which we can write to for the duration of a fullsync
		// amazons uploadmanager will read continuously from the pipe until closed.
		s3s.reader, pipeWriter = io.Pipe()
		s3s.writer = encoder.NewEntityEncoder(s3s.config, pipeWriter, s3s.logger)
		ctx, cancel := context.WithCancel(aws.BackgroundContext())
		s3s.cancelFunc = cancel
		s3s.waitGroup = sync.WaitGroup{}
		s3s.waitGroup.Add(1)
		if s3s.fullsyncTimout != nil {
			s3s.fullsyncTimout.Stop()
		}
		s3s.fullsyncTimout = time.AfterFunc(fullsyncTimeoutDuration, func() {
			s3s.logger.Warnf("fullsync id %v has not received new data in %v. abandoning.", s3s.fullsyncId, fullsyncTimeoutDuration)
			s3s.cancelFunc()
			s3s.writer.CloseWithError(errors.New("abandoned fullsync"))
		})

		properties := s3s.config.Properties
		var key string
		if properties.ResourceName == nil {
			key = s3s.createKey(entities, true)
		} else {
			if properties.CustomResourcePath != nil {
				key = *properties.ResourceName
			} else {
				key = s3s.fullSyncFixedKey()
			}
		}

		if s3s.env.Env == "local" {
			_, err := s3s.CreateBucketIfNotExist()
			if err != nil {
				return err
			}
			s3s.logger.Infof("Writing -> %s", key)
		}

		go func() {
			result, err := s3s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
				Body:   s3s.reader,
				Bucket: aws.String(*properties.Bucket),
				Key:    aws.String(key),
			})
			s3s.waitGroup.Done()
			if err != nil {
				s3s.logger.Error("Failed to upload ", err)
				return
			}
			s3s.logger.Info("Successfully uploaded to ", result.Location)
		}()
	}

	if s3s.writer != nil {
		if state.Id != s3s.fullsyncId {
			s3s.logger.Warnf("Invalid fullsync id. requester sent id %v, ongoing sync has id %v", state.Id, s3s.fullsyncId)
			return errors.New("Invalid fullsync ID")
		}
		// we wrap the writer.Write with a one minute timeout, because it will block and hang indefinitely if the
		// aws upload manager does not read in the other end for some reason.
		if len(entities) > 0 {
			writeCtx, writecancel := context.WithTimeout(context.Background(), 1*time.Minute)
			go func() {
				<-writeCtx.Done()
				if writeCtx.Err() == context.DeadlineExceeded {
					s3s.logger.Errorf("Upload timed out, could not write into uploader withing 1 minute.")
					s3s.cancelFunc()
					_ = s3s.writer.CloseWithError(writeCtx.Err())
				}
			}()
			written, err := s3s.writer.Write(entities)
			writecancel()
			if err != nil {
				return err
			}
			s3s.logger.Infof("piped %v entities into uploader. bytes written: %v", len(entities), written)
		}
		// refresh between-request timeout
		s3s.fullsyncTimout.Reset(fullsyncTimeoutDuration)
		if state.End {
			err := s3s.writer.Close()
			if err != nil {
				return err
			}
			s3s.logger.Info("waiting for uploader")
			s3s.waitGroup.Wait()
			s3s.logger.Info("wait done")
			s3s.fullsyncTimout.Stop()
		}
		return nil
	}
	return errors.New("fullsync is not initialized")
}

func (s3s *S3Storage) fullSyncFixedKey() string {
	return fmt.Sprintf("/datasets/%s/latest/%s", s3s.dataset, *s3s.config.Properties.ResourceName)
}

func (s3s *S3Storage) CreateBucketIfNotExist() (bool, error) {
	config := s3s.config.Properties
	input := &s3.ListBucketsInput{}
	buckets, err := s3s.uploader.S3.ListBuckets(input)
	if err != nil {
		return false, err
	}

	var bucketExist = false
	for i := range buckets.Buckets {
		current := *buckets.Buckets[i].Name
		if current == *config.Bucket {
			bucketExist = true
			break
		}
	}

	if bucketExist {
		return true, nil
	} else {
		input := &s3.CreateBucketInput{
			Bucket: aws.String(*config.Bucket),
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: aws.String(*config.Region),
			},
		}

		_, err := s3s.uploader.S3.CreateBucket(input)
		if err != nil {
			return false, err
		}
		return true, nil
	}

}

func (s3s *S3Storage) findNewestKey(folder string) (*string, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(*s3s.config.Properties.Bucket),
		Prefix: aws.String("datasets/" + s3s.config.Dataset + "/" + folder),
	}
	resp, err := s3s.downloader.S3.ListObjectsV2(params)
	if err != nil {
		return nil, err
	}
	for _, key := range resp.Contents {
		return key.Key, nil
	}
	return nil, errors.New(fmt.Sprintf(
		"nothing found in folder %v of dataset %v", folder, s3s.config.Dataset))
}

type FileObject struct {
	SortKey      string
	FilePath     string
	LastModified string
}

func (s3s *S3Storage) findObjects(folder string) ([]FileObject, error) {
	var path string
	if s3s.config.Properties.CustomResourcePath != nil && *s3s.config.Properties.CustomResourcePath {
		path = *s3s.config.Properties.ResourceName
	} else {
		path = "datasets/" + s3s.config.Dataset + "/" + folder
	}
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(*s3s.config.Properties.Bucket),
		Prefix: aws.String(path),
	}

	resp, err := s3s.downloader.S3.ListObjectsV2(params)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	resultList := make(map[string]FileObject, 0)
	for {
		for _, key := range resp.Contents {
			value := *key.Key
			lastModified := fmt.Sprintf("%v", key.LastModified.UnixNano())
			sortKey := fmt.Sprintf("%v-%v", lastModified, value)
			result[sortKey] = value
			resultList[sortKey] = FileObject{
				FilePath:     value,
				SortKey:      sortKey,
				LastModified: lastModified,
			}

		}
		if *resp.IsTruncated {
			params.SetContinuationToken(*resp.ContinuationToken)
			resp, err = s3s.downloader.S3.ListObjectsV2(params)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if len(result) > 0 {
		//sort by key
		keys := make([]string, 0, len(result))
		for k := range resultList {
			keys = append(keys, k)
		}
		//sort.Sort(sort.Reverse(sort.StringSlice(keys)))
		sort.Strings(keys)

		var sortedByLastModified []FileObject
		for _, k := range keys {
			//fmt.Println(k, result[k])
			sortedByLastModified = append(sortedByLastModified, resultList[k])
		}
		return sortedByLastModified, nil
	}
	return nil, errors.New(fmt.Sprintf(
		"nothing found in folder %v of dataset %v", folder, s3s.config.Dataset))
}
