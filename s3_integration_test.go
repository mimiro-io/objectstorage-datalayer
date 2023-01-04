//go:build integration
// +build integration

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/elgohr/go-localstack"
	"github.com/franela/goblin"

	"github.com/mimiro-io/objectstorage-datalayer/internal/app"
	"github.com/olivere/ndjson"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/fx"
)

func TestS3(t *testing.T) {
	g := goblin.Goblin(t)
	var fxApp *fx.App
	layerUrl := "http://localhost:19899/datasets"

	g.Describe("The s3 storage", func() {
		var l *localstack.Instance
		var awsSession *session.Session
		var s3Service *s3.S3
		var endpoint string
		ctx, cancel := context.WithCancel(context.Background())
		var testConf *os.File
		g.Before(func() {
			l, _ = localstack.NewInstance()
			pool, _ := dockertest.NewPool("")
			containers, _ := pool.Client.ListContainers(docker.ListContainersOptions{All: false})
			endpoint = ""
			for _, c := range containers {
				if c.Image == "localstack/localstack:latest" {
					for _, p := range c.Ports {
						if p.PrivatePort == 4566 {
							endpoint = fmt.Sprintf("localhost:%v", p.PublicPort)
							t.Log("found localstack running on endpoint " + endpoint)
							break
						}
					}
					break
				}
			}

			if endpoint == "" {
				t.Log("Starting localstack")
				l.Start()
				endpoint = l.Endpoint(localstack.S3)
			}
			awsSession, _ = session.NewSession(&aws.Config{
				Credentials:      credentials.NewStaticCredentials("not", "empty", ""),
				DisableSSL:       aws.Bool(true),
				Region:           aws.String(endpoints.UsWest1RegionID),
				Endpoint:         aws.String(endpoint),
				S3ForcePathStyle: aws.Bool(true),
			})
			s3Service = s3.New(awsSession)
			s3Service.CreateBucket((&s3.CreateBucketInput{}).SetBucket("s3-test-bucket"))

			os.Setenv("SERVER_PORT", "19899")
			os.Setenv("AUTHORIZATION_MIDDLEWARE", "noop")
		})
		g.After(func() {
			l.Stop()
			fxApp.Stop(ctx)
			cancel()
		})
		g.BeforeEach(func() {
			if fxApp != nil {
				fxApp.Stop(ctx)
			}
			params := &s3.ListObjectsInput{
				Bucket: aws.String("s3-test-bucket"),
			}
			resp, _ := s3Service.ListObjects(params)
			for _, key := range resp.Contents {
				s3Service.DeleteObject(&s3.DeleteObjectInput{
					Bucket: aws.String("s3-test-bucket"),
					Key:    key.Key,
				})
			}
			stdErr := os.Stderr
			stdOut := os.Stdout
			devNull, _ := os.Open("/dev/null")
			os.Stderr = devNull
			os.Stdout = devNull
			testConf = replaceTestConf("./resources/test/s3-test-config.json", endpoint, t)
			defer os.Remove(testConf.Name())
			os.Setenv("CONFIG_LOCATION", "file://"+testConf.Name())
			fxApp, _ = app.Start(ctx)
			os.Stderr = stdErr
			os.Stdout = stdOut
		})
		g.It("Should list all configured datasets", func() {
			res, err := http.Get(layerUrl)
			g.Assert(err).IsNil()
			g.Assert(res).IsNotZero()
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes)
			g.Assert(strings.Contains(body, "{\"name\":\"s3-csv-mapping\",\"type\":[\"POST\"]}")).IsTrue()
			g.Assert(strings.Contains(body, "{\"name\":\"s3-athena\",\"type\":[\"POST\"]}")).IsTrue()
			g.Assert(strings.Contains(body, "{\"name\":\"s3-athena-deletedTrue\",\"type\":[\"POST\"]}")).IsTrue()
			g.Assert(strings.Contains(body, "{\"name\":\"s3-parquet-mapping\",\"type\":[\"POST\"]}")).IsTrue()
			g.Assert(strings.Contains(body, "{\"name\":\"s3-parquet-test\",\"type\":[\"POST\"]}")).IsTrue()
		})
		g.It("Should upload batches larger than the json reader's batch size", func() {
			//g.Timeout(1 * time.Hour)
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			//payload has 3 entities
			// ?batchSize=2 coerces the entity parser to load with batchSize 2.
			time.Sleep(1 * time.Second)
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities?batchSize=1", strings.NewReader(string(fileBytes)))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err := http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			req2, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities?batchSize=1", strings.NewReader(string(fileBytes)))
			req2.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req2)
			g.Assert(err).IsNil()

			req3, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities?batchSize=1", strings.NewReader(string(fileBytes)))
			req3.Header.Add("universal-data-api-full-sync-end", "true")
			req3.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req3)
			g.Assert(err).IsNil()

			fileSize, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			//t.Logf("stored bytes: %v", ByteCountIEC(fileSize))
			g.Assert(int(*fileSize[0])).Eql(1374)
			g.Assert(storedContent).IsNotNil()

			entities := parseToEntities(storedContent)
			g.Assert(len(entities)).Eql(9)
			g.Assert(entities[0].ID).Eql("a:1")
			g.Assert(entities[0].IsDeleted).IsFalse()
			props := entities[0].Properties
			g.Assert(props).IsNotNil()
			g.Assert(props["a:age"].(float64)).Eql(float64(41))
			g.Assert(props["a:firstname"].(string)).Eql("Frank")
			g.Assert(props["a:surname"].(string)).Eql("TheTank")
			g.Assert(props["a:vaccinated"].(bool)).IsFalse()
		})
		g.It("Should upload batches smaller than json reader's batch size", func() {
			//g.Timeout(1 * time.Hour)
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", nil)
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSize, _ := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			//t.Logf("stored bytes: %v", ByteCountIEC(fileSize))
			g.Assert(len(fileSize)).Eql(1)
			g.Assert(int(*fileSize[0])).Eql(1374)
		})
		// big upload. use this to see how memory behaves
		g.It("Should upload files in incremental as separate files", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, _ = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(2, "expect two files for 2 incr uploads")
			g.Assert(int(*fileSizes[0])).Eql(458)
			g.Assert(int(*fileSizes[1])).Eql(458)
		})
		g.It("Should not store entities with deleted = true through /changes endpoint when storeDeleted=false", func() {
			//a1 -> deleted
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1v2.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(int(*fileSizes[0])).Eql(303)
			entities := parseToEntities(storedContent)
			g.Assert(len(entities)).Eql(2)
			g.Assert(entities[0].ID).Eql("a:2")
			g.Assert(entities[0].IsDeleted).IsFalse()
			props := entities[0].Properties
			g.Assert(props).IsNotNil()
			g.Assert(props["a:age"].(float64)).Eql(float64(27))
			g.Assert(props["a:firstname"].(string)).Eql("Fran")
			g.Assert(props["a:surname"].(string)).Eql("TheTan")
			g.Assert(props["a:vaccinated"].(bool)).IsTrue()
		})
		g.It("Should store entities with deleted = true through /changes endpoint when storeDeleted=true", func() {
			//a1 -> deleted
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1v2.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena-deletedTrue/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-athena-deletedTrue")
			g.Assert(int(*fileSizes[0])).Eql(456)
			entities := parseToEntities(storedContent)
			g.Assert(len(entities)).Eql(3)
			g.Assert(entities[0].ID).Eql("a:1")
			g.Assert(entities[0].IsDeleted).IsTrue()
			props := entities[0].Properties
			g.Assert(props).IsNotNil()
			g.Assert(props["a:age"].(float64)).Eql(float64(32))
			g.Assert(props["a:firstname"].(string)).Eql("Frank")
			g.Assert(props["a:surname"].(string)).Eql("TheTank")
			g.Assert(props["a:vaccinated"].(bool)).IsTrue()
		})
		g.It("Should not store entities with deleted = true through /entities endpoint when storeDeleted=false", func() {
			//a1 -> deleted
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1v2.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(int(*fileSizes[0])).Eql(303)
			entities := parseToEntities(storedContent)
			g.Assert(len(entities)).Eql(2)
			g.Assert(entities[0].ID).Eql("a:2")
			g.Assert(entities[0].IsDeleted).IsFalse()
			props := entities[0].Properties
			g.Assert(props).IsNotNil()
			g.Assert(props["a:age"].(float64)).Eql(float64(27))
			g.Assert(props["a:firstname"].(string)).Eql("Fran")
			g.Assert(props["a:surname"].(string)).Eql("TheTan")
			g.Assert(props["a:vaccinated"].(bool)).IsTrue()
		})
		g.It("Should store entities with deleted = true through /entities endpoint when storeDeleted=true", func() {
			//a1 -> deleted
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1v2.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena-deletedTrue/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-athena-deletedTrue")
			g.Assert(int(*fileSizes[0])).Eql(456)
			entities := parseToEntities(storedContent)
			g.Assert(len(entities)).Eql(3)
			g.Assert(entities[0].ID).Eql("a:1")
			g.Assert(entities[0].IsDeleted).IsTrue()
			props := entities[0].Properties
			g.Assert(props).IsNotNil()
			g.Assert(props["a:age"].(float64)).Eql(float64(32))
			g.Assert(props["a:firstname"].(string)).Eql("Frank")
			g.Assert(props["a:surname"].(string)).Eql("TheTank")
			g.Assert(props["a:vaccinated"].(bool)).IsTrue()
		})
		g.It("Should write csv", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-csv-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-csv-mapping")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(110)

		})
		g.It("Should write csv with special characters", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-3.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-csv-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			fileSizes, storedContent := retrieveFirstObjectFromS3(s3Service, "s3-csv-mapping")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(131)

			r := csv.NewReader(bytes.NewReader(storedContent))
			records, err := r.ReadAll()
			g.Assert(err).IsNil()
			g.Assert(records).IsNotNil()

			g.Assert(records[0][0]).Eql("id")
			g.Assert(records[0][1]).Eql("firstname")
			g.Assert(records[0][2]).Eql("surname")
			g.Assert(records[0][3]).Eql("age")
			g.Assert(records[0][4]).Eql("vaccinated")

			g.Assert(records[1][0]).Eql("a:1")
			g.Assert(records[1][1]).Eql("Frank´s")
			g.Assert(records[1][2]).Eql("& Hank's")
			g.Assert(records[1][3]).Eql("10")
			g.Assert(records[1][4]).Eql("false")

			g.Assert(records[2][1]).Eql("Frank & Tank")
			g.Assert(records[2][2]).Eql("Hank, Flank")

			g.Assert(records[3][1]).Eql("\n \t")
			g.Assert(records[3][2]).Eql("/\";")
		})
		g.It("Should write to two datasets in parallel", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()

			req, err := http.NewRequest("POST", layerUrl+"/s3-csv-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "43")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-csv-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "43")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-csv-mapping/entities", nil)
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-csv-mapping")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(184)

			fileSizes, _ = retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(916)
		})
		g.It("Should overwrite file specified in resourceName property in fullsync", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(458)

			// file 2 has only two entities
			fileBytes, err = ioutil.ReadFile("./resources/test/data/s3-test-2.json")
			g.Assert(err).IsNil()
			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ = retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(1, "expect still only one file (overwritten now)")
			g.Assert(int(*fileSizes[0])).Eql(307)
		})
		g.It("Should abandon running fs when new fs is started", func() {
			g.Timeout(1 * time.Hour)
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			fileBytes2, _ := ioutil.ReadFile("./resources/test/data/s3-test-2.json")

			//start first sync
			req, err := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err := http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes2))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "43")
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(400)

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes2))
			req.Header.Add("universal-data-api-full-sync-id", "43")
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(400)

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(0, "expect no files yet, we closed the abandoned fs")

			req, err = http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes2))
			req.Header.Add("universal-data-api-full-sync-end", "true")
			req.Header.Add("universal-data-api-full-sync-id", "43")
			res, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			fileSizes, _ = retrieveFirstObjectFromS3(s3Service, "s3-athena")
			g.Assert(len(fileSizes)).Eql(1, "expect file from sync 43 now")
			g.Assert(int(*fileSizes[0])).Eql(921) //3 times file 2
		})
		g.It("Should write incremental parquet files", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-parquet-mapping/changes")
			g.Assert(len(fileSizes)).Eql(2)
			g.Assert(int(*fileSizes[0])).Eql(339)
			g.Assert(int(*fileSizes[1])).Eql(339)
		})
		g.It("Should write incremental parquet files with optional values", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			// modify input data: let parquet value be null, to check that it is properly omitted
			fileBytes = []byte(strings.ReplaceAll(string(fileBytes), "\"Frank\"", "null"))
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-parquet-mapping/changes")
			g.Assert(len(fileSizes)).Eql(2)
			g.Assert(int(*fileSizes[0])).Eql(330)
			g.Assert(int(*fileSizes[1])).Eql(330)
		})
		g.It("Should write fullsync parquet files", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, err := http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			req, err = http.NewRequest("POST", layerUrl+"/s3-parquet-mapping/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, err = http.DefaultClient.Do(req)
			g.Assert(err).IsNil()

			fileSizes, _ := retrieveFirstObjectFromS3(s3Service, "s3-parquet-mapping/latest")
			g.Assert(len(fileSizes)).Eql(1)
			g.Assert(int(*fileSizes[0])).Eql(1062)
		})

		g.It("Should export athena schemas for parquet datasets", func() {
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			http.Post(layerUrl+"/s3-parquet-mapping/entities", "application/javascript", bytes.NewReader(fileBytes))

			params := &s3.ListObjectsV2Input{
				Bucket: aws.String("s3-test-bucket"),
				Prefix: aws.String("schemas"),
			}
			resp, err := s3Service.ListObjectsV2(params)
			if err != nil {
				fmt.Print(err)
			}
			var fileSizes []*int64
			for _, key := range resp.Contents {
				getRes, _ := s3Service.GetObject(&s3.GetObjectInput{
					Bucket: aws.String("s3-test-bucket"),
					Key:    key.Key,
				})
				fileSizes = append(fileSizes, getRes.ContentLength)
			}

			g.Assert(len(fileSizes)).Eql(2)
			g.Assert(int(*fileSizes[0])).Eql(220) // changes schema
			g.Assert(int(*fileSizes[1])).Eql(219) // latest schema
		})
		g.It("Should return entities from an s3 ndjson fullsync (single file) dataset", func() {
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			var expected []map[string]interface{}
			err := json.Unmarshal(fileBytes, &expected)
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/s3-athena/entities", bytes.NewReader(fileBytes))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			req.Header.Add("universal-data-api-full-sync-end", "true")
			_, _ = http.DefaultClient.Do(req)

			resp, err := http.Get(layerUrl + "/s3-athena/entities")
			g.Assert(err).IsNil()
			g.Assert(resp.StatusCode).Eql(200)
			bodyBytes, _ := io.ReadAll(resp.Body)
			//t.Log(string(bodyBytes))
			var entities []map[string]interface{}
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(entities).Eql(expected)
		})
		g.It("Should return changes from an s3 ndjson incremental (multi file) dataset", func() {
			g.Timeout(10 * time.Second)
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/changes-1.json")
			http.Post(layerUrl+"/s3-athena-stripped/entities", "application/json", bytes.NewReader(fileBytes))
			time.Sleep(1 * time.Second) //need 1 second to get different aws timestamps
			fileBytes, _ = ioutil.ReadFile("./resources/test/data/changes-2.json")
			http.Post(layerUrl+"/s3-athena-stripped/entities", "application/json", bytes.NewReader(fileBytes))
			time.Sleep(1 * time.Second) //need 1 second to get different aws timestamps
			fileBytes, _ = ioutil.ReadFile("./resources/test/data/changes-3.json")
			http.Post(layerUrl+"/s3-athena-stripped/entities", "application/json", bytes.NewReader(fileBytes))
			resp, err := http.Get(layerUrl + "/s3-athena-stripped/changes")
			g.Assert(err).IsNil()
			g.Assert(resp.StatusCode).Eql(200)
			bodyBytes, _ := io.ReadAll(resp.Body)
			//t.Log(string(bodyBytes))
			var entities []map[string]interface{}
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(4, "context plus 3 changes")
			g.Assert(entities[0]["id"]).Eql("@context")
			g.Assert(entities[1]["id"]).Eql("a:1")
			g.Assert(entities[2]["id"]).Eql("a:2")
			g.Assert(entities[3]["id"]).Eql("a:3")
			g.Assert(entities[3]["refs"]).Eql(map[string]interface{}{"b:address": "b:2"})
			g.Assert(entities[3]["props"]).Eql(map[string]interface{}{
				"a:age": 67, "a:firstname": "Dan", "a:surname": "TheMan", "a:vaccinated": true, "id": "a:3"})
		})
		g.It("Should return changes from a s3 flatfile incremental (multi file) dataset", func() {
			g.Timeout(10 * time.Second)
			uploader := s3manager.NewUploaderWithClient(s3Service)

			// upload a flatfile to s3
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/flatfile-changes-1.txt")
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String("s3-test-bucket"),
				Key:    aws.String("s3-flatfile/1.txt"),
				Body:   bytes.NewReader(fileBytes),
			})
			g.Assert(err).IsNil()

			// read flatfile as json entities
			resp, err := http.Get(layerUrl + "/s3-flatfile/changes")
			g.Assert(err).IsNil()
			bodyBytes, _ := io.ReadAll(resp.Body)
			var entities []map[string]interface{}
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()
			g.Assert(len(entities)).Eql(5, "context, continuation and 3 changes")
			var continuationToken string
			for _, entity := range entities {
				if entity["id"] == "@continuation" {
					continuationToken = entity["token"].(string)
				}
			}

			// upload another flatfile to s3
			time.Sleep(1 * time.Second) //need 1 second to get different aws timestamps
			fileBytes, _ = ioutil.ReadFile("./resources/test/data/flatfile-changes-2.txt")
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String("s3-test-bucket"),
				Key:    aws.String("s3-flatfile/2.txt"),
				Body:   bytes.NewReader(fileBytes),
			})
			g.Assert(err).IsNil()

			// read flatfile as json entities
			resp2, err := http.Get(fmt.Sprintf("%s/s3-flatfile/changes?since=%s", layerUrl, continuationToken))
			g.Assert(err).IsNil()
			bodyBytes2, _ := io.ReadAll(resp2.Body)
			var entities2 []map[string]interface{}
			err = json.Unmarshal(bodyBytes2, &entities2)
			g.Assert(err).IsNil()
			g.Assert(len(entities2)).Eql(5, "context, continuation and 3 changes")

			for _, entity := range entities2 {
				if entity["id"] == "@continuation" {
					continuationToken = entity["token"].(string)
				}
			}

			// upload another flatfile to s3
			time.Sleep(1 * time.Second) //need 1 second to get different aws timestamps
			fileBytes, _ = ioutil.ReadFile("./resources/test/data/flatfile-changes-3.txt")
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String("s3-test-bucket"),
				Key:    aws.String("s3-flatfile/3.txt"),
				Body:   bytes.NewReader(fileBytes),
			})
			g.Assert(err).IsNil()

			// read flatfile as json entities
			resp3, err := http.Get(fmt.Sprintf("%s/s3-flatfile/changes?since=%s", layerUrl, continuationToken))
			g.Assert(err).IsNil()
			bodyBytes3, _ := io.ReadAll(resp3.Body)
			var entities3 []map[string]interface{}
			err = json.Unmarshal(bodyBytes3, &entities3)
			g.Assert(err).IsNil()
			g.Assert(len(entities3)).Eql(5, "context, continuation and 3 changes")
		})
		g.It("Should upload and read parquet to S3", func() {
			fileBytes, _ := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			var expected []map[string]interface{}
			err := json.Unmarshal(fileBytes, &expected)
			g.Assert(err).IsNil()

			req, _ := http.NewRequest("POST", layerUrl+"/s3-parquet-test/entities", bytes.NewReader(fileBytes))
			_, _ = http.DefaultClient.Do(req)
			retrieveFirstObjectFromS3(s3Service, "s3-test-bucket")
			resp, err := http.Get(layerUrl + "/s3-parquet-test/changes")
			g.Assert(err).IsNil()

			g.Assert(resp.StatusCode).Eql(200)
			bodyBytes, _ := io.ReadAll(resp.Body)
			var entities []map[string]interface{}
			err = json.Unmarshal(bodyBytes, &entities)
			g.Assert(err).IsNil()

		})
	})
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
func retrieveFirstObjectFromS3(s3Service *s3.S3, bucketname string) ([]*int64, []byte) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String("s3-test-bucket"),
		Prefix: aws.String("datasets/" + bucketname),
	}
	resp, err := s3Service.ListObjectsV2(params)
	if err != nil {
		fmt.Print(err)
	}
	//fmt.Println("files in bucket: " + strconv.Itoa(len(resp.Contents)))
	var res []*int64
	var bodyBytes []byte
	for _, key := range resp.Contents {
		//fmt.Println(*key.Key)
		getRes, _ := s3Service.GetObject(&s3.GetObjectInput{
			Bucket: aws.String("s3-test-bucket"),
			Key:    key.Key,
		})
		//Only content from the last file is returned
		bodyBytes, _ = io.ReadAll(getRes.Body)
		//fmt.Println(string( bodyBytes))
		res = append(res, getRes.ContentLength)
	}
	return res, bodyBytes
}

func parseToEntities(storedContent []byte) []uda.Entity {
	r := ndjson.NewReader(strings.NewReader(string(storedContent)))
	var entities []uda.Entity
	for r.Next() {
		var entity uda.Entity
		if err := r.Decode(&entity); err != nil {
			fmt.Fprintf(os.Stderr, "Decode failed: %v", err)
			return nil
		}
		entities = append(entities, entity)
	}
	if err := r.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Reader failed: %v", err)
		return nil
	}
	return entities
}

func replaceTestConf(fileTemplate string, endpoint string, t *testing.T) *os.File {
	bts, err := ioutil.ReadFile(fileTemplate)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.ReplaceAll(string(bts), "localhost:8888", endpoint)
	tmpfile, err := ioutil.TempFile(".", "integration-test.json")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	return tmpfile
}
