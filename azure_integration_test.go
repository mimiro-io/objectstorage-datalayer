//go:build integration
// +build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/docker/go-connections/nat"
	"github.com/franela/goblin"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/mimiro-io/objectstorage-datalayer/internal/app"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx"
)

func TestAzure(t *testing.T) {
	g := goblin.Goblin(t)
	var fxApp *fx.App
	layerUrl := "http://localhost:19898/datasets"

	g.Describe("The azure storage", func() {
		var endpoint string
		ctx, cancel := context.WithCancel(context.Background())
		var testConf *os.File
		var azureContainer testcontainers.Container
		var plainContainerName = "azure-plain"
		var parquetContainerName = "azure-parquet"
		var mixedContainerName = "azure-mixed"
		var client *azblob.Client
		g.Before(func() {
			os.Setenv("SERVER_PORT", "19898")
			os.Setenv("AUTHORIZATION_MIDDLEWARE", "noop")

			pool, _ := dockertest.NewPool("")
			containers, _ := pool.Client.ListContainers(docker.ListContainersOptions{All: false})
			endpoint = ""
			for _, c := range containers {
				if c.Image == "mcr.microsoft.com/azure-storage/azurite" {
					for _, p := range c.Ports {
						if p.PrivatePort == 10000 {
							endpoint = fmt.Sprintf("localhost:%v", p.PublicPort)
							t.Log("found azurite running on endpoint " + endpoint)
							break
						}
					}
					break
				}
			}

			if endpoint == "" {
				ctx := context.Background()
				req := testcontainers.ContainerRequest{
					Image:           "mcr.microsoft.com/azure-storage/azurite",
					ExposedPorts:    []string{"10000"},
					Cmd:             []string{"azurite-blob", "--blobHost", "0.0.0.0"},
					Entrypoint:      nil,
					WaitingFor:      wait.ForLog("Azurite Blob service"),
					AlwaysPullImage: true,
				}

				var err error
				azureContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
					ContainerRequest: req,
					Started:          true,
				})
				if err != nil {
					t.Error(err)
				}
				actualPort, _ := azureContainer.MappedPort(ctx, nat.Port("10000/tcp"))
				ip, _ := azureContainer.Host(ctx)

				port := actualPort.Port()
				endpoint = ip + ":" + port
			}
			// use new format with create client for azure
			credential, _ := azblob.NewSharedKeyCredential("devstoreaccount1",
				"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
			azUrl := fmt.Sprintf("http://%s/devstoreaccount1", endpoint)
			client, _ = azblob.NewClientWithSharedKeyCredential(azUrl, credential, nil)
			// Create the container on the service (with no metadata and public access)
			_, err := client.CreateContainer(ctx, plainContainerName, nil)
			_, err = client.CreateContainer(ctx, parquetContainerName, nil)
			_, err = client.CreateContainer(ctx, mixedContainerName, nil)

			if err != nil {
				fmt.Println("couldn't create containers")
			}

		})
		g.After(func() {
			if azureContainer != nil {
				_ = azureContainer.Terminate(ctx)
			}
			if fxApp != nil {
				fxApp.Stop(ctx)
			}
			cancel()
		})
		g.BeforeEach(func() {
			if fxApp != nil {
				fxApp.Stop(ctx)
			}

			//reset azure container
			_, _ = client.DeleteContainer(ctx, plainContainerName, nil)
			_, _ = client.CreateContainer(ctx, plainContainerName, nil)
			_, _ = client.DeleteContainer(ctx, parquetContainerName, nil)
			_, _ = client.CreateContainer(ctx, parquetContainerName, nil)
			_, _ = client.DeleteContainer(ctx, mixedContainerName, nil)
			_, _ = client.CreateContainer(ctx, mixedContainerName, nil)

			stdErr := os.Stderr
			stdOut := os.Stdout
			devNull, _ := os.Open("/dev/null")
			os.Stderr = devNull
			os.Stdout = devNull
			testConf = ReplaceTestConf("./resources/test/azure-test-config.json", endpoint, t)
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
			g.Assert(strings.Contains(body, "{\"name\":\"azure-plain\",\"type\":[\"POST\"]}")).IsTrue()
		})

		g.It("Should reject attempts to upload fullsyncs", func() {
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			req, _ := http.NewRequest("POST", layerUrl+"/azure-plain/entities", strings.NewReader(string(fileBytes)))
			req.Header.Add("universal-data-api-full-sync-start", "true")
			req.Header.Add("universal-data-api-full-sync-id", "42")
			res, err := http.DefaultClient.Do(req)
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(400)
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes)
			g.Assert(body).Eql("{\"message\":\"full sync not supported on dataset type\"}\n")
		})

		g.It("Should accept incremental file uploads", func() {
			g.Timeout(1 * time.Hour)
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			res, err := http.DefaultClient.Post(layerUrl+"/azure-plain/entities",
				"application/json", strings.NewReader(string(fileBytes)))
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)
			items := GetBlobItems(client, plainContainerName, "")
			g.Assert(len(items)).Eql(1)
			content := ReadBlobContents(*client, plainContainerName, *items[0].Name, uint64(*items[0].Properties.ContentLength))

			var entities []map[string]interface{}
			_ = json.Unmarshal(content, &entities)
			g.Assert(len(entities)).Eql(3)
		})

		g.It("Should store parquet files", func() {
			g.Timeout(1 * time.Hour)
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()

			// store first file
			res, err := http.DefaultClient.Post(layerUrl+"/azure-parquet/entities",
				"application/json", strings.NewReader(string(fileBytes)))
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			// store a second file
			res, err = http.DefaultClient.Post(layerUrl+"/azure-parquet/entities",
				"application/json", strings.NewReader(string(fileBytes)))
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)

			items := GetBlobItems(client, parquetContainerName, "")
			g.Assert(len(items)).Eql(2)

			content := ReadBlobContents(*client, parquetContainerName, *items[0].Name, uint64(*items[0].Properties.ContentLength))
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(content), "id", "firstname")
			g.Assert(err).IsNil()

			g.Assert(pqReader.NumRows()).Eql(int64(3))
			row, _ := pqReader.NextRow()
			g.Assert(string(row["id"].([]byte))).Eql("a:1")
			g.Assert(string(row["firstname"].([]byte))).Eql("Frank")
		})

		g.It("Should download files and unmarshal", func() {
			g.Timeout(1 * time.Hour)
			fileBytes, err := ioutil.ReadFile("./resources/test/data/s3-test-1.json")
			g.Assert(err).IsNil()
			res, err := http.DefaultClient.Post(layerUrl+"/azure-plain/entities",
				"application/json", strings.NewReader(string(fileBytes)))
			g.Assert(err).IsNil()
			g.Assert(res.StatusCode).Eql(200)
			items := GetBlobItems(client, plainContainerName, "")

			g.Assert(len(items)).Eql(1)
			content := ReadBlobContents(*client, plainContainerName, *items[0].Name, uint64(*items[0].Properties.ContentLength))

			var entities []map[string]interface{}
			_ = json.Unmarshal(content, &entities)
			g.Assert(len(entities)).Eql(3)
		})
	})
}

// GetBlobItems return list of blobs in the storage account
func GetBlobItems(client *azblob.Client, containerName string, prefix string) (blobItems []container.BlobItem) {

	fmt.Println("Listing the blobs in the container:")

	pager := *client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	blobItems = make([]container.BlobItem, 0)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			fmt.Println("error reading the blobs")
		}
		for _, blob := range resp.Segment.BlobItems {
			blobItems = append(blobItems, *blob)
		}
	}
	return blobItems
}

// ReadBlobContents returns the byte array of the content of blob
func ReadBlobContents(client azblob.Client, containerName string, blobName string, blobsize uint64) []byte {

	ctx := context.Background()
	get, _ := client.DownloadStream(ctx, containerName, blobName, nil)

	b := bytes.Buffer{}
	retryReader := get.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	_, err := b.ReadFrom(retryReader)
	if err != nil {
		fmt.Println(err)
	}

	err = retryReader.Close()
	if err != nil {
		fmt.Println(err)
	}

	return b.Bytes()
}
