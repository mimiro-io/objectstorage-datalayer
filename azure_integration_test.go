// +build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/docker/go-connections/nat"
	"github.com/franela/goblin"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx"

	"github.com/mimiro-io/objectstorage-datalayer/internal/app"
)

func TestAzure(t *testing.T) {
	g := goblin.Goblin(t)
	var fxApp *fx.App
	layerUrl := "http://localhost:19898/datasets"
	var containerURL azblob.ContainerURL

	g.Describe("The azure storage", func() {
		var endpoint string
		ctx, cancel := context.WithCancel(context.Background())
		var testConf *os.File
		var azureContainer testcontainers.Container
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
					Image:        "mcr.microsoft.com/azure-storage/azurite",
					ExposedPorts: []string{"10000"},
					Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0"},
					Entrypoint:   nil,
					WaitingFor:   wait.ForLog("Azurite Blob service"),
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

			u, _ := url.Parse("http://" + endpoint + "/devstoreaccount1")
			cred, _ := azblob.NewSharedKeyCredential("devstoreaccount1",
				"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
			// Create an ServiceURL object that wraps the service URL and a request pipeline.
			serviceURL := azblob.NewServiceURL(*u, azblob.NewPipeline(cred, azblob.PipelineOptions{}))
			//ctx := context.Background()
			containerURL = serviceURL.NewContainerURL("local")
			// Create the container on the service (with no metadata and public access)
			_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessContainer)
			if err != nil {
				//								t.Error(err)
			}
			/*
							//blobURL := containerURL.NewBlockBlobURL("/foo/2021/08/12/dfasdfhei.txt?Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
							url,_ := url.Parse("http://localhost:10000/devstoreaccount1/local/azuretest/foo/2021/08/12/dfasdfhei.txt?")// +
				//				"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
							blobURL := azblob.NewBlockBlobURL(
								*url,
								azblob.NewPipeline(cred, azblob.PipelineOptions{}))

							res, err := azblob.UploadBufferToBlockBlob(ctx, []byte("hei"), blobURL, azblob.UploadToBlockBlobOptions{
								BlockSize:   4 * 1024 * 1024,
								Parallelism: 16})

							t.Log(err)
							t.Log(fmt.Sprintf("%+v",res))
			*/
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
			_, _ = containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
			_, _ = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessContainer)

			stdErr := os.Stderr
			stdOut := os.Stdout
			devNull, _ := os.Open("/dev/null")
			os.Stderr = devNull
			os.Stdout = devNull
			testConf = replaceTestConf("./resources/test/azure-test-config.json", endpoint, t)
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
			items := GetBlobItems(containerURL, "")
			g.Assert(len(items)).Eql(1)
			content := ReadBlobContents(containerURL, items[0].Name, uint64(*items[0].Properties.ContentLength))

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

			items := GetBlobItems(containerURL, "")
			g.Assert(len(items)).Eql(2)

			content := ReadBlobContents(containerURL, items[0].Name, uint64(*items[0].Properties.ContentLength))
			pqReader, err := goparquet.NewFileReader(bytes.NewReader(content), "id", "firstname")
			g.Assert(err).IsNil()

			g.Assert(pqReader.NumRows()).Eql(int64(3))
			row, _ := pqReader.NextRow()
			g.Assert(string(row["id"].([]byte))).Eql("a:1")
			g.Assert(string(row["firstname"].([]byte))).Eql("Frank")
		})
	})
}

// GetBlobItems return list of blobs in the storage account
func GetBlobItems(containerURL azblob.ContainerURL, prefix string) (blobItems []azblob.BlobItemInternal) {
	ctx := context.Background()
	// log.Printf("Get Blob Items: %s", prefix)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		options := azblob.ListBlobsSegmentOptions{}
		options.Details.Metadata = true
		if prefix != "" {
			options.Prefix = prefix
		}
		listBlob, err := containerURL.ListBlobsHierarchySegment(ctx, marker, "/", options)
		if err != nil {
			fmt.Printf("Error")
		}
		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobItems = append(blobItems, blobInfo)
		}

		for _, blobPrefix := range listBlob.Segment.BlobPrefixes {
			for _, nestedItem := range GetBlobItems(containerURL, blobPrefix.Name) {
				blobItems = append(blobItems, nestedItem)
			}
		}
	}
	return blobItems
}

// ReadBlobContents returns the byte array of the content of blob
func ReadBlobContents(containerURL azblob.ContainerURL, blobName string, blobsize uint64) []byte {
	ctx := context.Background()
	// log.Printf("RedBlobContent: %s", blobName)
	blobURL := containerURL.NewBlobURL(blobName)
	b := make([]byte, blobsize)
	o := azblob.DownloadFromBlobOptions{
		Parallelism: 5,
	}
	err := azblob.DownloadBlobToBuffer(ctx, blobURL, 0, 0, b, o)
	if err != nil {
		fmt.Errorf("%w", err)
	}
	return b
}
