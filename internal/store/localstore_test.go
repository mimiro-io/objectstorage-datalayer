package store

import (
	"github.com/franela/goblin"
	"strings"
	"testing"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
)

func TestLocalstore(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The localstore backend", func() {
		g.It("Should list files in folder", func() {
			ls := LocalStorage{dataset: "testfolder"}
			ls.config = conf.StorageBackend{LocalFileConfig: &conf.LocalFileConfig{
				RootFolder: "../../resources/test/data",
			}}
			objects, err := ls.findObjects(ls.config.LocalFileConfig.RootFolder)
			if err != nil {
				panic(err)
			}
			var resultList []string
			for _, object := range objects {
				resultList = append(resultList, object.FilePath)
			}
			g.Assert(resultList).IsNotNil()
			expected := "../../resources/test/data/changes-1.json,../../resources/test/data/changes-2.json,../../resources/test/data/changes-3.json,../../resources/test/data/flatfile-changes-1.txt,../../resources/test/data/flatfile-changes-2.txt,../../resources/test/data/flatfile-changes-3.txt,../../resources/test/data/localstore-test.csv,../../resources/test/data/s3-test-1.json,../../resources/test/data/s3-test-1v2.json,../../resources/test/data/s3-test-2.json,../../resources/test/data/s3-test-3.json,../../resources/test/data/stripped.ndjson,../../resources/test/data/unstripped.ndjson"
			expectedList := strings.Split(expected, ",")
			g.Assert(resultList).Eql(expectedList)
		})
		/*g.It("Should not return folders", func() {
			ls := LocalStorage{dataset: "testfolder"}
			ls.config = conf.StorageBackend{LocalFileConfig: &conf.LocalFileConfig{
				RootFolder: "../../resources/test/data",
			}}
			objects, err := ls.findObjects(ls.config.LocalFileConfig.RootFolder)
			if err != nil {
				panic(err)
			}
			var resultList []string
			for _, object := range objects {
				resultList = append(resultList, object.FilePath)
			}
			expected := "../../resources/test/data/changes-1.json,../../resources/test/data/changes-2.json,../../resources/test/data/changes-3.json,../../resources/test/data/flatfile-changes-1.txt,../../resources/test/data/flatfile-changes-2.txt,../../resources/test/data/flatfile-changes-3.txt,../../resources/test/data/localstore-test.csv,../../resources/test/data/s3-test-1.json,../../resources/test/data/s3-test-1v2.json,../../resources/test/data/s3-test-2.json,../../resources/test/data/s3-test-3.json,../../resources/test/data/stripped.ndjson,../../resources/test/data/unstripped.ndjson"
			expectedList := strings.Split(expected, ",")
			g.Assert(expectedList)
		})*/
	})
}
