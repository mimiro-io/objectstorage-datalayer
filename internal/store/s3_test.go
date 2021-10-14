package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/franela/goblin"

	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/entity"
)

func TestS3(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The s3 backend", func() {
		g.It("Should generate partitioned keys if the dataset declares it", func() {
			s3 := S3Storage{dataset: "testds"}
			s3.config = conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				Partitioning: []string{"year", "month", "day", "foo"},
			}}
			var entities []*entity.Entity
			key := s3.createKey(entities, false)
			g.Assert(key[24:28]).Eql("year")
			year, month, day := time.Now().Date()
			expected := fmt.Sprintf("datasets/testds/changes/year=%v/month=%v/day=%v", year, int(month), day)
			g.Assert(key[:47]).Eql(expected[:47])
		})
		g.It("Should not apply partitions to fullsync keys", func() {
			s3 := S3Storage{dataset: "testds"}
			s3.config = conf.StorageBackend{ParquetConfig: &conf.ParquetConfig{
				Partitioning: []string{"year", "month", "day"},
			}}
			var entities []*entity.Entity
			key := s3.createKey(entities, true)
			g.Assert(key[25:29] == "year").IsFalse("year not expected in path")
		})
	})
}
