package encoder_test

import (
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/objectstorage-datalayer/internal/conf"
	"github.com/mimiro-io/objectstorage-datalayer/internal/encoder"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
)

func encodeTwice(backend conf.StorageBackend, entities []*uda.Entity, entityContext *uda.Context) ([]byte, error) {
	reader, writer := io.Pipe()
	enc := encoder.NewEntityEncoder(backend, writer, zap.NewNop().Sugar())
	if backend.ResolveNamespace {
		entities = uda.ExpandUris(entities, entityContext)
	}
	go func() {
		_, err := enc.Write(entities)
		if err != nil {
			_ = enc.CloseWithError(err)
		}
		_, err = enc.Write(entities)
		if err != nil {
			_ = enc.CloseWithError(err)
			return
		}
		_ = enc.Close()
	}()
	result, err := ioutil.ReadAll(reader)
	return result, err

}

func encodeOnce(backend conf.StorageBackend, entities []*uda.Entity, entityContext *uda.Context) ([]byte, error) {
	reader, writer := io.Pipe()
	enc := encoder.NewEntityEncoder(backend, writer, zap.NewNop().Sugar())
	if backend.ResolveNamespace {
		entities = uda.ExpandUris(entities, entityContext)
	}
	go func() {
		_, err := enc.Write(entities)
		if err != nil {
			_ = enc.CloseWithError(err)
			return
		}
		err = enc.Close()
		if err != nil {
			println(err)
		}
	}()
	result, err := ioutil.ReadAll(reader)
	return result, err
}

func decodeOnce(backend conf.StorageBackend, fileContent []byte) (io.Reader, error) {
	reader, writer := io.Pipe()
	dec, err := encoder.NewEntityDecoder(backend, reader, "", zap.NewNop().Sugar(), false)
	if err != nil {
		return nil, err
	}
	go func() {
		defer writer.Close()
		_, err := writer.Write(fileContent)
		if err != nil {
			panic(err)
		}
	}()
	return dec, err
}
