package entity

import (
	"github.com/bcicen/jstream"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"io"
	"strconv"
)

func ParseStream(reader io.Reader, emitEntities func(entities []*uda.Entity, entityContext *uda.Context) error, batchSize int, storeDeleted bool) error {
	decoder := jstream.NewDecoder(reader, 1)
	isFirst := true
	read := 0
	entities := make([]*uda.Entity, 0)
	var entityContext *uda.Context

	for mv := range decoder.Stream() {
		if isFirst {
			ec := uda.AsContext(mv)
			entityContext = ec
			isFirst = false
		} else {
			entity := asEntity(mv, storeDeleted)
			if entity != nil {
				entities = append(entities, entity)
			}
			read++
			if read == batchSize {
				read = 0
				// do stuff with entities
				err := emitEntities(entities, entityContext)
				if err != nil {
					return err
				}
				entities = make([]*uda.Entity, 0)
			}
		}
	}

	if read > 0 {
		// do stuff with leftover entities
		err := emitEntities(entities, entityContext)
		if err != nil {
			return err
		}
	}

	return nil
}

func asEntity(value *jstream.MetaValue, storeDeleted bool) *uda.Entity {
	entity := uda.NewEntity()
	raw := value.Value.(map[string]interface{})
	entity.ID = raw["id"].(string)
	deleted, ok := raw["deleted"]

	if ok {
		if deleted.(bool) && !storeDeleted {
			return nil
		}

		entity.IsDeleted = deleted.(bool)
	}

	if recorded, ok := raw["recorded"]; ok {
		// fixme recorded becomes something else after made to string!!
		i := int64(recorded.(float64))
		entity.Recorded = strconv.FormatInt(i, 10)
	}

	if refs, ok := raw["refs"]; ok {
		entity.References = refs.(map[string]interface{})
	}

	if props, ok := raw["props"]; ok {
		entity.Properties = props.(map[string]interface{})
	}

	return entity
}
