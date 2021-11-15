package conf

type StorageConfig struct {
	Id              string                    `json:"id"`
	StorageBackends []StorageBackend          `json:"storageBackends"`
	StorageMapping  map[string]StorageBackend `json:"-"` //ignore field  when marshaling/unmarshaling
}

type StorageBackend struct {
	Dataset          string            `json:"dataset"`
	StorageType      string            `json:"storageType"`
	StripProps       bool              `json:"stripProps"`
	StoreDeleted     bool              `json:"storeDeleted"`
	AthenaCompatible bool              `json:"athenaCompatible"`
	CsvConfig        *CsvConfig        `json:"csv"`
	FlatFileConfig   *FlatFileConfig   `json:"flatFile"`
	ParquetConfig    *ParquetConfig    `json:"parquet"`
	Properties       PropertiesMapping `json:"props"`
	DecodeConfig     *DecodeConfig     `json:"decode"`
}
type DecodeConfig struct {
	Namespaces       map[string]string `json:"namespaces"`
	PropertyPrefixes map[string]string `json:"propertyPrefixes"`
	Refs             []string          `json:"refs"`
	IdProperty       string            `json:"idProperty"`
	DefaultNamespace string            `json:"defaultNamespace"`
}
type PropertiesMapping struct {
	Bucket             *string `json:"bucket,omitempty"`
	Region             *string `json:"region,omitempty"`
	AuthType           *string `json:"authType,omitempty"`
	ResourceName       *string `json:"resourceName,omitempty"`
	CustomResourcePath *bool   `json:"customResourcePath,omitempty"`
	RootFolder         *string `json:"rootFolder,omitempty"`
	FilePrefix         *string `json:"filePrefix,omitempty"`
	Endpoint           string  `json:"endpoint"`
	Key                *string `json:"key,omitempty"`
	Secret             *string `json:"secret,omitempty"` //Note, need to be called secret to be injected in injectSecrets in manager.go
}

type CsvConfig struct {
	Header    bool     `json:"header"`
	Encoding  string   `json:"encoding"`
	Separator string   `json:"separator"`
	Order     []string `json:"order"`
}

type ParquetConfig struct {
	SchemaDefinition string   `json:"schema"`
	FlushThreshold   int64    `json:"flushThreshold"`
	Partitioning     []string `json:"partitioning"`
}

type FlatFileConfig struct {
	Fields map[string]FlatFileField `json:"fields"`
}

type FlatFileField struct {
	Substring  [][]int `json:"substring"`
	Type       string  `json:"type"`
	Decimals   int     `json:"decimals"`
	DateLayout string  `json:"dateLayout"`
}
