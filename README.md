# Storage DataLayer

Datahub Data Layer for Storage, supported is S3 and Azure Blob
You can either post a full dataset or incremental changes

## API

The service is an implementation of the [Universal Data API](https://open.mimiro.io/specifications/uda/latest.html).
The main use is the [POST aspect](https://open.mimiro.io/specifications/uda/latest.html#post) of the API description, which
can help to store datasets as files in cloud storage services.
But the listing of datasets and retieval of existing storage objects as UDA entity batches are also available.

### POST entities

`POST /datasets/{datasetname}/entities`

Users of the service may send json serialized batches of [entities](https://open.mimiro.io/specifications/uda/latest.html#json-serialisation). Either
incremental changes, or complete datasets as connected batch-sequences (fullsync).

### GET entities

It is also possible to GET a compatible storage file in UDA format [UDA documentation](https://open.mimiro.io/specifications/uda/latest.html#dataset-entities)
However, objectstorage-datalayer does not offer sinceToken support. It is only possible to retrieve complete datasets

`GET /datasets/{dataset_name}/entities`

### GET datasets

It is also possible to list all available datasets, as specified in the [UDA documentation](https://open.mimiro.io/specifications/uda/latest.html#dataset-list)

`GET /datasets`

### Fullsync

The [UDA spec](https://open.mimiro.io/specifications/uda/latest.html#post) does detail the general http protocol,
but not every aspect of a fullsync process. These properties are specific for this objectstorage-datalayer:

S3:
* multiple fullsyncs to different datasets are possible
* If a new fullsync is started while another fullsync is active for a dataset, the old fullsync will be abandoned and the new sync takes over.
* incremental uploads to a dataset that has a fullsync in process are possible.
* If a fullsync is started and not appended to or finished, it will time out after 30 minutes. All data uploaded to this point will be discarded.

Azure:
* currently no fullsync support for azure

## Incremental Changes

Since this service writes to immutable object storages (you cannot modify azure/s3 objects in place, they have to be replaced), all
batches of incremental changes are just added as new objects to the target storage location. The aggregation of all changes to a final dataset state
is not handled by the layer service, and left to users of the storage data.

## Testing

Unit tests only: `make testlocal`

Integration test: `make integration`

    ***Development note***
    the integration test is activated by the build flag `integration`. This causes the Goland IDE to
    stop providing all kinds of help for the file.
    To make Goland work for this file, open settings. Find "Go"->"Build Tags & Vendoring".
    Type "integration" into the "Custom tags" text field and press OK.

## Run

`make run` or `make build && bin/server`

Ensure a config file exists in the location configured in the CONFIG_LOCATION variable

With Docker

```bash
make docker
docker run -d -p 4343:4343 -v $(pwd)/local.config.json:/root/config.json -e PROFILE=dev -e CONFIG_LOCATION=file://config.json datahub-storagedatalayer
```

## Env

Server will by default use the .env file, AND an extra file per environment,
for example .env-prod if PROFILE is set to "prod". This allows for pr environment
configuration of the environment in addition to the standard ones. All variables
declared in the .env file (but left empty) are available for reading from the ENV
in Docker.

The server will start with a bad or missing configuration file, it has an empty
default file under resources/ that it will load instead, and in general a call
to a misconfigured server should just return empty results or 404's.

Every 60s (or otherwise configured) the server will look for updated config's, and
load these if it detects changes. It should also then "fix" it's connection if changed.

It supports configuration locations that either start with "file://" or "http(s)://".

```bash
# the default server port, this will be overridden to 8080 in AWS
SERVER_PORT=4343

# how verbose the logger should be
LOG_LEVEL=INFO

# setting up token integration with Auth0
TOKEN_WELL_KNOWN=https://auth.yoursite.io/jwks/.well-known/jwks.json
TOKEN_AUDIENCE=https://api.yoursite.io
TOKEN_ISSUER=https://yoursite.auth0.com/

# statsd agent location, if left empty, statsd collection is turned off
DD_AGENT_HOST=

# if config is read from the file system, refer to the file here, for example "file://.config.json"
CONFIG_LOCATION=

# how often should the system look for changes in the configuration. This uses the cron system to
# schedule jobs at the given interval. If ommitted, the default is every 60s.
CONFIG_REFRESH_INTERVAL=@every 60s


```
By default the PROFILE is set to local, to easier be able to run on local machines. This also disables
security features, and must NOT be set to local in AWS. It should be PROFILE=dev or PROFILE=prod.

This also changes the loggers.

## Configuration

The service is configured with either a local json file or a remote variant of the same.
It is strongly recommended to leave the Password and User fields empty.

### Configuration file syntax

The general shape of a layer configuration file looks like this:

```json
{
    "id": "name-of-layer-service",
    "storageBackends": [
        {"dataset":  "dataset-name-1", ... },
        {"dataset":  "dataset-name-2", ... },
        ...
    ]
}
```

property name | description
-- | --
`id` | specify the name of the layer service
`storageBackends` | a list of 0 or more dataset configurations

#### dataset configuration

Depending on storage type and security requirements the configuration of each dataset is different. These are the available options:
```json
{
    "dataset": "string",
    "storageType": "string",
    "stripProps": false,
    "storeDeleted": false,
    "athenaCompatible": false,
    "csv": {
        "header": false,
        "encoding": "string",
        "separator": "string",
        "order": [
            "string",
            "string"
        ]
    },
    "parquet": {
        "schema": "string",
        "flushThreshold": 33554432
    },
    "props": {
        "bucket": "string",
        "region": "string",
        "authType": "string",
        "resourceName": "string",
        "rootFolder": "string",
        "filePrefix": "string",
        "endpoint": "string",
        "key": "string",
        "secret": "string"
    },
    "decode": {
        "namespaces": {
            "string": "string"
        },
        "propertyPrefixes": {
            "string": "string"
        },
        "idProperty": "string",
        "refs": [
            "string"
        ]
    }
}
```

property name | description
-- | --
`dataset` |  name of the dataset.
`storageType` | `S3` or `Azure`. Note that other types will not produce an error, uploaded data will be logged to server logs instead.
`stripProps` | only relevant for json encoded datasets. Csv and Parquet will implicitly set this to true. If true, the layer will transform each uploaded entity such that only properties are stored, and all property keys have their prefixes removed. If false, the complete entities are stored. Default false
`storeDeleted` | If true, entities with the deleted flag are included in the stored object. If false, they are filtered out by the layer. Default false. Should only ever be set to true for unstripped json encoded datasets.
`athenaCompatible` | reformat json batches as newline-delimited lists of json objects (ndjson). Default false
`csv` | if not empty, the layer will use a csv encoder to transform entities into csv files. If both parquet and csv config objects are present, parquet has precedence.
`csv.header` | if true, the csv encoder will prefix csv files with a column header line. default false.
`csv.encoding` | overide csv file character encoding. default UTF-8
`csv.separator` | set a csv delimiter. default is comma. should only be a single character.
`csv.order` | array of properties to include in given order in csv  file. each array element has to map to a stripped property name in the given entities.
`parquet` | if not empty, the layer will use a parquet encoder to transform entities into parquet files. If both parquet and csv config objects are present, parquet has precedence.
`parquet.schema` | a parquet schema string. each column name must match a stripped property name in the given entities.
`parquet.flushThreshold` | override number of bytes after which parquet streams are flushed to the storage target. Default is 1MB. The higher this value is set, the more optimized parquet read performance will be. But higher flushThreshold also means more memory buildup. for a typical layer installation 64MB is a recommended max.
`parquet.partitioning` | array of athena partition fields. currently only 'year', 'month', 'day' possible for time-of-writing partitioning
`props.bucket` |  name of storage bucket. should be created beforehand.
`props.region` | cloud provider region
`props.authType`| Can be "SAS" for azure, otherwise ignored.
`props.resourceName` | static filename for fullsyncs. If given, the layer will always write fullsyncs as single file to this object. If empty, the layer will generate new filenames each time.
`props.rootFolder` | only supported in azure. can be used to override object folder name. default is dataset name
`props.filePrefix` | only supported in azure. default is that there is no prefix.
`props.endpoint` | only needed in azure to declare storage service endpoint url. Can also be used to point s3 datasets to alternative s3 providers like ceph or localstack.
`props.key` |  access key id for the credentials provider of the dataset's storage backend
`props.secret` | name of environment variable that contains the auth secret string
`decode` | this configuration block can help to translate flat data structures in storage files to UDA entities
`decode.namespaces` | mapping of prefix strings to expanded namespace URIs. necessary to build @context element of valid UDA payloads
`decode.propertyPrefixes` | mapping of object keys to prefixes. each key in a flat data structure that is found in this map will be prefixed. A prefix value can have one of these three formats:<br/> * `prefixA` : the property key is prefixed with `prefixA`. example: `{"name": "bob"}` becomes `{"prefixA:name": "bob"}` <br/>* `prefixA:prefixB` : denotes different prefixes for key and value - separated by colon. example: `{"name": "bob"}` becomes `{"prefixA:name": "prefixB:bob"}` <br/>* `:prefixA` : only the value is prefixed with `prefixA`. example: `{"name": "bob"}` becomes `{"name": "prefixA:bob"}`. __caution__: to produce valid UDA documents all property keys must be prefixed. To support unprefixed keys you must declare a default namespace with prefix `_` in the document context.
`decode.idProperty` | UDA entities require an `id` field. This field declares which object key to fetch the id value from. value prefixes from correlating `propertyPrefix` settings are also applied to the id value.
`decode.refs` | list of object keys that should be placed into refs instead of props. prefixes from propertiesPrefixes are still applied.

#### Encoders.

* Default encoder is a `json` encoder. It produces valid json files.

* with `athenaCompatible`, files are encoded as ndjson.

* by providing a `csv` object in a dataset configuration, the csv encoder is enabled. Csv files require a column declaration in `csv.order`

* by providing a `parquet` object in a dataset configuration, files are encoded as parquet files. Parquet encoding requires a parquet schema to describe the columns and data types of the target files.

##### parquet schemas

The parquet encoder needs a textual schema definition. The [specification](https://pkg.go.dev/github.com/fraugster/parquet-go/parquetschema) of parquetschema is mostly supported.
A list of supported parquet data types can be found [here](https://FIX.ME#L82).
Since the configuration format in the layer is json, schemas must be provided as single string. So this parquet schema:

```
message test_schema {
    required int64 id;
    required binary key (STRING);
}
```

must be provided like this:
```
{
  "parquet": {
    "schema": "message test_schema { required int64 id; required binary key (STRING); }"
  }
}
```

#### Decoders

Currently there is only support for decoding ndjson (athena) formatted s3 files.

For s3 files that contain complete, valid UDA entities in ndjson format, set `stripProps=false`
In the dataset configuration. This will cause the decoder to simple concatenate all lines (entities) to a valid json array with leading @context entity.

for other flat json objects, you can set `stripProps=true` and provide a `decode` block in the dataset configuration. An example:

Given this ndjson file
```json
{ "postcode":"0158", "name": "Oslo", "municipality_code": 13 }
```

we can produce this UDA entities payload

```json
[ {
    "id":  "@context", "namespaces":  {
    "a" : "http://world/cities",
    "b" : "http://world/municipalities" }
},{
    "id": "a:0158",
    "deleted": false,
    "refs": { "b:municipality_code": "b:13" },
    "props": { "a:postcode":"a:0158", "a:name": "Oslo" }
} ]
```

with this `decode` configuration:
```json
{
    "decode": {
        "namespaces": {
            "a": "http://world/cities",
            "b": "http://world/municipalities"
        },
        "propertyPrefixes": {
            "name": "a",
            "postcode": "a:a",
            "municipality_code": "b:b"
        },
        "refs": [ "municipality_code" ],
        "idProperty": "postcode"
    }
}
```
### Example

A complete example can be found under "resources/test/test-config.json"

```json
{
    "id": "storage-local",
    "storageBackends": [
        {
            "dataset": "example.Owner",
            "storageType": "S3",
            "storeDeleted": false,
            "stripProps": false,
            "props": {
                "bucket": "datalayer-sink",
                "endpoint": "http://localhost:4566",
                "region": "us-east-1",
                "key": "AccessKeyId",
                "secret": "S3_STORAGE_SECRET_ACCESSKEYID"
            }

        },
        {
            "dataset": "example.Address",
            "storageType": "S3",
            "storeDeleted": false,
            "stripProps": true,
            "csv" : {
                "header" : true,
                "encoding" : "UTF-8",
                "separator" : ",",
                "order" : [
                    "City", "Zipcode", "Street", "HouseNumber"
                ]
            },
            "props": {
                "bucket": "datalayer-sink",
                "endpoint": "http://localhost:4566",
                "region": "us-east-1",
                "key": "AccessKeyId",
                "secret": "S3_STORAGE_SECRET_ACCESSKEYID"
            }
        },
        {
            "dataset": "example.Order",
            "storageType": "S3",
            "storeDeleted": false,
            "stripProps": false,
            "csv": {
                "header": true,
                "encoding": "UTF-8",
                "separator": ",",
                "order": [
                    "Id",
                    "Timestamp",
                    "OwnerId"
                ]
            },
            "props": {
                "bucket": "datalayer-sink",
                "resourceName": "latest-fullsync.csv",
                "endpoint": "http://localhost:4566",
                "region": "us-east-1",
                "key": "AccessKeyId",
                "secret": "S3_STORAGE_SECRET_ACCESSKEYID"
            }
        },
        {
            "dataset": "example.Stuff",
            "storageType": "Azure",
            "storeDeleted": false,
            "stripProps": true,
            "props": {
                "authType": "ClientSecret",
                "endpoint": "http://127.0.0.1:10000/myaccount1/stuff",
                "resourceName": "stuff",
                "rootFolder": "",
                "key": "myaccount1",
                "secret": ""
            }
        }
    ]
}


```