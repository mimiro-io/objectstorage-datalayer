build:
	go build -o bin/server cmd/storage/main.go

run:
	go run cmd/storage/main.go

docker:
	docker build . -t objectstorage-datalayer

test:
	go vet ./...
	go test ./... -coverprofile=coverage.out -json > report.json

docker-test:
	docker build --target builder . -t objectstorage-layer-build
	docker run objectstorage-layer-build go test ./...


testlocal:
	go vet ./...
	go test ./... -v

integration:
	go test ./... -v -tags=integration

docker-test-integration:
	docker build --target builder . -t objectstorage-layer-build
	docker run -v /var/run/docker.sock:/var/run/docker.sock --net=host objectstorage-layer-build go test ./... -v -tags=integration
