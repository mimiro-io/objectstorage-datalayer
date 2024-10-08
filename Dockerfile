FROM golang:1.23   as builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go vet ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -o server cmd/storage/main.go

FROM alpine:latest
RUN apk update && apk upgrade --no-cache libcrypto3 libssl3
RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/server .
ADD .env .
ADD resources/default-config.json resources/

# Expose port 8080 to the outside world
EXPOSE 8080

CMD ["./server"]
