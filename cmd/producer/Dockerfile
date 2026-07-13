# Build context must be the repo root (internal/... packages are outside cmd/producer):
#   docker build -f cmd/producer/Dockerfile .

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 beaver
USER beaver
WORKDIR /app
COPY --from=build /out/producer /usr/local/bin/producer

# Mount the real config.yml to /app/config.yml (the binary's default --config path).
ENTRYPOINT ["producer"]
