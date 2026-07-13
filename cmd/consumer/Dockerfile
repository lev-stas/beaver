# Build context must be the repo root (internal/... packages are outside cmd/consumer):
#   docker build -f cmd/consumer/Dockerfile .

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 beaver
USER beaver
WORKDIR /app
COPY --from=build /out/consumer /usr/local/bin/consumer

# Mount the real config.yml to /app/config.yml (the binary's default --config path).
ENTRYPOINT ["consumer"]
