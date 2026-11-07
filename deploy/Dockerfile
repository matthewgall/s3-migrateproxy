# Build stage
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/gateway ./cmd/gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/migratectl ./cmd/migratectl

# Final stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates && \
    addgroup -g 65532 migrateproxy && \
    adduser -u 65532 -G migrateproxy -s /sbin/nologin -D migrateproxy
WORKDIR /app
COPY --from=builder /src/bin/gateway /app/gateway
COPY --from=builder /src/bin/worker /app/worker
COPY --from=builder /src/bin/migratectl /app/migratectl
COPY --from=builder /src/internal/migrations/*.sql /app/migrations/
ENV PATH=/app:$PATH
EXPOSE 8080 8081 9090
USER migrateproxy:migrateproxy
CMD ["/app/gateway"]
