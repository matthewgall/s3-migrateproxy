.PHONY: all build test vet lint fmt clean docker-up docker-down migrate coverage coverage-report

BINDIR := bin
BINARY := $(BINDIR)/s3-migrateproxy
CMDS := gateway worker migratectl

# Use scripts/go.sh to avoid GOTOOLCHAIN wrapper mismatches (e.g. go1.25.1
# bootstrap running go1.26.0 module toolchain). Set GO=go to use the PATH go.
GO ?= ./scripts/go.sh

all: build

build:
	@mkdir -p $(BINDIR)
	$(GO) build -o $(BINDIR)/gateway ./cmd/gateway
	$(GO) build -o $(BINDIR)/worker ./cmd/worker
	$(GO) build -o $(BINDIR)/migratectl ./cmd/migratectl

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

lint: vet
	@echo "Run golangci-lint here if installed"

coverage-report:
	@output=$$($(GO) test -count=1 -coverprofile=coverage.out ./... 2>&1 | grep -vE '^(# |compile:)'); \
	echo "$$output"; \
	$(GO) tool cover -func=coverage.out | tail -5

coverage:
	@$(GO) test -cover -count=1 ./... 2>&1 | grep -E '(coverage:|^ok|^FAIL|^\?)'

fmt:
	$(GO) fmt ./...

clean:
	rm -rf $(BINDIR) gateway worker migratectl

docker-up:
	cd deploy && docker compose up -d

docker-down:
	cd deploy && docker compose down -v

run-gateway: build
	./$(BINDIR)/gateway

run-worker: build
	./$(BINDIR)/worker
