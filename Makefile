.PHONY: all check test lint bench fix update-tools

GOPATH := $(shell go env GOPATH)
GOBIN := $(GOPATH)/bin
PATH := $(GOBIN):$(PATH)
export PATH

all: fix check

# Lint + the full test suite.
check: lint test

# Unit tests with coverage. The module is dependency-light (only x/sys).
test:
	go test -cover ./...

lint: $(GOBIN)/golangci-lint
	golangci-lint run ./...

# Run every benchmark without rendering markdown — a quick smoke check.
bench:
	go test -run='^$$' -bench=. -benchmem ./...

fix:
	gofmt -w .
	go mod tidy

update-tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

$(GOBIN)/golangci-lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
