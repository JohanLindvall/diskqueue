.DEFAULT_GOAL := help

.PHONY: help all check fmt-check vet test faults lint examples bench fuzz fmt fix tools update-tools

# Keep this in sync with .github/workflows/ci.yml.
GOLANGCI_LINT_VERSION := v2.14.0
TOOLS_DIR := $(CURDIR)/.bin
LINT_DIR := $(TOOLS_DIR)/golangci-lint-$(GOLANGCI_LINT_VERSION)
LINT_BIN := $(LINT_DIR)/golangci-lint
FUZZTIME ?= 30s

help:
	@echo "make check     Check formatting, vet, lint, race/fault tests, and examples"
	@echo "make test      Run tests with race detection and coverage"
	@echo "make faults    Run the full suite with fault injection and race detection"
	@echo "make examples  Run the basic and JSON replay examples"
	@echo "make fmt       Format Go source"
	@echo "make fix       Format Go source and tidy module dependencies"
	@echo "make lint      Run golangci-lint $(GOLANGCI_LINT_VERSION)"
	@echo "make bench     Run all benchmarks with allocation statistics"
	@echo "make fuzz      Fuzz the store (FUZZTIME=30s by default)"
	@echo "make tools     Install the pinned linter into .bin/"

all: check

# Verify without rewriting source or module files.
check: fmt-check vet lint test faults examples

fmt-check:
	@files=$$(gofmt -l .) || exit 1; \
	if [ -n "$$files" ]; then \
		echo "$$files"; \
		echo "Run 'make fmt' to format these files."; \
		exit 1; \
	fi

vet:
	go vet ./...
	go vet -tags diskqueue_faults ./...

# Unit tests, raced and with coverage. The race detector is not optional here:
# the queue hands a reused buffer to user code under a mutex, and the iterators
# release that mutex across yields.
test:
	go test -race -cover ./...

# The whole suite under the fault-injection build. Not filtered by test name: the
# tag only adds nil-hook branches, so everything still passes, and a -run regex
# would silently skip the next fault test whose name did not match it.
faults:
	go test -race -tags diskqueue_faults ./...

lint: $(LINT_BIN)
	"$(LINT_BIN)" run ./...

examples:
	go run ./examples/basic
	go run ./examples/replay

# Run every benchmark without rendering markdown — a quick smoke check.
bench:
	go test -run='^$$' -bench=. -benchmem ./...

fuzz:
	go test -run='^$$' -fuzz='^FuzzStore$$' -fuzztime=$(FUZZTIME) .

fmt:
	gofmt -w .

fix: fmt
	go mod tidy

tools: $(LINT_BIN)

# Retained as an alias; bump the version above to upgrade deliberately.
update-tools: tools

$(LINT_BIN):
	mkdir -p "$(LINT_DIR)"
	GOBIN="$(LINT_DIR)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
