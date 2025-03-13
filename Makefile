# Makefile for Redis HLL Performance Test

# Variables
BINARY_NAME=redis_hll_perf_test
MAIN_FILE=main.go
GO=go
GOFLAGS=-v
BUILD_DIR=build

# Targets
.PHONY: all build clean test lint fmt vet run deps help

all: clean deps fmt vet test build ## Run the clean, deps, fmt, vet, test and build targets

build: ## Build the binary
	mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_FILE)

clean: ## Remove build artifacts and temporary files
	rm -f $(BUILD_DIR)/$(BINARY_NAME)
	$(GO) clean

lint: ## Run linter
	golint ./...

fmt: ## Run gofmt
	$(GO) fmt ./...

vet: ## Run go vet
	$(GO) vet ./...

run: build ## Build and run the binary
	./$(BUILD_DIR)/$(BINARY_NAME)

deps: ## Download and install dependencies
	$(GO) mod download
	$(GO) mod tidy

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Default target
.DEFAULT_GOAL := help
