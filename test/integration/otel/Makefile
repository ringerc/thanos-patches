.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Dependencies

.PHONY: deps
deps: ## Download Go module dependencies
	go mod download
	go mod tidy

.PHONY: deps-update
deps-update: ## Update Go module dependencies
	go get -u ./...
	go mod tidy

##@ Testing

.PHONY: test
test: ## Run integration tests
	go test -v -timeout 10m ./...

.PHONY: test-verbose
test-verbose: ## Run integration tests with verbose output
	go test -v -timeout 10m -count=1 ./...

.PHONY: test-short
test-short: ## Run integration tests in short mode (skips long tests)
	go test -v -timeout 5m -short ./...

.PHONY: test-race
test-race: ## Run integration tests with race detector
	go test -v -timeout 15m -race ./...

.PHONY: test-specific
test-specific: ## Run specific test (usage: make test-specific TEST=TestName)
	@if [ -z "$(TEST)" ]; then \
		echo "Error: TEST variable not set. Usage: make test-specific TEST=TestName"; \
		exit 1; \
	fi
	go test -v -timeout 10m -run $(TEST) ./...

##@ Code Quality

.PHONY: fmt
fmt: ## Format Go code
	go fmt ./...
	gofmt -s -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (requires golangci-lint installed)
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install from https://golangci-lint.run/usage/install/"; \
		exit 1; \
	}
	golangci-lint run

##@ Build

.PHONY: build
build: ## Build test binary
	go test -c -o otel.test ./...

.PHONY: build-thanos
build-thanos: ## Build Thanos binary (for manual testing)
	cd ../../.. && go build -o test/integration/otel/thanos ./cmd/thanos

##@ Cleanup

.PHONY: clean
clean: ## Clean build artifacts and test cache
	rm -f *.test
	rm -f thanos
	rm -rf vendor/
	go clean -testcache
	go clean -cache

.PHONY: clean-all
clean-all: clean ## Clean everything including module cache
	go clean -modcache

##@ Pre-requisites Check

.PHONY: check-prereqs
check-prereqs: ## Check if prerequisites are installed
	@echo "Checking prerequisites..."
	@command -v go >/dev/null 2>&1 || { echo "  ❌ Go not found"; exit 1; }
	@echo "  ✅ Go $(shell go version | awk '{print $$3}')"
	@command -v prometheus >/dev/null 2>&1 || { echo "  ❌ Prometheus not found in PATH"; exit 1; }
	@echo "  ✅ Prometheus $(shell prometheus --version 2>&1 | head -1 | awk '{print $$3}')"
	@echo "All prerequisites satisfied!"

##@ CI

.PHONY: ci
ci: deps check-prereqs test ## Run CI pipeline locally

.PHONY: ci-full
ci-full: deps check-prereqs fmt vet test ## Run full CI pipeline with checks

##@ Development

.PHONY: watch
watch: ## Watch for changes and re-run tests (requires entr)
	@command -v entr >/dev/null 2>&1 || { \
		echo "entr not found. Install with: brew install entr (macOS) or apt-get install entr (Linux)"; \
		exit 1; \
	}
	@echo "Watching for changes... (press Ctrl+C to stop)"
	@find . -name '*.go' | entr -c make test

.PHONY: debug
debug: ## Run test with delve debugger (requires dlv)
	@command -v dlv >/dev/null 2>&1 || { \
		echo "delve not found. Install with: go install github.com/go-delve/delve/cmd/dlv@latest"; \
		exit 1; \
	}
	dlv test -- -test.v -test.timeout 10m

##@ Documentation

.PHONY: docs
docs: ## Open README in browser
	@command -v mdv >/dev/null 2>&1 && mdv README.md || \
	command -v glow >/dev/null 2>&1 && glow README.md || \
	cat README.md
