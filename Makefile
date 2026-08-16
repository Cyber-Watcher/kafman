BINARY      := kafman
MODULE      := github.com/Cyber-Watcher/kafman
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE        ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X $(MODULE)/internal/version.Version=$(VERSION) \
	-X $(MODULE)/internal/version.Commit=$(COMMIT) \
	-X $(MODULE)/internal/version.Date=$(DATE)

COMPOSE := docker compose -f docker/compose.yml

# The platforms a release is built for. Everything is CGO-free, so all of them
# cross-compile from one machine — no per-OS runner needed.
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.PHONY: help build dist run run-dev test test-integration test-race lint vuln release-check fmt vet tidy clean \
        stack-prepare stack-up stack-init stack-live stack-live-down stack-down stack-logs \
        stack-status stack-check

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build a static binary
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) .

dist: ## Cross-compile release archives for every platform into dist/
	@rm -rf dist && mkdir -p dist
	@for platform in $(PLATFORMS); do \
		os=$${platform%/*}; arch=$${platform#*/}; \
		name=$(BINARY)_$(VERSION)_$${os}_$${arch}; \
		bin=$(BINARY); \
		if [ "$$os" = windows ]; then bin=$(BINARY).exe; fi; \
		mkdir -p dist/$$name; \
		printf '  %-24s' "$$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch \
			go build -trimpath -ldflags "$(LDFLAGS)" -o dist/$$name/$$bin . || exit 1; \
		cp README.md LICENSE dist/$$name/ 2>/dev/null || true; \
		if [ "$$os" = windows ]; then \
			(cd dist && zip -qr $$name.zip $$name); \
		else \
			tar -czf dist/$$name.tar.gz -C dist $$name; \
		fi; \
		rm -rf dist/$$name; \
		echo "ok"; \
	done
	@cd dist && sha256sum * > checksums.txt
	@echo; ls -la dist/

run: build ## Build and run
	./$(BINARY)

run-dev: ## Run with the dev build tag (artificial I/O latency, visible spinners)
	go run -tags dev . $(ARGS)

test: ## Run unit tests
	go test ./...

test-integration: ## Run integration tests against the local stack (needs make stack-up)
	go test -tags integration -count=1 ./...

test-race: ## Run unit tests with the race detector (needs a C compiler)
	@command -v gcc >/dev/null 2>&1 || command -v clang >/dev/null 2>&1 || { \
		echo "the race detector needs cgo and a C compiler; install gcc, e.g.: sudo dnf install gcc"; \
		exit 1; }
	CGO_ENABLED=1 go test -race ./...

vet: ## Run go vet
	go vet ./...

GOBIN := $(shell go env GOPATH)/bin

lint: ## Run golangci-lint (installs it on first use)
	@command -v golangci-lint >/dev/null 2>&1 || test -x $(GOBIN)/golangci-lint || { \
		echo "golangci-lint not found, installing..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; }
	@LINT=$$(command -v golangci-lint || echo $(GOBIN)/golangci-lint); $$LINT run

vuln: ## Scan the dependencies for known vulnerabilities
	@command -v govulncheck >/dev/null 2>&1 || test -x $(GOBIN)/govulncheck || { \
		echo "govulncheck not found, installing..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; }
	@VULN=$$(command -v govulncheck || echo $(GOBIN)/govulncheck); $$VULN ./...

release-check: vet lint vuln test ## Everything that has to pass before a release
	@echo "vet, lint, govulncheck and the unit tests are clean"

fmt: ## Format the code
	gofmt -s -w .

tidy: ## Tidy go.mod
	go mod tidy

clean: ## Remove build artefacts
	rm -f $(BINARY)
	rm -rf dist
	go clean

stack-prepare: ## Generate certificates and download the JMX agent
	./docker/gen-certs.sh
	./docker/fetch-jmx-agent.sh

stack-up: stack-prepare ## Start the test Kafka cluster (3 brokers, KRaft)
	$(COMPOSE) up -d
	@echo "waiting for the brokers to report healthy..."
	@until [ "$$($(COMPOSE) ps --format '{{.Status}}' | grep -c healthy)" = "3" ]; do sleep 3; done
	@echo "cluster is up:"
	@echo "  PLAINTEXT       localhost:9092,9192,9292"
	@echo "  SASL_PLAINTEXT  localhost:9093,9193,9293"
	@echo "  SASL_SSL        localhost:9094,9194,9294"
	@echo "  JMX metrics     localhost:9404,9405,9406"
	@echo "  node_exporter   localhost:9100,9101,9102"

stack-init: ## Populate the test cluster with topics, users, ACLs and lagging groups
	./docker/init.sh

stack-live: ## Start the live producer and consumer groups (real members, growing lag)
	$(COMPOSE) --profile live up -d
	@echo "live groups running: live-orders-fast (3 members) and live-orders-slow (1 member)"

# The services are named one by one on purpose: a --profile flag adds profiled
# services to the selection, it does not restrict the command to them, so
# `--profile live stop` would stop the brokers too.
LIVE_SERVICES := live-producer live-fast-1 live-fast-2 live-fast-3 live-slow-1

stack-live-down: ## Stop the live load, leaving the cluster and its data alone
	$(COMPOSE) --profile live rm -sf $(LIVE_SERVICES)

stack-status: ## Show the test cluster status
	$(COMPOSE) ps

stack-check: build ## Test kafman's connection to every profile of the test stack
	./$(BINARY) --config docker/kafman.dev.yaml --check-all --log-level error

stack-down: ## Stop the test cluster and remove its volumes
	$(COMPOSE) down -v

stack-logs: ## Follow the test cluster logs
	$(COMPOSE) logs -f
