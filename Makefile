SHELL := /bin/bash

GO ?= go
BUF ?= buf

.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: docs
docs: ## Build the documentation site
	cd docs-site && pnpm install --frozen-lockfile=false && pnpm build

.PHONY: proto
proto: ## Regenerate protobuf bindings
	$(BUF) lint
	$(BUF) generate

.PHONY: tidy
tidy: ## Tidy Go modules
	$(GO) mod tidy

.PHONY: test
test: ## Run Go tests
	$(GO) test -race -count=1 ./...

.PHONY: docker
docker: ## Build the service image
	docker build --target server -t rate-limiter:local .

.PHONY: dev
dev: ## Start local Redis and quota service
	docker compose up -d --build

.PHONY: stop
stop: ## Stop local services
	docker compose down

.PHONY: clean
clean: ## Stop local services and remove volumes
	docker compose down -v --remove-orphans

