# Common tasks. Run `make help` for a list.

COMPOSE := docker compose -f deploy/compose.yml --env-file .env
GOBIN   := $(shell go env GOPATH)/bin
ARCH    := $(shell go env GOARCH)

.PHONY: help up down logs ps test lint proto lambda tf-check reference backtest-demo

help: ## Show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Build and start everything (Kafka, LocalStack, Terraform, services)
	$(COMPOSE) up -d --build --wait
	@echo "Dashboard http://localhost:8080 · Grafana http://localhost:3000 · Kafka UI http://localhost:8081 · Jaeger http://localhost:16686"

down: ## Stop everything and delete its data
	$(COMPOSE) down -v --remove-orphans

logs: ## Follow service logs
	$(COMPOSE) logs -f --tail=50

ps: ## Show container status
	$(COMPOSE) ps

test: ## Run the Go tests with the race detector
	go test -race ./...

lint: ## Lint Go code and protobuf schemas
	golangci-lint run
	buf lint

proto: ## Regenerate Go code from proto/
	PATH="$(GOBIN):$$PATH" buf generate

lambda: ## Build the alert-handler Lambda zip into build/
	mkdir -p build
	CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) go build -trimpath -ldflags="-s -w" -tags lambda.norpc \
		-o build/bootstrap ./cmd/alert-handler
	cd build && rm -f alert-handler.zip && zip -q alert-handler.zip bootstrap

tf-check: lambda ## Format, validate and security-scan the Terraform
	terraform -chdir=infra fmt -check -recursive
	terraform -chdir=infra init -backend=false -input=false >/dev/null
	terraform -chdir=infra validate
	docker run --rm -v "$(CURDIR)/infra:/data" -w /data --entrypoint /bin/sh \
		ghcr.io/terraform-linters/tflint:v0.64.0 -c "tflint --init >/dev/null && tflint"
	checkov -d infra --quiet --compact

reference: ## Check the metrics engine against the pandas reference
	test -d .venv || python3 -m venv .venv
	.venv/bin/pip install -q -r scripts/requirements.txt
	.venv/bin/python scripts/reference_check.py

backtest-demo: ## Backtest the bundled synthetic fixture
	go run ./cmd/backtester -dir testdata/archive -run-id demo \
		-from 2026-10-04T13:50:00Z -to 2026-10-04T14:10:00Z
