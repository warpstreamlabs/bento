.PHONY: all serverless deps docker docker-cgo clean docs protos test test-race test-integration fmt lint install deploy-docs playground
TAGS ?=

GOMAXPROCS         ?= 1
INSTALL_DIR        ?= $(GOPATH)/bin
WEBSITE_DIR        ?= ./website
DEST_DIR           ?= ./target
PATHINSTBIN        = $(DEST_DIR)/bin
PATHINSTTOOLS      = $(DEST_DIR)/tools
PATHINSTSERVERLESS = $(DEST_DIR)/serverless
PATHINSTDOCKER     = $(DEST_DIR)/docker
DOCKER_IMAGE       ?= ghcr.io/warpstreamlabs/bento

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
VER_RC    := $(shell echo $(VER_PATCH) | cut -f2 -d-)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X main.Version=$(VERSION) -X main.DateBuilt=$(DATE)

LD_FLAGS   ?= -w -s
GO_FLAGS   ?=
DOCS_FLAGS ?=

APPS = bento
all: $(APPS)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: $(APPS) ## Install binaries to $(INSTALL_DIR)
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/bento
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

deps: ## Go mod tidy
	@go mod tidy

SOURCE_FILES = $(shell find internal public cmd -type f)
TEMPLATE_FILES = $(shell find internal/impl -type f -name "template_*.yaml")

$(PATHINSTBIN)/%: $(SOURCE_FILES)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

TOOLS = bento_docs_gen
tools: $(TOOLS)

$(PATHINSTTOOLS)/%: $(SOURCE_FILES)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/tools/$*

$(TOOLS): %: $(PATHINSTTOOLS)/%

SERVERLESS = bento-lambda
serverless: $(SERVERLESS)

$(PATHINSTSERVERLESS)/%: $(SOURCE_FILES)
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/serverless/$*
	@zip -m -j $@.zip $@

$(SERVERLESS): %: $(PATHINSTSERVERLESS)/%

docker-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR).$(VER_MINOR),$(VER_MAJOR)" > .tags

docker-rc-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR)-$(VER_RC)" > .tags

docker-cgo-tags:
	@echo "latest-cgo,$(VER_CUT)-cgo,$(VER_MAJOR).$(VER_MINOR)-cgo,$(VER_MAJOR)-cgo" > .tags

docker:
	@docker build -f ./resources/docker/Dockerfile . -t $(DOCKER_IMAGE):$(VER_CUT)
	@docker tag $(DOCKER_IMAGE):$(VER_CUT) $(DOCKER_IMAGE):latest

docker-cgo:
	@docker build -f ./resources/docker/Dockerfile.cgo . -t $(DOCKER_IMAGE):$(VER_CUT)-cgo
	@docker tag $(DOCKER_IMAGE):$(VER_CUT)-cgo $(DOCKER_IMAGE):latest-cgo

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}
	@go list -f {{.Dir}} ./... | xargs -I{} goimports -w -local github.com/warpstreamlabs/bento {}
	@go mod tidy

lint: ## Run Go linter
	@go vet $(GO_FLAGS) ./...
	@golangci-lint -j $(GOMAXPROCS) run --fix -c .golangci.yml ./...

test: $(APPS) ## Run tests
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m ./...
	@$(PATHINSTBIN)/bento template lint $(TEMPLATE_FILES)
	@$(PATHINSTBIN)/bento test ./config/test/...

test-race: $(APPS) ## Run tests with -race
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m -race ./...

test-integration:
	$(warning WARNING! Running the integration tests in their entirety consumes a huge amount of computing resources and is likely to time out on most machines. It's recommended that you instead run the integration suite for connectors you are working selectively with `go test -run 'TestIntegration/kafka' ./...` and so on.)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -run "^Test.*Integration.*$$" -timeout 5m ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(DEST_DIR)/tools
	rm -rf $(DEST_DIR)/serverless
	rm -rf $(PATHINSTDOCKER)
	rm -rf $(WEBSITE_DIR)/static/playground

docs: $(APPS) $(TOOLS)
	@$(PATHINSTTOOLS)/bento_docs_gen $(DOCS_FLAGS)
	@$(PATHINSTBIN)/bento lint --deprecated "./config/examples/*.yaml" \
		"$(WEBSITE_DIR)/cookbooks/**/*.md" \
		"$(WEBSITE_DIR)/docs/**/*.md"
	@$(PATHINSTBIN)/bento template lint "./config/template_examples/*.yaml"

PROTO_FILES = $(shell find resources/protos -name '*.proto')

protos:
	@echo "Generating protocol buffer files..."
	protoc \
		--proto_path=resources/protos \
		--go_out=. \
		--go_opt=module=github.com/warpstreamlabs/bento \
		$(PROTO_FILES)
	@echo "Proto files generated to internal/message/ and internal/docs/"

# HACK:(gregfurman): Change misc/wasm/wasm_exec.js => lib/wasm/wasm_exec.js when using Go 1.24
playground:
	@cp -r "internal/cli/blobl/playground" "$(WEBSITE_DIR)/static/playground"
	@sed -i.bak '/<!-- BEGIN SERVER MODE -->/,/<!-- END SERVER MODE -->/d' "$(WEBSITE_DIR)/static/playground/index.html"
	@if [ -f "$$(go env GOROOT)/lib/wasm/wasm_exec.js" ]; then \
		cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" "$(WEBSITE_DIR)/static/playground/js/wasm_exec.js"; \
	else \
		cp "$$(go env GOROOT)/misc/wasm/wasm_exec.js" "$(WEBSITE_DIR)/static/playground/js/wasm_exec.js"; \
	fi
	@GOOS=js GOARCH=wasm go build -o "$(WEBSITE_DIR)/static/playground/playground.wasm" "cmd/tools/playground/main.go"
