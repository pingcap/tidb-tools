.PHONY: build importer dump_region binlogctl sync_diff_inspector ddl_checker test check deps

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif


CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)


LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.Version=1.0.0~rc2+git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"

CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3
PACKAGES := $$(go list ./... | grep -vE 'vendor')
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor')
VENDOR_TIDB := vendor/github.com/pingcap/tidb
PACKAGE_LIST  := go list ./...
PACKAGES  := $$($(PACKAGE_LIST))
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

build: prepare check importer binlogctl sync_diff_inspector ddl_checker finish

prepare:		
	cp go.mod1 go.mod
	cp go.sum1 go.sum

importer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/importer ./importer

dump_region:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dump_region ./dump_region

binlogctl:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/binlogctl ./tidb-binlog/binlogctl

sync_diff_inspector:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/sync_diff_inspector ./sync_diff_inspector

ddl_checker:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/ddl_checker ./ddl_checker

test:
	@export log_level=error; \
	$(GOTEST) -cover $(PACKAGES)

fmt:
	go fmt ./...
	@goimports -w $(FILES)

check:
	#go get github.com/golang/lint/golint
	@echo "vet"
	$(GO) vet -all $(PACKAGES) 2>&1 | tee /dev/stderr | $(FAIL_ON_STDOUT)
	#@echo "golint"
	#@ golint ./... 2>&1 | grep -vE '\.pb\.go' | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

finish:
	cp go.mod go.mod1
	cp go.sum go.sum1
