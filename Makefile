
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.Version=1.0.0~rc2+git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"

CURDIR   := $(shell pwd)
GO       := GO15VENDOREXPERIMENT="1" go
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3
PACKAGES := $$(go list ./... | grep -vE 'vendor')

.PHONY: build importer checker dump_region binlogctl sync_diff_inspector test check deps

build: importer checker check test

importer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/importer ./importer

checker:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/checker ./checker

dump_region:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dump_region ./dump_region

binlogctl:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/binlogctl ./tidb_binlog/binlogctl

sync_diff_inspector:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/sync_diff_inspector ./sync_diff_inspector

binlog_reader:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/binlog_reader ./binlog_reader

test:
	@export log_level=error; \
	$(GOTEST) -cover $(PACKAGES)

check:
	$(GO) get github.com/golang/lint/golint

	$(GO) tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	$(GO) tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -w -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	glide get -s -v --skip-test ${PKG}
else
	glide update -s -v -u --skip-test
endif
	@echo "removing test files"
	glide vc --use-lock-file --only-code --no-tests

