
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.Version=1.0.0~rc2+git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"

CURDIR   := $(shell pwd)
GO       := GO15VENDOREXPERIMENT="1" go
GOTEST   := GOPATH=$(CURDIR)/_vendor:$(GOPATH) CGO_ENABLED=1 $(GO) test
PACKAGES := $$(go list ./... | grep -vE 'vendor')

.PHONY: build importer checker dump_region generate_binlog_position test check deps

build: importer checker check test

importer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/importer ./importer

checker:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/checker ./checker

dump_region:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dump_region ./dump_region

generate_binlog_position:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/generate_binlog_position ./generate_binlog_position

test:
	@export log_level=error; \
	$(GOTEST) -cover $(PACKAGES)

check:
	$(GO) get github.com/golang/lint/golint

	$(GO) tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	$(GO) tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'

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

