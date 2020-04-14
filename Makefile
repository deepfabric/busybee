VERSION    = $(version)
ifeq ("$(VERSION)","")
	VERSION := "dev"
endif

ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
LD_GIT_COMMIT      = -X 'github.com/deepfabric/busybee/pkg/util.GitCommit=`git rev-parse --short HEAD`'
LD_BUILD_TIME      = -X 'github.com/deepfabric/busybee/pkg/util.BuildTime=`date +%FT%T%z`'
LD_GO_VERSION      = -X 'github.com/deepfabric/busybee/pkg/util.GoVersion=`go version`'
LD_BIN_VERSION     = -X 'github.com/deepfabric/busybee/pkg/util.Version=$(VERSION)'
LD_FLAGS = -ldflags "$(LD_GIT_COMMIT) $(LD_BUILD_TIME) $(LD_GO_VERSION) $(LD_BIN_VERSION) -w -s"

GOOS 		= linux
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)busybee

.PHONY: grafana
grafana: dist_dir; $(info ======== compiled busybee)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)grafana $(LD_FLAGS) $(ROOT_DIR)cmd/grafana/*.go

.PHONY: busybee
busybee: dist_dir; $(info ======== compiled busybee)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)busybee $(LD_FLAGS) $(ROOT_DIR)cmd/server/*.go

.PHONY: checker
checker: dist_dir; $(info ======== compiled checker)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)checker $(LD_FLAGS) $(ROOT_DIR)cmd/checker/*.go

.PHONY: docker
docker: ; $(info ======== compiled busybee docker)
	docker build -t deepfabric/busybee:$(VERSION) -f Dockerfile .
	docker tag deepfabric/busybee:$(VERSION) deepfabric/busybee

.PHONY: test
test: ; $(info ======== test busybee)
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/core
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/storage
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/queue
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/notify
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/expr
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/api
	env GO111MODULE=off go test -count=1 github.com/deepfabric/busybee/pkg/util
.DEFAULT_GOAL := busybee