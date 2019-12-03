ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
LD_FLAGS = -ldflags "-w -s"

GOOS 		= linux
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: busybee
busybee: dist_dir; $(info ======== compiled busybee)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)busybee $(LD_FLAGS) $(ROOT_DIR)/cmd/server/*.go

