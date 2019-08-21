# Force Go Modules
GO111MODULE = on

GOCC ?= go
GOFLAGS ?=

# make reproducable
GOFLAGS += -asmflags=all=-trimpath="$(GOPATH)" -gcflags=all=-trimpath="$(GOPATH)"

# If set, override the install location for plugins
IPFS_PATH ?= $(HOME)/.ipfs

# If set, override the IPFS version to build against. This _modifies_ the local
# go.mod/go.sum files and permanently sets this version.
IPFS_VERSION ?= $(lastword $(shell $(GOCC) list -m github.com/ipfs/go-ipfs))

.PHONY: install build

go.mod: FORCE
	./set-target.sh $(IPFS_VERSION)

FORCE:

dsync.so: plugin.go go.mod
	$(GOCC) build $(GOFLAGS) -buildmode=plugin -o "$@" "$<"
	chmod +x "$@"

build: dsync.so
	@echo "Built against" $(IPFS_VERSION)

install: build
	chmod +x dsync.so
	mkdir -p "$(IPFS_PATH)/plugins"
	cp dsync.so "$(IPFS_PATH)/plugins/dsync.so"

build-cli:
	$(GOCC) build $(GOFLAGS) ./cli/dsync.go