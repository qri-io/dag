GOFILES = $(shell find . -name '*.go' -not -path './vendor/*')
define GOPACKAGES 
github.com/briandowns/spinner \
github.com/datatogether/api/apiutil \
github.com/fatih/color \
github.com/ipfs/go-datastore \
github.com/olekukonko/tablewriter \
github.com/qri-io/bleve \
github.com/qri-io/dataset \
github.com/qri-io/doggos \
github.com/qri-io/dsdiff \
github.com/qri-io/varName \
github.com/qri-io/registry/regclient \
github.com/sergi/go-diff/diffmatchpatch \
github.com/sirupsen/logrus \
github.com/spf13/cobra \
github.com/spf13/cobra/doc \
github.com/theckman/go-flock \
github.com/ugorji/go/codec \
github.com/beme/abide \
github.com/ghodss/yaml \
github.com/qri-io/ioes \
github.com/pkg/errors
endef

define GX_DEP_PACKAGES 
github.com/qri-io/cafs \
github.com/qri-io/startf
endef

default: test

require-gopath:
ifndef GOPATH
	$(error $$GOPATH must be set. plz check: https://github.com/golang/go/wiki/SettingGOPATH)
endif

install-deps:
	go get -v -u $(GOPACKAGES)

install-gx:
	go get -v -u github.com/whyrusleeping/gx github.com/whyrusleeping/gx-go

install-gx-deps:
	gx install

install-gx-dep-packages:
	go get -v $(GX_DEP_PACKAGES)

workdir:
	mkdir -p workdir

lint:
	golint ./...

test:
	go test ./...

test-all-coverage:
	./.circleci/cover.test.sh

cli-docs:
	go run docs/docs.go --dir temp --filename cli_commands.md

update-changelog:
	conventional-changelog -p angular -i CHANGELOG.md -s

