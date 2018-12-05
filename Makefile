GOFILES = $(shell find . -name '*.go' -not -path './vendor/*')
define GOPACKAGES 
github.com/ugorji/go/codec
endef

define GX_DEP_PACKAGES 
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

update-changelog:
	conventional-changelog -p angular -i CHANGELOG.md -s

list-deps:
	go list -f '{{.Deps}}' | tr "[" " " | tr "]" " " | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}'