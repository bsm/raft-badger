PKG=$(shell go list ./... | grep -v vendor)

default: vet test

deps:
	go get -t $(PKG)

vet:
	go vet $(PKG)

test:
	go test $(PKG) -v 1

bench:
	go test $(PKG) -bench=. -v 1
