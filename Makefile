default: vet test

deps:
	go get -t ./...

vet:
	go vet ./...

test:
	go test ./... -v 1

bench:
	go test ./... -bench=. -v 1
