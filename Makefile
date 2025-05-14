.PHONY: compile run test

build:
	go build -o bin/

run:
	go run *.go

test:
	go test -v -race ./...