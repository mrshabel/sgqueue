.PHONY: compile run test bench help

build:
	go build -o bin/

run:
	go run *.go

client:
	go run ./example/client/*.go

test:
	go test -v -race ./...

bench:
	go test ./... -bench=. -race

help:
	@echo "Available targets:"
	@echo "  build   - Compile the project and output the binary to the bin/ directory"
	@echo "  run     - Run the application"
	@echo "  client     - Run the example client"
	@echo "  test    - Run all tests with verbose output and race detection"
	@echo "  bench   - Run all benchmarks with race detection"
	@echo "  help    - Display this help message"
