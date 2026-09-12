.PHONY: test lint build vuln check

test:
	go test ./...

lint:
	go vet -tags=e2e ./...
	go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 -tags=e2e ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 -tags=e2e ./...

check:
	test "$$(go env GOVERSION)" = go1.26.8
	$(MAKE) test
	$(MAKE) lint
	$(MAKE) build
	test -z "$$(gofmt -l .)"
	go mod tidy && git diff --exit-code -- go.mod go.sum
