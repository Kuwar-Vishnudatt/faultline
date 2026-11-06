.PHONY: build experiment test test-race test-all fmt vet clean

build:
	go build -o bin/chaos-runner ./cmd/chaos-runner
	@if [ ! -f bin/idemq-server ]; then \
		echo "building target: idemq-server (from github.com/Kuwar-Vishnudatt/idemq)"; \
		GOBIN=$$(pwd)/bin go install github.com/Kuwar-Vishnudatt/idemq/cmd/idemq-server@latest; \
	fi

experiment: build
	./bin/chaos-runner --idemq-bin=./bin/idemq-server --out=results/$$(date +%Y-%m-%dT%H-%M-%S)

test:
	go test ./pkg/... -count=1

test-race:
	go test ./pkg/... -race -count=1

test-all:
	go test ./... -count=1 -timeout 60s

fmt:
	gofmt -l .

vet:
	go vet ./...

clean:
	rm -rf bin/
