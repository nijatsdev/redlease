.PHONY: test test-integration lint fmt check tidy

# Run tests with the race detector and shuffled order. The real-Redis
# integration tests are skipped unless REDIS_ADDR is set.
test:
	go test -race -shuffle=on -count=1 ./...

# Run the full suite including the real-Redis integration tests; expects a
# Redis server on localhost:6379 (or override REDIS_ADDR).
test-integration:
	REDIS_ADDR=$${REDIS_ADDR:-localhost:6379} go test -race -shuffle=on -count=1 ./...

# Run the linters (config in .golangci.yml).
lint:
	golangci-lint run ./...

# Format the code using the formatters configured in .golangci.yml.
fmt:
	golangci-lint fmt

# Verify go.mod/go.sum are tidy.
tidy:
	go mod tidy
	git diff --exit-code go.mod go.sum

# Everything CI runs: tidy, lint, test.
check: tidy lint test
