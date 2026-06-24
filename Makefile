.PHONY: test lint fmt check tidy

# Run tests with the race detector and shuffled order.
test:
	go test -race -shuffle=on -count=1 ./...

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
