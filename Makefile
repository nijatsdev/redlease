.PHONY: test lint fmt tidy check

test: ## run tests with race detector and shuffle; set REDIS_ADDR to include real-Redis integration tests
	@REDIS_ADDR=$(REDIS_ADDR) go test -race -shuffle=on -count=1 ./...

lint: ## run golangci-lint
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found: https://golangci-lint.run/usage/install/"; exit 1; \
	fi

fmt: ## format code with golangci-lint fmt
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint fmt ./...; \
	else \
		gofmt -w .; \
	fi

tidy: ## verify go.mod/go.sum are tidy
	go mod tidy
	git diff --exit-code go.mod go.sum

check: ## Everything CI runs: tidy, lint, test.
	@$(MAKE) tidy
	@$(MAKE) lint
	@$(MAKE) test
