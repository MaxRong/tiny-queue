GO ?= go

.PHONY: run test

run:
	$(GO) run ./cmd/tiny-queue

test:
	$(GO) test ./...
