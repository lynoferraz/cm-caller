.PHONY: all
all: | build

.PHONY: build
build:
	go build ./...

.PHONY: run
run:
	go run github.com/lynoferraz/cm-caller
