APP_NAME := token-atlas
MAIN_PKG := .
DIST_DIR := dist
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -s -w

.PHONY: help build run clean build-linux darwin-amd64 darwin-arm64

help:
	@echo "可用命令:"
	@echo "  make build         - 构建当前平台二进制"
	@echo "  make run           - 直接运行项目"
	@echo "  make build-linux   - 构建 Linux x64 可执行文件"
	@echo "  make darwin-amd64  - 构建 macOS Intel 可执行文件"
	@echo "  make darwin-arm64  - 构建 macOS Apple Silicon 可执行文件"
	@echo "  make clean         - 清理 dist 目录"

build:
	@mkdir -p $(DIST_DIR)
	go build -ldflags '$(LDFLAGS)' -o $(DIST_DIR)/$(APP_NAME) $(MAIN_PKG)

run:
	go run $(MAIN_PKG)

build-linux:
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o $(DIST_DIR)/$(APP_NAME)-linux-amd64 $(MAIN_PKG)


darwin-amd64:
	@mkdir -p $(DIST_DIR)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o $(DIST_DIR)/$(APP_NAME)-darwin-amd64 $(MAIN_PKG)


darwin-arm64:
	@mkdir -p $(DIST_DIR)
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o $(DIST_DIR)/$(APP_NAME)-darwin-arm64 $(MAIN_PKG)

clean:
	rm -rf $(DIST_DIR)
