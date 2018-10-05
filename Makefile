#
# Makefile for building all things related to this repo
#
NAME := rdsmysql
ORG := pinpt
PKG := $(ORG)/$(NAME)
SHELL := /bin/bash

.PHONY: all test clean dist dependencies linux osx

all: test

dist:
	@mkdir -p dist

clean:
	@rm -rf dist

dependencies:
	@dep ensure

linux: dist
	@GOOS=linux go build -o dist/$(NAME)-linux cmd/main.go

osx: dist
	@go build -race -o dist/$(NAME)-darwin cmd/main.go

test:
	@go test -race -v ./... | grep -v "no test"