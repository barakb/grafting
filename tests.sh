#!/usr/bin/env bash

#go fmt ./... ; go tool vet -shadowstrict -v . && go test   ./...
go fmt ./... ; go tool vet -shadowstrict -v . && go test -v  ./...
#go fmt ./... ; go tool vet -shadowstrict -v . && go test  ./... -run TestTCPConnectorSendToClient
#go fmt ./... ; go tool vet -shadowstrict -v . && go test   ./... -run Test3TCPServersReplicateData
