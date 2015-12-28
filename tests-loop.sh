#!/usr/bin/env bash
# set -x
##################################################
# Run all the tests in loop, find any rare issue
##################################################

#go fmt ./... ; go tool vet -shadowstrict -v . && go test -v  ./...
go fmt ./... ; go tool vet -shadowstrict -v . && go test  ./...
exit_code=$?
until [ $exit_code -ge "1" ]
do
#    go test -v  ./... -run Test3ServerReplicateData
#    go test -v  ./...
    go test  ./...
    exit_code=$?
done

