#!/usr/bin/env bash

##################################################
# Run all the tests in loop, find any rare issue
##################################################

go fmt ./... ; go tool vet -shadowstrict -v . && go test -v  ./...
exit_code = $?
until [ $exit_code -ge "1" ]
do
#    go test -v  ./... -run TestServerSaveLast5RequestFromEachClient
    go test -v  ./...
    exit_code=$?
done

