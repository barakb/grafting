#!/usr/bin/env bash

go fmt ./... ; go tool vet -shadowstrict -v . && go test -v  ./...
exit_code = $?
until [ $exit_code -ge "1" ]
do
    go test -v  ./...
    exit_code=$?
done

