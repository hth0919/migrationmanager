\#!/bin/bash
docker_id="hth0919"
image_name="migration-manager"

export GO111MODULE=on
go mod vendor
kubectl config view >> `pwd`/build/bin/config

go build -o `pwd`/build/_output/bin/$image_name -mod=vendor `pwd`/cmd/manager/main.go && \
docker build -t $docker_id/$image_name:v0.0.1 build && \
docker push $docker_id/$image_name:v0.0.1
