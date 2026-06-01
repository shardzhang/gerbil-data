#!/bin/zsh

# name: shard zhang
# date: 2026/6/1 22:42
# note:  

cd $(dirname $0)

source ../env.sh

cd ../../

"$protoc" \
--proto_path=./proto \
--java_out=./src/main/java \
tensorflow/core/example/example.proto \
tensorflow/core/example/feature.proto
