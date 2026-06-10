#!/bin/zsh

# name: shard zhang
# date: 2026/6/1 22:42
# note: Compile TensorFlow Example protobuf definitions to Java classes

cd $(dirname $0)

source ../conf/env.sh

cd ${PROJECT_HOME}

"$protoc" \
--proto_path=./proto \
--java_out=./src/main/java \
tensorflow/core/example/example.proto \
tensorflow/core/example/feature.proto
