#!/bin/bash

# dataset
export ML_1M_PATH="/Users/dazhang/PycharmProject/data/ml-1m"
export ML_1M_OUTPUT_PATH="/Users/dazhang/PycharmProject/data/ml-1m-output"

# Spark
export SPARK_HOME=/Users/dazhang/Library/Caches/spark-3.4.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# Pin Java 8 for local Spark 3.4.x scripts.
if /usr/libexec/java_home -v 1.8 >/dev/null 2>&1; then
  export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
  export PATH=$JAVA_HOME/bin:$PATH
fi

# project
export SPARK_LOCAL_IP=127.0.0.1
export PROJECT_HOME=/Users/dazhang/PycharmProject/gerbil-data
export JAR_PATH=$PROJECT_HOME/target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar

# tools
# curl -L -O "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.0/protoc-3.6.0-osx-x86_64.zip"
export protoc="/usr/local/bin/protoc-3.6.0-osx-x86_64/bin/protoc"
