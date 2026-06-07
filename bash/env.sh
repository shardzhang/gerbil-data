#!/bin/bash

# name: shard zhang
# date: 2026/5/29 21:47
# note:  
# spark3.4.0: curl -fL -O  "https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz"
# protoc3.6.0: curl -L -O "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.0/protoc-3.6.0-osx-x86_64.zip"


if [[ $(uname -s) == "Darwin" ]]; then
  echo "Running on macOS"
  export BASE_HOME="/Users/dazhang/PycharmProject"
  export protoc="/usr/local/bin/protoc-3.6.0-osx-x86_64/bin/protoc"
  if /usr/libexec/java_home -v 1.8 >/dev/null 2>&1; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
  fi
else
  echo "Running on Linux"
  export BASE_HOME="/root/PycharmProjects"
fi

# Dataset
export ML_1M_PATH="${BASE_HOME}/data/ml-1m"
export ML_1M_OUTPUT_PATH="${BASE_HOME}/data/ml-1m-output"

# Project
export PROJECT_HOME="${BASE_HOME}/gerbil-data"

# Spark
export SPARK_HOME=$( [[ $(uname -s) == "Darwin" ]] && echo "/Users/dazhang/Library/Caches/spark-3.4.0-bin-hadoop3" || echo "/opt/spark-3.4.0-bin-hadoop3" )
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1

# Java
export PATH=$JAVA_HOME/bin:$PATH
export JAR_PATH=$PROJECT_HOME/target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar
