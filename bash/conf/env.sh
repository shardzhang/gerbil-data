#!/bin/bash

# name: shard zhang
# date: 2026/5/29 21:47
# note:  
  # spark3.4.0: curl -fL -O  "https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz"
  # protoc3.6.0: curl -L -O "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.0/protoc-3.6.0-osx-x86_64.zip"
  # protoc3.6.0: curl -L -O "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.0/protoc-3.6.0-linux-x86_64.zip"

fail_env() {
  echo "$1" >&2
  return 1 2>/dev/null || exit 1
}

PROTOC_VERSION="3.6.0"

if [[ $(uname -s) == "Darwin" ]]; then
  echo "Running on macOS"
  export BASE_HOME="~/PycharmProject"
  PROTOC_ARCHIVE="protoc-${PROTOC_VERSION}-osx-x86_64.zip"
  PROTOC_HOME="/usr/local/bin/protoc-${PROTOC_VERSION}-osx-x86_64"
  if /usr/libexec/java_home -v 1.8 >/dev/null 2>&1; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
  fi
else
  echo "Running on Linux"
  export BASE_HOME="/root/PycharmProjects"
  PROTOC_ARCHIVE="protoc-${PROTOC_VERSION}-linux-x86_64.zip"
  PROTOC_HOME="/opt/protoc-${PROTOC_VERSION}-linux-x86_64"
fi

export protoc="${PROTOC_HOME}/bin/protoc"
if [[ ! -x "${protoc}" ]]; then
  echo "protoc ${PROTOC_VERSION} not found, downloading..."
  command -v curl >/dev/null 2>&1 || fail_env "curl is required to download protoc"
  command -v unzip >/dev/null 2>&1 || fail_env "unzip is required to extract protoc"

  PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ARCHIVE}"
  PROTOC_ZIP="/tmp/${PROTOC_ARCHIVE}"

  mkdir -p "${PROTOC_HOME}" || fail_env "failed to create protoc directory: ${PROTOC_HOME}"
  curl -fL -o "${PROTOC_ZIP}" "${PROTOC_URL}" || fail_env "failed to download protoc from ${PROTOC_URL}"
  unzip -o "${PROTOC_ZIP}" -d "${PROTOC_HOME}" >/dev/null || fail_env "failed to unzip ${PROTOC_ZIP}"
fi

# Dataset
export ML_1M_PATH="${BASE_HOME}/data/ml-1m"
export ML_1M_OUTPUT_PATH="${BASE_HOME}/data/ml-1m-output"

# Project
export PROJECT_HOME="${BASE_HOME}/gerbil-data"

# Spark
export SPARK_HOME=$( [[ $(uname -s) == "Darwin" ]] && echo "~/Library/Caches/spark-3.4.0-bin-hadoop3" || echo "/opt/spark-3.4.0-bin-hadoop3" )
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1

# Java
export PATH=$JAVA_HOME/bin:$PATH
export JAR_PATH=$PROJECT_HOME/target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar
