#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
export SPARK_CONF_DIR="$HIVE12_CONF_DIR"
export HIVE_CONF_DIR="$HIVE12_CONF_DIR"

"$SPARK_HOME/bin/spark-submit" \
--master 'local[*]' \
--class sample.TestHive \
--conf spark.ui.port=8688 \
--conf spark.sql.parquet.compression.codec=uncompressed \
--conf spark.driver.maxResultSize=10g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.cores=5 \
--conf spark.network.timeout=600s \
--conf spark.default.parallelism=2 \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-memory 4g \
--executor-memory 2g \
"$JAR_PATH"
