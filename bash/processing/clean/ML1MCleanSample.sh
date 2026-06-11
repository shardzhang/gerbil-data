#!/bin/bash

# name: shard zhang
# date: 2026/5/28 17:54
# note: Clean raw ML-1M ratings (dedup, filter invalid), output CSV with day field

cd "$(dirname "$0")"

source ../../conf/env.sh

path=${ML_1M_PATH}

"${SPARK_HOME}/bin/spark-submit" \
--master 'local[*]' \
--class processing.clean.ML1MCleanSample \
--conf spark.ui.port=8688 \
--conf spark.driver.maxResultSize=10g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.extraJavaOptions='-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:./gc.log' \
--conf spark.executor.extraJavaOptions='-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' \
--conf spark.driver.cores=5 \
--conf spark.network.timeout=600s \
--conf spark.default.parallelism=2 \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
--driver-memory 4g \
--executor-memory 2g \
${JAR_PATH} \
${path}