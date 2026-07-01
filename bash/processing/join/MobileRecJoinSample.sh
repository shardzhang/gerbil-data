#!/bin/bash

# name: shard zhang
# date: 2026/7/1
# note: Join cleaned samples with item features and user behavior sequences

cd "$(dirname "$0")"

source ../../conf/env.sh

path=${MOBILE_REC_PATH}

"${SPARK_HOME}/bin/spark-submit" \
--master 'local[*]' \
--class processing.join.MobileRecJoinSample \
--conf spark.ui.port=8688 \
--conf spark.driver.maxResultSize=10g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -XX:+UseCodeCacheFlushing -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:./gc.log' \
--conf spark.executor.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -XX:+UseCodeCacheFlushing -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' \
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
