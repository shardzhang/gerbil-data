#!/bin/bash

# name: shard zhang
# date: 2026/6/6 14:42
# note: Encode features (multi-class mode) and generate TFRecord + Parquet training samples for ML models

cd "$(dirname "$0")"

source ../conf/env.sh

day="20260623"
input_path=${ML_1M_PATH}
output_path=${input_path}/train_sample/rating

log_path="${output_path}"
mkdir -p "${log_path}"
timestamp=$(date +"%Y%m%d_%H%M%S")
log_file="${log_path}/${timestamp}.log"

(
"${SPARK_HOME}/bin/spark-submit" \
--master 'local[*]' \
--class pipeline.ML1MPipeline \
--conf spark.ui.port=8688 \
--conf spark.driver.maxResultSize=10g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.cores=5 \
--conf spark.network.timeout=600s \
--conf spark.default.parallelism=64 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
--driver-memory 8g \
--executor-memory 8g \
--conf spark.hadoop.fs.defaultFS=file:/// \
--conf spark.driver.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -XX:+UseCodeCacheFlushing -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:./gc.log' \
--conf spark.executor.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -XX:+UseCodeCacheFlushing -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' \
${JAR_PATH} \
--feature_threshold 5 \
--target_threshold 0 \
--input_dir ${input_path} \
--output_dir ${output_path} \
--yesterday ${day} \
--parts 10 \
--output_format tfrecord \
--train_ratio 0.8 \
--val_ratio 0.1 \
--feature_config ${PROJECT_HOME}/src/main/resources/ml1m/binary_features.yaml \
--target_mode rating
) 2>&1 | tee "${log_file}"