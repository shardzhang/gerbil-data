#!/bin/bash

# Evaluates featurized training data: label distribution + feature coverage
# Reads TFRecord or Parquet output from ML1MPipeline

cd "$(dirname "$0")"

source ../../conf/env.sh

day="20260610"
input_path=${ML_1M_PATH}
data_path=${input_path}/train_sample/${day}/train/tfrecord

"${SPARK_HOME}/bin/spark-submit" \
--master 'local[*]' \
--class pipeline.eval.OfflineEvaluator \
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
${JAR_PATH} \
--data_path ${data_path} \
--format tfrecord \
--label_col target \
--top_k 20
