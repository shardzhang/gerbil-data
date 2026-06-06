#!/bin/bash

# name: shard zhang
# date: 2026/6/6 14:42
# note:

cd $(dirname $0)

source ../env.sh

path=${ML_1M_PATH}


"${SPARK_HOME}/bin/spark-submit" \
--master 'local[*]' \
--class driver.ML1MDataDriver \
--conf spark.ui.port=8688 \
--queue root.dataming.prd \
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
${JAR_PATH} \
--feature_threshold 10 \
--target_threshold 1 \
--sample_ratio 1.0 \
--base_dir ${path}
