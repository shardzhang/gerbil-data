#!/bin/bash

# name: shard zhang
# date: 2026/5/29 12:09
# note:  

cd $(dirname $0)

path="/Users/zhangda/IdeaProjects/RecommendSample/data/ali_display_ad_ctr_sample"
#yesterday="2017-05-06"
yesterday=$1
part=$2

spark-submit \
--master local[*] \
--class sample.JoinSampleDayV01 \
--conf spark.ui.port=8688 \
--queue root.dataming.prd \
--conf spark.driver.maxResultSize=4g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.extraJavaOptions='-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps' \
--conf spark.driver.cores=5 \
--conf spark.network.timeout=600s \
--conf spark.default.parallelism=10 \
--conf spark.sql.shuffle.partitions=10 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4  \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
--driver-memory 6g \
--executor-memory 4g \
--jars /Users/zhangda/IdeaProjects/RecommendSample/jar/spark-tensorflow-connector_2.11-1.15.0.jar \
/Users/zhangda/IdeaProjects/RecommendSample/target/recommend-sample-1.0-SNAPSHOT-jar-with-dependencies.jar \
${path} \
${yesterday} \
${part}
