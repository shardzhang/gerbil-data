
# note: Mini integration test: creates a tiny ML-1M dataset, runs the full pipeline, validates output
python3 - <<'PY'
import shutil
from pathlib import Path

base = Path('./tmp/ml-1m-mini')
output = Path('./tmp/ml-1m-mini-output')
shutil.rmtree(base, ignore_errors=True)
shutil.rmtree(output, ignore_errors=True)

(base / 'item_feature').mkdir(parents=True, exist_ok=True)
(base / 'join_sample').mkdir(parents=True, exist_ok=True)
(base / 'item_feature' / 'part-00000.csv').write_text(
    "324\tToy Story (1995)\tAnimation|Children's|Comedy\t3\t100\t4.5\t1\n",
    encoding='utf-8'
)
(base / 'join_sample' / 'part-00000.csv').write_text(
    "1\t324\t978300760\t5\t20000901\t{\"gender\":\"F\",\"age\":\"1\",\"occupation\":\"10\",\"zip_code\":\"48067\"}\t{\"movie_title\":\"Toy Story (1995)\",\"movie_genres\":\"Animation|Children's|Comedy\",\"movie_rate_count\":100,\"movie_avg_rate\":4.5,\"movie_hot_rank\":1}\t{\"user_movie_rate\":\"1:5:978300760\"}\n",
    encoding='utf-8'
)
print('created', base)
print('reset', output)
PY

source "../conf/env.sh" && "${SPARK_HOME}/bin/spark-submit" \
--master "local[1]" \
--class pipeline.ML1MPipeline \
--conf spark.ui.enabled=false \
--conf spark.driver.maxResultSize=1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.cores=1 \
--conf spark.default.parallelism=1 \
--conf spark.sql.shuffle.partitions=1 \
--driver-memory 2g \
--executor-memory 2g \
--conf spark.driver.extraJavaOptions='-XX:ReservedCodeCacheSize=256m' \
--conf spark.executor.extraJavaOptions='-XX:ReservedCodeCacheSize=256m' "${JAR_PATH}" \
--feature_threshold 1 \
--target_threshold 1 \
--sample_ratio 1.0 \
--input_dir ./tmp/ml-1m-mini \
--output_dir ./tmp/ml-1m-mini-output \
--yesterday "20260601" \
--parts 1 \
--output_format tfrecord
