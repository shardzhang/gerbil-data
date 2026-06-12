"""
ML-1M Feature Engineering Pipeline — Airflow DAG.

DAG structure:
  clean_sample ─┬─ movie_stats ──┐
                └─ user_behavior ┘
                      │
                      ▼
                  join_sample ─┬─ neg_sampler (optional)
                               └─ pipeline ──► evaluate

Each task submits a Spark job via spark-submit using the shared uber-jar.

Configuration (Airflow Variables or env vars):
  ml1m_spark_home       Spark installation directory
  ml1m_jar_path         Path to gerbil-data uber-jar
  ml1m_data_path        ML-1M dataset root (input/output)
  ml1m_project_home     Project root (for feature_config YAML)
  ml1m_feature_config   Path to features.yaml
  ml1m_base_home        Parent of project_home and data_path

Deployment:
  cp dag/ml1m_pipeline_dag.py $AIRFLOW_HOME/dags/
  # Set Airflow variables or ensure env vars are available to the scheduler
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from pathlib import Path
import os

# ── Configuration ────────────────────────────────────────────────────────────
BASE_HOME = Variable.get(
    "ml1m_base_home",
    default_var=os.environ.get("BASE_HOME", str(Path.home() / "PycharmProject")),
)
PROJECT_HOME = Variable.get(
    "ml1m_project_home",
    default_var=os.environ.get("PROJECT_HOME", f"{BASE_HOME}/gerbil-data"),
)
DATA_PATH = Variable.get(
    "ml1m_data_path",
    default_var=os.environ.get("ML_1M_PATH", f"{BASE_HOME}/data/ml-1m"),
)
SPARK_HOME = Variable.get(
    "ml1m_spark_home",
    default_var=os.environ.get("SPARK_HOME", "/opt/spark-3.4.0-bin-hadoop3"),
)
JAR_PATH = Variable.get(
    "ml1m_jar_path",
    default_var=f"{PROJECT_HOME}/target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar",
)
FEATURE_CONFIG = Variable.get(
    "ml1m_feature_config",
    default_var=f"{PROJECT_HOME}/src/main/resources/ml1m/features.yaml",
)

SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"
DS_NODASH = "{{ ds_nodash }}"

# ── env.sh loader (same pattern as run_pipeline.py) ────────────────────────
ENV_SH = Path(PROJECT_HOME) / "bash" / "conf" / "env.sh"


def shell_wrap(cmd: str) -> str:
    """Wrap a command so it runs in bash after sourcing env.sh."""
    if ENV_SH.exists():
        return f"source {ENV_SH} && {cmd}"
    return cmd


# ── Spark template builders ─────────────────────────────────────────────────
def common_spark_args(memory: str = "4g") -> list[str]:
    return [
        "--master", "local[*]",
        "--conf", "spark.ui.port=8688",
        "--conf", "spark.driver.maxResultSize=10g",
        "--conf", "spark.dynamicAllocation.enabled=false",
        "--conf", "spark.driver.cores=5",
        "--conf", "spark.network.timeout=600s",
        "--conf", "spark.kryoserializer.buffer.max=2000m",
        "--conf", 'spark.serializer="org.apache.spark.serializer.KryoSerializer"',
        "--driver-memory", memory,
        "--executor-memory", "2g",
        "--conf", "spark.default.parallelism=2",
        "--conf", "spark.sql.shuffle.partitions=2",
        "--conf", "spark.driver.extraJavaOptions='-verbose:gc -XX:+PrintGCDetails'",
        "--conf", "spark.executor.extraJavaOptions='-verbose:gc -XX:+PrintGCDetails'",
    ]


def pipeline_spark_args(memory: str = "8g") -> list[str]:
    return [
        "--master", "local[*]",
        "--conf", "spark.ui.port=8688",
        "--conf", "spark.driver.maxResultSize=10g",
        "--conf", "spark.dynamicAllocation.enabled=false",
        "--conf", "spark.driver.cores=5",
        "--conf", "spark.network.timeout=600s",
        "--conf", "spark.default.parallelism=64",
        "--conf", "spark.sql.shuffle.partitions=64",
        "--conf", "spark.kryoserializer.buffer.max=2000m",
        "--conf", 'spark.serializer="org.apache.spark.serializer.KryoSerializer"',
        "--driver-memory", memory,
        "--executor-memory", "8g",
        "--conf", "spark.hadoop.fs.defaultFS=file:///",
        "--conf", "spark.driver.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -verbose:gc -XX:+PrintGCDetails'",
        "--conf", "spark.executor.extraJavaOptions='-XX:ReservedCodeCacheSize=512m -verbose:gc -XX:+PrintGCDetails'",
    ]


def spark_cmd(class_name: str, jar: str, args: list[str], spark_args_builder=common_spark_args) -> str:
    parts = [SPARK_SUBMIT] + spark_args_builder() + ["--class", class_name, jar] + args
    return " ".join(parts)


# ── Default arguments ───────────────────────────────────────────────────────
default_args = {
    "owner": "shard zhang",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}

# ── DAG ─────────────────────────────────────────────────────────────────────
dag = DAG(
    dag_id="ml1m_pipeline",
    default_args=default_args,
    description="ML-1M feature engineering: clean → features → join → encode → evaluate",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ml-1m", "recommender-system", "feature-engineering"],
    max_active_runs=1,
    concurrency=2,
)

# ── Tasks ────────────────────────────────────────────────────────────────────

# Stage 1: Data cleaning
clean_sample = BashOperator(
    task_id="clean_sample",
    bash_command=shell_wrap(spark_cmd(
        "processing.clean.ML1MCleanSample",
        JAR_PATH,
        [DATA_PATH],
    )),
    dag=dag,
)

# Stage 2a: Movie statistics
movie_stats = BashOperator(
    task_id="movie_stat_features",
    bash_command=shell_wrap(spark_cmd(
        "processing.feature.ML1MMovieStatFeature",
        JAR_PATH,
        [DATA_PATH],
    )),
    dag=dag,
)

# Stage 2b: User behavior sequences
user_behavior = BashOperator(
    task_id="user_movie_rate_sequence",
    bash_command=shell_wrap(spark_cmd(
        "processing.feature.ML1MUserMovieRateSequence",
        JAR_PATH,
        [DATA_PATH],
    )),
    dag=dag,
)

# Stage 3: Feature join
join_sample = BashOperator(
    task_id="join_sample",
    bash_command=shell_wrap(spark_cmd(
        "processing.join.ML1MJoinSample",
        JAR_PATH,
        [DATA_PATH],
    )),
    dag=dag,
)

# Stage 3b (optional): Negative sampling
neg_sampler = BashOperator(
    task_id="negative_sampler",
    bash_command=shell_wrap(spark_cmd(
        "processing.sampling.ML1MNegativeSampler",
        JAR_PATH,
        [
            "--input", DATA_PATH,
            "--output", f"{DATA_PATH}/neg_sample",
            "--neg_ratio", "5",
            "--strategy", "popular",
        ],
    )),
    dag=dag,
)

# Stage 4: Training sample generation
pipeline = BashOperator(
    task_id="ml1m_pipeline",
    bash_command=shell_wrap(spark_cmd(
        "pipeline.ML1MPipeline",
        JAR_PATH,
        [
            "--feature_threshold", "10",
            "--target_threshold", "1",
            "--sample_ratio", "1.0",
            "--input_dir", DATA_PATH,
            "--output_dir", f"{DATA_PATH}/train_sample",
            "--yesterday", DS_NODASH,
            "--parts", "10",
            "--output_format", "tfrecord",
            "--train_ratio", "0.8",
            "--val_ratio", "0.1",
            "--feature_config", FEATURE_CONFIG,
        ],
        spark_args_builder=pipeline_spark_args,
    )),
    dag=dag,
)

# Stage 5: Offline evaluation
evaluate = BashOperator(
    task_id="offline_evaluation",
    bash_command=shell_wrap(spark_cmd(
        "pipeline.eval.OfflineEvaluator",
        JAR_PATH,
        [
            "--data_path", f"{DATA_PATH}/train_sample/{DS_NODASH}/train/tfrecord",
            "--format", "tfrecord",
            "--label_col", "target",
            "--top_k", "20",
        ],
        spark_args_builder=pipeline_spark_args,
    )),
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────────────────────────
clean_sample >> [movie_stats, user_behavior]
movie_stats >> join_sample
user_behavior >> join_sample
join_sample >> neg_sampler
join_sample >> pipeline
neg_sampler >> pipeline
pipeline >> evaluate
