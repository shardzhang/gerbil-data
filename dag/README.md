# ML-1M Pipeline DAG

ML-1M feature engineering pipeline with two entry points:

- **`run_pipeline.py`** — Standalone script (local dev / CI, no Airflow required)
- **`ml1m_pipeline_dag.py`** — Airflow DAG (production scheduling)

## Pipeline Structure

```
clean_sample ─┬─ movie_stat_features ──┐
              └─ user_movie_rate_seq  ──┤
                                         ▼
                                    join_sample ─┬─ negative_sampler (optional) ──┐
                                                 │                                 ▼
                                                 └────────► ml1m_pipeline ──► offline_evaluation
```

| Stage | Scala Class | Description |
|---|---|---|
| `clean_sample` | `ML1MCleanSample` | Clean raw ratings (dedup, filter, add day partition) |
| `movie_stat_features` | `ML1MMovieStatFeature` | Movie statistics (rating count, avg, hot rank) |
| `user_movie_rate_sequence` | `ML1MUserMovieRateSequence` | User behavior sequences (time-sorted movie ID sequences) |
| `join_sample` | `ML1MJoinSample` | Join all features into each rating record |
| `negative_sampler` | `ML1MNegativeSampler` | Negative sampling (popularity-based 1:5) |
| `ml1m_pipeline` | `ML1MPipeline` | Feature encoding + vocabulary building + train/val/test split, output TFRecord |
| `offline_evaluation` | `OfflineEvaluator` | Offline evaluation, compute top-K metrics |

## Environment Setup

```bash
source bash/conf/env.sh
```

`env.sh` automatically configures the following environment variables:

| Variable | Description |
|---|---|
| `SPARK_HOME` | Spark 3.4.0 installation path |
| `JAVA_HOME` | JDK 1.8 |
| `JAR_PATH` | Path to gerbil-data uber-jar |
| `ML_1M_PATH` | ML-1M dataset root directory |
| `PROJECT_HOME` | Project root directory |

## Option 1: Standalone (No Airflow)

```bash
# Dry-run to preview commands
python3 dag/run_pipeline.py --dry-run --skip-neg-sample

# Full run
python3 dag/run_pipeline.py

# Specify date (for --yesterday parameter)
python3 dag/run_pipeline.py --date 20260612
```

Optional arguments:
- `--skip-neg-sample` — Skip negative sampling
- `--dry-run` — Print commands without executing
- `--date YYYYMMDD` — Specify execution date

## Option 2: Airflow Scheduling

```bash
cp dag/ml1m_pipeline_dag.py $AIRFLOW_HOME/dags/
```

1. Install Airflow and initialize the database
2. Set Airflow Variables or configure via `env.sh`
3. Start scheduler and webserver
4. Manually trigger: `airflow dags trigger ml1m_pipeline`

The DAG runs on a daily schedule (`schedule="@daily"`) with no backfill.

### Check Task Run Status

The first time you run `airflow tasks states-for-dag-run` without a run_id, the CLI shows usage help. After providing the correct run_id, it displays the execution status of each Task in the DAG Run:

```bash

~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  source .venv/bin/activate    
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline
Usage: airflow tasks states-for-dag-run [-h] [-o table, json, yaml, plain] [-v] dag_id execution_date_or_run_id

Get the status of all task instances in a dag run

Positional Arguments:
  dag_id                The id of the dag
  execution_date_or_run_id
                        The execution_date of the DAG or run_id of the DAGRun

Optional Arguments:
  -h, --help            show this help message and exit
  -o, --output (table, json, yaml, plain)
                        Output format. Allowed values: json, yaml, plain, table (default: table)
  -v, --verbose         Make logging output more verbose

airflow tasks states-for-dag-run command error: the following arguments are required: execution_date_or_run_id, see help above.

 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  export AIRFLOW_HOME=~/airflow
export PATH="/Users/dazhang/PycharmProject/gerbil-data/.venv/bin:$PATH"
echo "=== Task States ==="
airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00
=== Task States ===
dag_id        | execution_date            | task_id                  | state   | start_date                       | end_date
==============+===========================+==========================+=========+==================================+=================================
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | clean_sample             | success | 2026-06-12T04:57:23.895341+00:00 | 2026-06-12T04:58:03.805839+00:00
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | movie_stat_features      | success | 2026-06-12T04:58:05.929723+00:00 | 2026-06-12T04:58:52.409529+00:00
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | user_movie_rate_sequence | success | 2026-06-12T04:58:56.464890+00:00 | 2026-06-12T04:59:21.757937+00:00
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | join_sample              | success | 2026-06-12T04:59:25.818581+00:00 | 2026-06-12T04:59:52.152856+00:00
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | negative_sampler         | running | 2026-06-12T04:59:54.114350+00:00 |
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | ml1m_encode              | None    |                                  |
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | offline_evaluation       | None    |                                  |

```

### List DAG Runs

List all (including historical) run records for a specific DAG:

```bash
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow dags list-runs -d ml1m_pipeline

/Users/dazhang/PycharmProject/gerbil-data/.venv/lib/python3.9/site-packages/airflow/utils/dot_renderer.py:29 UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.
dag_id        | run_id                            | state   | execution_date            | start_date                       | end_date
==============+===================================+=========+===========================+==================================+=========
ml1m_pipeline | manual__2026-06-12T04:57:14+00:00 | running | 2026-06-12T04:57:14+00:00 | 2026-06-12T04:57:23.162014+00:00 |
```

### Check Task Status in JSON (Clean Format)

Using `-o json | jq .` for properly formatted output, making it easy to read all Task details:

```bash
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00 -o json | jq .
[
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "clean_sample",
    "state": "success",
    "start_date": "2026-06-12T04:57:23.895341+00:00",
    "end_date": "2026-06-12T04:58:03.805839+00:00"
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "movie_stat_features",
    "state": "success",
    "start_date": "2026-06-12T04:58:05.929723+00:00",
    "end_date": "2026-06-12T04:58:52.409529+00:00"
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "user_movie_rate_sequence",
    "state": "success",
    "start_date": "2026-06-12T04:58:56.464890+00:00",
    "end_date": "2026-06-12T04:59:21.757937+00:00"
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "join_sample",
    "state": "success",
    "start_date": "2026-06-12T04:59:25.818581+00:00",
    "end_date": "2026-06-12T04:59:52.152856+00:00"
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "ml1m_pipeline",
    "state": "success",
    "start_date": "2026-06-12T05:19:04.782324+00:00",
    "end_date": "2026-06-12T05:32:45.615393+00:00"
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "offline_evaluation",
    "state": "success",
    "start_date": "2026-06-12T05:32:48.916991+00:00",
    "end_date": "2026-06-12T05:49:31.688929+00:00"
  }
]
```