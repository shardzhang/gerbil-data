env.sh 和 uber-jar 都存在。两种运行方式：
1. 开发/调试（不需要 Airflow）
   python3 dag/run_pipeline.py --date $(date +%Y%m%d)
   可选参数：
- --skip-neg-sample — 跳过负采样
- --dry-run — 只打印命令不执行
- --date YYYYMMDD — 指定执行日期（用于训练样本的 --yesterday 参数）
2. 生产（Airflow）
   cp dag/ml1m_pipeline_dag.py $AIRFLOW_HOME/dags/
   然后 Airflow Scheduler 会自动按每日调度执行。
   不过运行前需要确保 env.sh 前置条件满足，检查其中几项是否已就位：
# 检查 protoc
ls -la $HOME/Library/Caches/protoc-3.6.0-osx-x86_64/bin/protoc
# 检查 Spark
ls -la $HOME/Library/Caches/spark-3.4.0-bin-hadoop3/bin/spark-submit
# 检查 ML-1M 原始数据
ls ~/PycharmProject/data/ml-1m/ratings.dat


```bash

~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  source .venv/bin/activate                                                                                               ✔  at 13:00:35
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline                                                          ✔  gerbil-data Py  at 13:00:36
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

 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  export AIRFLOW_HOME=~/airflow                                                                  ✔  took 4s  gerbil-data Py  at 13:01:08
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
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | ml1m_pipeline            | None    |                                  |
ml1m_pipeline | 2026-06-12T04:57:14+00:00 | offline_evaluation       | None    |                                  |

```

```bash
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow dags list-runs -d ml1m_pipeline                                                                 ✔  gerbil-data Py  at 13:01:41

/Users/dazhang/PycharmProject/gerbil-data/.venv/lib/python3.9/site-packages/airflow/utils/dot_renderer.py:29 UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.
dag_id        | run_id                            | state   | execution_date            | start_date                       | end_date
==============+===================================+=========+===========================+==================================+=========
ml1m_pipeline | manual__2026-06-12T04:57:14+00:00 | running | 2026-06-12T04:57:14+00:00 | 2026-06-12T04:57:23.162014+00:00 |


```

```bash
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00 -o json              1 ✘  gerbil-data Py  at 13:04:22
[{"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "clean_sample", "state": "success", "start_date": "2026-06-12T04:57:23.895341+00:00", "end_date": "2026-06-12T04:58:03.805839+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "movie_stat_features", "state": "success", "start_date": "2026-06-12T04:58:05.929723+00:00", "end_date": "2026-06-12T04:58:52.409529+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "user_movie_rate_sequence", "state": "success", "start_date": "2026-06-12T04:58:56.464890+00:00", "end_date": "2026-06-12T04:59:21.757937+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "join_sample", "state": "success", "start_date": "2026-06-12T04:59:25.818581+00:00", "end_date": "2026-06-12T04:59:52.152856+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "negative_sampler", "state": "running", "start_date": "2026-06-12T04:59:54.114350+00:00", "end_date": ""}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "ml1m_pipeline", "state": null, "start_date": "", "end_date": ""}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "offline_evaluation", "state": null, "start_date": "", "end_date": ""}]
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00 -o json | jq .         ✔  gerbil-data Py  at 13:04:37
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
    "task_id": "negative_sampler",
    "state": "running",
    "start_date": "2026-06-12T04:59:54.114350+00:00",
    "end_date": ""
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "ml1m_pipeline",
    "state": null,
    "start_date": "",
    "end_date": ""
  },
  {
    "dag_id": "ml1m_pipeline",
    "execution_date": "2026-06-12T04:57:14+00:00",
    "task_id": "offline_evaluation",
    "state": null,
    "start_date": "",
    "end_date": ""
  }
]
```