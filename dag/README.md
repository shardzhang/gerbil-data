# ML-1M Pipeline DAG

ML-1M 特征工程流水线，包含两个入口：

- **`run_pipeline.py`** — 独立运行脚本（本地开发 / CI，无需 Airflow）
- **`ml1m_pipeline_dag.py`** — Airflow DAG（生产调度）

## Pipeline 结构

```
clean_sample ─┬─ movie_stat_features ──┐
              └─ user_movie_rate_seq  ──┤
                                         ▼
                                    join_sample ─┬─ negative_sampler (可选) ──┐
                                                 │                              ▼
                                                 └────────► ml1m_pipeline ──► offline_evaluation
```

| Stage | Scala Class | 功能 |
|---|---|---|
| `clean_sample` | `ML1MCleanSample` | 清洗原始 ratings（去重、过滤、添加天级分区） |
| `movie_stat_features` | `ML1MMovieStatFeature` | 电影统计特征（评分人数、平均分、热度排名） |
| `user_movie_rate_sequence` | `ML1MUserMovieRateSequence` | 用户行为序列（按时间排序的电影 ID 序列） |
| `join_sample` | `ML1MJoinSample` | 将上述特征 Join 到每条评分记录 |
| `negative_sampler` | `ML1MNegativeSampler` | 负采样（按 popularity 1:5） |
| `ml1m_pipeline` | `ML1MPipeline` | 特征编码 + 词典构建 + 训练/验证/测试集划分，输出 TFRecord |
| `offline_evaluation` | `OfflineEvaluator` | 离线评估，计算 top-K 指标 |

## 环境准备

```bash
source bash/conf/env.sh
```

`env.sh` 会自动配置以下环境变量：

| 变量 | 说明 |
|---|---|
| `SPARK_HOME` | Spark 3.4.0 安装路径 |
| `JAVA_HOME` | JDK 1.8 |
| `JAR_PATH` | gerbil-data uber-jar 路径 |
| `ML_1M_PATH` | ML-1M 数据集根目录 |
| `PROJECT_HOME` | 项目根目录 |

## 方式一：独立运行（无需 Airflow）

```bash
# Dry-run 查看命令
python3 dag/run_pipeline.py --dry-run --skip-neg-sample

# 完整运行
python3 dag/run_pipeline.py

# 指定日期（用于 --yesterday 参数）
python3 dag/run_pipeline.py --date 20260612
```

可选参数：
- `--skip-neg-sample` — 跳过负采样
- `--dry-run` — 只打印命令，不执行
- `--date YYYYMMDD` — 指定执行日期

## 方式二：Airflow 调度

```bash
cp dag/ml1m_pipeline_dag.py $AIRFLOW_HOME/dags/
```

1. 安装 Airflow 后初始化数据库
2. 设置 Airflow Variables 或通过 `env.sh` 配置环境
3. 启动 scheduler 和 webserver
4. 手动触发：`airflow dags trigger ml1m_pipeline`

DAG 默认每日调度（`schedule="@daily"`），不回溯历史。

### 查看任务运行状态

首次使用 `airflow tasks states-for-dag-run` 时缺少 run_id 参数，CLI 会提示用法。补充正确的 run_id 后即可查看本次 DAG Run 中各 Task 的执行状态：

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

### 查看 DAG Run 列表

列出指定 DAG 的所有（含历史）运行记录：

```bash
 ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow dags list-runs -d ml1m_pipeline

/Users/dazhang/PycharmProject/gerbil-data/.venv/lib/python3.9/site-packages/airflow/utils/dot_renderer.py:29 UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.
dag_id        | run_id                            | state   | execution_date            | start_date                       | end_date
==============+===================================+=========+===========================+==================================+=========
ml1m_pipeline | manual__2026-06-12T04:57:14+00:00 | running | 2026-06-12T04:57:14+00:00 | 2026-06-12T04:57:23.162014+00:00 |
```

### 查看 JSON 格式任务状态（含管道符混乱版）

使用 `-o json` 输出 JSON 格式，便于程序化解析。下面演示了缺少管道符和正确使用 `jq` 格式化的两种写法：

```bash
 ~/PycharmProject/gerbil-data  on main !1  airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00 -o json` ~/PycharmProject/gerbil-data  on main ⇣2⇡1 !4 ?1  airflow tasks states-for-dag-run ml1m_pipeline manual__2026-06-12T04:57:14+00:00 -o json | jq .         ✔  gerbil-data Py  at 13:04:46
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
    "task_id": "offline_evaluation",``````
    "state": "success",
    "start_date": "2026-06-12T05:32:48.916991+00:00",
    "end_date": "2026-06-12T05:49:31.688929+00:00"
  }
]`
[{"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "clean_sample", "state": "success", "start_date": "2026-06-12T04:57:23.895341+00:00", "end_date": "2026-06-12T04:58:03.805839+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "movie_stat_features", "state": "success", "start_date": "2026-06-12T04:58:05.929723+00:00", "end_date": "2026-06-12T04:58:52.409529+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "user_movie_rate_sequence", "state": "success", "start_date": "2026-06-12T04:58:56.464890+00:00", "end_date": "2026-06-12T04:59:21.757937+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "join_sample", "state": "success", "start_date": "2026-06-12T04:59:25.818581+00:00", "end_date": "2026-06-12T04:59:52.152856+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "ml1m_encode", "state": "success", "start_date": "2026-06-12T05:19:04.782324+00:00", "end_date": "2026-06-12T05:32:45.615393+00:00"}, {"dag_id": "ml1m_pipeline", "execution_date": "2026-06-12T04:57:14+00:00", "task_id": "offline_evaluation", "state": "success", "start_date": "2026-06-12T05:32:48.916991+00:00", "end_date": "2026-06-12T05:49:31.688929+00:00"}]
```

### 查看 JSON 格式任务状态（正确版）

正确使用 `-o json | jq .` 格式化输出，便于阅读所有 Task 的完整信息：

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