"""
Standalone ML-1M pipeline orchestrator — for CI / dev environments without Airflow.

Usage:
  python dag/run_pipeline.py [--date YYYYMMDD] [--skip-neg-sample] [--dry-run]

Runs each stage in topological order, respects dependencies, and fails fast on errors.
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import argparse
import shlex

ROOT = Path(__file__).resolve().parent.parent
JAR = ROOT / "target" / "gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar"
FEATURE_CONFIG = ROOT / "src" / "main" / "resources" / "ml1m" / "features.yaml"

BASE_HOME = Path(os.environ.get("BASE_HOME", Path.home() / "PycharmProject"))
DATA_PATH = Path(os.environ.get("ML_1M_PATH", BASE_HOME / "data" / "ml-1m"))

SPARK_BASE = [
    "spark-submit",
    "--master", "local[*]",
    "--conf", "spark.ui.port=8688",
    "--conf", "spark.driver.maxResultSize=10g",
    "--conf", "spark.dynamicAllocation.enabled=false",
    "--conf", "spark.driver.cores=5",
    "--conf", "spark.default.parallelism=64",
    "--conf", "spark.sql.shuffle.partitions=64",
    "--conf", "spark.kryoserializer.buffer.max=2000m",
    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
    "--driver-memory", "8g",
    "--executor-memory", "8g",
    "--conf", "spark.hadoop.fs.defaultFS=file:///",
]

STAGES = {
    "clean_sample": {
        "class": "processing.clean.ML1MCleanSample",
        "args": [str(DATA_PATH)],
        "depends": [],
        "desc": "Clean raw ratings (dedup, filter, add day)",
    },
    "movie_stats": {
        "class": "processing.feature.ML1MMovieStatFeature",
        "args": [str(DATA_PATH)],
        "depends": ["clean_sample"],
        "desc": "Compute movie statistics (avg rate, count, hot rank)",
    },
    "user_behavior": {
        "class": "processing.feature.ML1MUserMovieRateSequence",
        "args": [str(DATA_PATH)],
        "depends": ["clean_sample"],
        "desc": "Build user movie rating sequences",
    },
    "join_sample": {
        "class": "processing.join.ML1MJoinSample",
        "args": [str(DATA_PATH)],
        "depends": ["movie_stats", "user_behavior"],
        "desc": "Join all features into unified sample",
    },
    "neg_sampler": {
        "class": "processing.sampling.ML1MNegativeSampler",
        "args": [
            "--input", str(DATA_PATH),
            "--output", str(DATA_PATH / "neg_sample"),
            "--neg_ratio", "5",
            "--strategy", "popular",
        ],
        "depends": ["join_sample"],
        "desc": "Generate negative samples",
        "optional": True,
    },
    "encode": {
        "class": "pipeline.ML1MPipeline",
        "args": lambda date: [
            "--feature_threshold", "10",
            "--target_threshold", "1",
            "--sample_ratio", "1.0",
            "--input_dir", str(DATA_PATH),
            "--output_dir", str(DATA_PATH / "train_sample"),
            "--yesterday", date,
            "--parts", "10",
            "--output_format", "tfrecord",
            "--train_ratio", "0.8",
            "--val_ratio", "0.1",
            "--feature_config", str(FEATURE_CONFIG),
        ],
        "depends": ["join_sample"],
        "desc": "Encode features + build vocabulary + write TFRecord",
    },
    "evaluate": {
        "class": "pipeline.eval.OfflineEvaluator",
        "args": lambda date: [
            "--data_path", str(DATA_PATH / "train_sample" / date / "train" / "tfrecord"),
            "--format", "tfrecord",
            "--label_col", "target",
            "--top_k", "20",
        ],
        "depends": ["encode"],
        "desc": "Evaluate output quality (label dist + feature coverage)",
    },
}


ENV_SH = ROOT / "bash" / "conf" / "env.sh"


def build_cmd(class_name: str, args: list[str]) -> list[str]:
    return SPARK_BASE + ["--class", class_name, str(JAR)] + args


def shell_wrap(cmd: list[str]) -> str:
    """Wrap a command so it runs in bash after sourcing env.sh."""
    if ENV_SH.exists():
        return f"source {ENV_SH} && " + shlex.join(cmd)
    return shlex.join(cmd)


def run_stage(stage_name: str, date: str, dry_run: bool = False) -> bool:
    info = STAGES[stage_name]
    if callable(info["args"]):
        args = info["args"](date)
    else:
        args = info["args"]
    cmd = build_cmd(info["class"], args)
    shell_cmd = shell_wrap(cmd)
    print(f"\n{'='*72}")
    print(f"Stage: {stage_name}")
    print(f"Desc:  {info['desc']}")
    print(f"Cmd:   {shell_cmd}")
    print(f"{'='*72}")
    if dry_run:
        print("[DRY-RUN] skipped\n")
        return True
    result = subprocess.run(["bash", "-c", shell_cmd], capture_output=False)
    if result.returncode != 0:
        print(f"\n[FAIL] {stage_name} exited with code {result.returncode}")
        return False
    print(f"\n[OK]   {stage_name} completed")
    return True


def toposort(stages: dict, skip_optional: bool) -> list[str]:
    """Topological sort of stages; respects dependencies.
    拓扑排序经典写法: 天然保证依赖项一定排在当前节点前面.
    :param stages: dict, 阶段字典，结构示例：
        {
            "stageA": {"depends": ["stageB", "stageC"], "optional": False},
            "stageB": {"depends": [], "optional": False},
            "stageC": {"depends": [], "optional": True}
        }
    :param skip_optional: bool. True = 跳过可选阶段；False = 一并执行可选阶段
    """
    visited = set()
    order = []

    def dfs(node: str):
        if node in visited:
            return
        visited.add(node)
        info = stages.get(node)
        if info is None:
            return
        if skip_optional and info.get("optional"):
            return

        # 先递归处理所有依赖 → 再加入当前节点
        for dep in info.get("depends", []):
            dfs(dep)
        order.append(node)

    for name in stages:
        dfs(name)
    return order


def main():
    parser = argparse.ArgumentParser(description="ML-1M pipeline orchestrator")
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y%m%d"), help="Execution date (YYYYMMDD)")
    parser.add_argument("--skip-neg-sample", action="store_true", help="Skip negative sampling stage")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    args = parser.parse_args()

    if not JAR.exists():
        print(f"JAR not found: {JAR}")
        print("Run 'mvn package -DskipTests' first.")
        sys.exit(1)

    order = toposort(STAGES, skip_optional=args.skip_neg_sample)
    print(f"Pipeline order: {' -> '.join(order)}")
    print(f"Execution date: {args.date}")
    print(f"Data path:      {DATA_PATH}")
    print(f"Feature config: {FEATURE_CONFIG}")
    if args.dry_run:
        print("Mode: DRY-RUN\n")

    for stage in order:
        if not run_stage(stage, args.date, dry_run=args.dry_run):
            sys.exit(1)

    print(f"\n{'='*72}")
    print("Pipeline completed successfully.")
    print(f"{'='*72}")


if __name__ == "__main__":
    main()
