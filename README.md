# gerbil-data

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.12.17-red)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.4.0-orange)](https://spark.apache.org/)
[![CI](https://img.shields.io/github/actions/workflow/status/shardzhang/gerbil-data/ci.yml?branch=main)](https://github.com/shardzhang/gerbil-data/actions/workflows/ci.yml)

A production-grade feature engineering pipeline for recommender systems, built on Apache Spark. It processes raw user-item interaction data through an ETL pipeline, extracts rich features (user demographics, item attributes, context signals, multi-window behavior sequences), and outputs featurized training samples in **TFRecord** and **Parquet** formats — ready for TensorFlow deep learning models.

Currently supports the [MovieLens 1M (ML-1M)](https://grouplens.org/datasets/movielens/1m/) dataset with a modular, extensible architecture designed for easy adaptation to other datasets.

## Features

1. **Data Cleaning and Feature Extraction**: Transforms raw interaction logs into structured training samples. Handles deduplication, anomaly filtering, and validation of raw data through Spark SQL. Extracts user profiles, item attributes, context signals, and behavior sequences with configurable time windows — covering the full spectrum of features needed for recommendation models.

2. **Negative Sampling**: For each positive instance, generates unobserved items as negative samples — a critical component for CTR model training. Supports uniform random, popularity-biased sampling with exponent 0.75, and hybrid strategies. The popularity-biased variant mitigates the "Matthew effect" where popular items dominate training, leading to better generalization on long-tail items.

3. **Statistical and Cross Features**: Computes count-based and ratio-based features (popularity, activity, rating variance, etc.) alongside second-order and third-order feature crosses for deeper pattern capture. All 58 features are encoded through a type-safe generic `Featurizer[T]` architecture — each producing `{name}_raw / _index / _value` triplets, the standard embedding lookup schema for FFM, DeepFM, DIN, and similar models. Supports both hash-based encoding for rapid prototyping and vocabulary-based embedding for production serving. Features are registered via YAML with zero code changes required.

4. **Vocabulary Management and Multi-Format Output**: Builds embedding vocabularies through frequency thresholding, allocating slots for frequent values while recycling rare ones. Feature position maps are persisted in JSON (readable) and binary with mean/std (online normalization). Outputs training samples in TFRecord (TensorFlow Example protobuf) and Parquet (columnar storage) formats, with time-based train/val/test split to prevent data leakage.

5. **Data Quality**: Guards against training-serving skew and data drift. Validates column-level statistics — null ratios, cardinality, numeric distributions — across ETL stages. Tracks parse success rates and target distribution during featurization. Cross-run drift detection alerts when volume deviates by over 20%, null ratios shift by over 5%, or means change by over 50%.

6. **TFRecord Data Source**: A custom Spark SQL connector bridging ETL and TensorFlow. Provides native `format("tfrecords")"` read/write with schema inference, Example and SequenceExample protobuf handling, and full Spark type codecs — eliminating intermediate data conversion.

7. **Configuration**: YAML-driven feature registry for experimentation velocity. Adding or disabling features requires editing a single config file — no code changes, no recompilation. Supports classpath and external file loading.

8. **Orchestration**: Dual-mode pipeline execution. Airflow DAG for production scheduling; standalone Python runner for local development and CI. Topological sort ensures stage dependencies are honored.

9. **C++ Online Inference**: A bit-exact C++ reimplementation of the Scala featurizer for latency-critical serving. Loads the identical vocabulary binary and executes the same MurmurHash3 with matching key concatenation — eliminating training-serving skew. Verified by golden data diff across thousands of rows.

10. **Multiple Prediction Targets and Infrastructure**: Supports multi-class classification, binary classification, and regression targets. Includes Spark-submit wrappers with environment configuration, Hive DDLs for persistent tables, and TensorFlow protobuf definitions.

## Architecture

```
gerbil-data/
├── bash/                       # Shell scripts for running pipeline steps
│   ├── conf/                   # Environment configuration
│   ├── pipeline/               # Training sample generation scripts
│   ├── processing/             # Data preprocessing scripts
│   │   ├── clean/              #   Data cleaning
│   │   ├── feature/            #   Feature extraction
│   │   └── join/               #   Feature joining
│   ├── proto/                  # Protobuf compilation
│   └── tools/                  # Utility scripts
├── dag/                        # Pipeline DAG (Airflow + standalone)
│   ├── ml1m_pipeline_dag.py    # Airflow DAG definition
│   └── run_pipeline.py         # Standalone runner (no Airflow required)
├── docs/                       # Documentation
├── proto/                      # TensorFlow Example protobuf definitions
├── sql/                        # Hive/Spark SQL scripts
├── src/
│   └── main/
│       ├── java/               # Java utilities (TensorFlow Hadoop I/O)
│       └── scala/
│           ├── processing/     # ETL: raw data → flat intermediate tables
│           │   ├── clean/      #   Data cleaning & validation
│           │   ├── feature/    #   Feature derivation (stats, sequences)
│           │   └── join/       #   Multi-table feature joining
│           ├── featurizer/     # ML encoding: features → embedding indices
│           │   ├── core/       #   Abstract featurization framework
│           │   └── ml1m/       #   ML-1M concrete implementations
│           ├── pipeline/       # Orchestration & training sample generation
│           │   ├── serde/      #   Serialization (TFRecord, Parquet, pos-map)
│           │   └── stats/      #   Online statistics (running value, pos info)
│           ├── tfrecords/      # Custom Spark SQL TFRecord data source
│           │   ├── serde/      #   Serialization/deserialization
│           │   └── udf/        #   User-defined functions
│           └── utils/          # Utility functions
├── pom.xml                     # Maven build configuration
└── requirements.txt            # Python dependencies
```

### Pipeline Overview

```
                         ┌─────────────────────────────┐
                         │       Raw Data Sources       │
                         │  ratings.dat  users.dat      │
                         │  movies.dat                  │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │       ML1MCleanSample       │
                         │  Filter · Dedup · Validate  │
                         │  Output: clean_sample/      │
                         └──────────────┬───────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
 ┌─────────────────────────────┐ ┌─────────────────────┐ ┌──────────────────┐
 │  ML1MUserMovieRateSequence  │ │ML1MMovieStatFeature │ │ (users.dat)      │
 │  Behavior sequences by user │ │Movie stats: count,  │ │ User profile     │
 │  (1d / 3d / 7d / 15d / all)│ │avg rate, hot rank   │ │ parsing          │
 │  Output: user_movie_rate/   │ │Output: item_feature/│ └────────┬─────────┘
 └──────────────┬──────────────┘ └──────────┬──────────┘          │
                │                           │                    │
                └───────────────┬───────────┴────────────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │       ML1MJoinSample        │
                 │  Join user + item + context │
                 │  + behavior sequences       │
                 │  Output: join_sample/       │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │        ML1MPipeline         │
                 │  Feature encoding · Hash →  │
                 │  embedding index            │
                 │  Vocabulary management      │
                 ├─────────────────────────────┤
                 │  Output:                    │
                 │  ├── tfrecord/              │
                 │  ├── parquet/               │
                 │  ├── pos_map.json           │
                 │  ├── pos_map.bin            │
                 │  └── pos_map.txt            │
                 └─────────────────────────────┘
```

## Prerequisites

- **Java** 8+
- **Scala** 2.12
- **Maven** 3.x
- **Apache Spark** 3.4.0
- **protoc** 3.6.0 (for protobuf compilation, optional)

## Quick Start

### 1. Build the project

```bash
mvn clean package -DskipTests
```

### 2. Download the ML-1M dataset

```bash
curl -O https://files.grouplens.org/datasets/movielens/ml-1m.zip
unzip ml-1m.zip
```

### 3. Run the pipeline

#### Step 1: Clean raw data
```bash
spark-submit --class processing.clean.ML1MCleanSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### Step 2: Extract user behavior sequences
```bash
spark-submit --class processing.feature.ML1MUserMovieRateSequence \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### Step 3: Compute movie statistics
```bash
spark-submit --class processing.feature.ML1MMovieStatFeature \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### Step 4: Join all features
```bash
spark-submit --class processing.join.ML1MJoinSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### Step 5: Generate TFRecord / Parquet samples
```bash
spark-submit --class pipeline.ML1MPipeline \
  --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --yesterday <date> \
  --parts <num_partitions> \
  --feature_threshold <threshold> \
  --target_threshold <threshold> \
  --sample_ratio <ratio> \
  --input_dir /path/to/ml-1m \
  --output_dir /path/to/output \
  --output_format tfrecord
```

### Or run with shell scripts

```bash
# Edit bash/conf/env.sh with your paths
bash bash/processing/clean/ML1MCleanSample.sh
bash bash/processing/feature/ML1MMovieStatFeature.sh
bash bash/processing/feature/ML1MUserMovieRateSequence.sh
bash bash/processing/join/ML1MJoinSample.sh
bash bash/pipeline/ML1MPipeline.sh
```

## Feature Types

### Raw Features

| Category | Features |
|----------|----------|
| User | ID, gender, age, occupation, zip code, rating count, avg rating, rating std, active days |
| Item | ID, title, genres, genre count, rating count, avg rating, hot rank, publish year |
| Context | time hour, time area (morning/afternoon/evening/night), week day, weekend flag |
| Behavior | movie rating sequences (all-time, 1d, 3d, 7d, 15d), genre rating sequences |

### Cross Features (configurable)

- **Second-order**: genre × user genre preference, publish year × age, hot rank × user avg rating, genre × gender, genre × weekend
- **Third-order**: age × gender × genre, publish year × age × occupation, genre × gender × occupation

### Targets

- **Multi-class**: rating (1-5) as categorical target
- **Binary**: rating >= 3 as positive, < 3 as negative
- **Regression**: raw rating value

## Output Formats

### TFRecord
Binary protobuf records in TensorFlow Example format, optimized for TensorFlow model training.

### Parquet
Columnar storage format compatible with Spark and many big data tools.

### Vocabulary Files
- `pos_map.json` — Human-readable structured feature position mapping
- `pos_map.bin` — Binary feature mapping with mean/std for online normalization
- `pos_map.txt` — Field dimension summary in plain text

## Project Modules

| Module | Description |
|--------|-------------|
| `processing` | ETL pipeline: data cleaning, feature derivation, multi-table joining |
| `sampling` | Negative sampling for CTR training (random/popular/mixed) |
| `featurizer` | ML feature encoding: categorical/continuous/cross featurizers, hash/PosMap embedding |
| `pipeline` | Orchestration: sample generation, vocabulary management, TFRecord/Parquet output |
| `config` | YAML-driven feature configuration (SnakeYAML → Scala case classes) |
| `tfrecords` | Custom Spark SQL data source for TFRecord format |
| `utils` | Logging, MurmurHash3, date utilities, protobuf helpers |
| `dag` | Pipeline orchestration: Airflow DAG (production) + standalone Python runner (CI/dev) |
| `bash` | Spark-submit wrapper scripts with environment configuration |
| `sql` | Hive DDL for persistent tables |
| `proto` | TensorFlow Example / SequenceExample protobuf definitions |
| `tools` | C++ online inference featurizer + golden data generators |

## Dependencies

- **Apache Spark** 3.4.0 (core, sql, mllib, hive)
- **Scala** 2.12.17
- **Protobuf** 3.6.0
- **Hadoop** 3.3.4
- **TensorFlow Hadoop** (for TFRecord I/O, embedded)

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## References

- [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/)
- [TensorFlow Example Protocol](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example)
- [TensorFlow Hadoop](https://github.com/tensorflow/ecosystem/tree/master/hadoop)
- [Spark TensorFlow Connector](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector)
