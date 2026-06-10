# gerbil-data

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.12.17-red)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.4.0-orange)](https://spark.apache.org/)

基于 Apache Spark 的推荐系统数据处理和训练样本生成 pipeline。处理原始用户-物品交互数据，提取丰富的特征（用户、物品、上下文、行为序列），并输出 **TFRecord** 和 **Parquet** 格式的训练样本供机器学习模型使用。

目前支持 [MovieLens 1M (ML-1M)](https://grouplens.org/datasets/movielens/1m/) 数据集，模块化架构可扩展至其他数据集。

## 功能特性

- **数据清洗** — 过滤无效记录、去重、验证原始交互数据
- **特征提取** — 用户画像、物品属性、上下文特征、可配置时间窗口的行为序列
- **统计特征** — 基于计数和比率的特征（流行度、活跃度、评分方差等）
- **交叉特征** — 二阶和三阶特征组合，捕捉更深层模式
- **多格式输出** — TFRecord（TensorFlow Example protobuf）和 Parquet（列式存储）
- **词表管理** — 特征位置映射，支持 JSON（可读）和二进制（在线推理归一化）
- **多种预测目标** — 多分类、二分类、回归

## 项目架构

```
gerbil-data/
├── bash/                       # 执行 pipeline 步骤的 Shell 脚本
│   ├── conf/                   # 环境配置
│   ├── pipeline/               # 训练样本生成脚本
│   ├── processing/             # 数据预处理脚本
│   │   ├── clean/              #   数据清洗
│   │   ├── feature/            #   特征提取
│   │   └── join/               #   特征关联
│   ├── proto/                  # Protobuf 编译
│   └── tools/                  # 工具脚本
├── docs/                       # 文档
├── proto/                      # TensorFlow Example protobuf 定义
├── sql/                        # Hive/Spark SQL 脚本
├── src/
│   └── main/
│       ├── java/               # Java 工具类（TensorFlow Hadoop I/O）
│       └── scala/
│           ├── processing/     # ETL：原始数据 → 平面中间表
│           │   ├── clean/      #   数据清洗与验证
│           │   ├── feature/    #   特征衍生（统计量、序列）
│           │   └── join/       #   多表特征关联
│           ├── featurizer/     # ML 编码：特征 → 嵌入索引
│           │   ├── core/       #   抽象特征化框架
│           │   └── ml1m/       #   ML-1M 具体实现
│           ├── pipeline/       # 编排与训练样本生成
│           │   ├── serde/      #   序列化（TFRecord、Parquet、pos-map）
│           │   └── stats/      #   在线统计（运行值、位置信息）
│           ├── tfrecords/      # 自定义 Spark SQL TFRecord 数据源
│           │   ├── serde/      #   序列化/反序列化
│           │   └── udf/        #   用户自定义函数
│           └── utils/          # 工具函数
├── pom.xml                     # Maven 构建配置
└── requirements.txt            # Python 依赖
```

### Pipeline 数据流

```
                         ┌─────────────────────────────┐
                         │         原始数据源            │
                         │  ratings.dat  users.dat      │
                         │  movies.dat                  │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │       ML1MCleanSample       │
                         │  数据过滤 · 去重 · 校验     │
                         │  输出: clean_sample/        │
                         └──────────────┬───────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
 ┌─────────────────────────────┐ ┌─────────────────────┐ ┌──────────────────┐
 │  ML1MUserMovieRateSequence  │ │ML1MMovieStatFeature │ │ (users.dat)      │
 │  按用户提取行为序列          │ │电影统计: 评分次数、 │ │ 用户画像解析     │
 │  (1天/3天/7天/15天/全部)     │ │均分、热度排名       │ │                  │
 │  输出: user_movie_rate/     │ │输出: item_feature/  │ └────────┬─────────┘
 └──────────────┬──────────────┘ └──────────┬──────────┘          │
                │                           │                    │
                └───────────────┬───────────┴────────────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │       ML1MJoinSample        │
                 │  关联用户 + 物品 + 上下文    │
                 │  + 行为序列                 │
                 │  输出: join_sample/         │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │        ML1MPipeline         │
                 │  特征编码 · Hash → 嵌入索引  │
                 │  词表管理                   │
                 ├─────────────────────────────┤
                 │  输出:                      │
                 │  ├── tfrecord/              │
                 │  ├── parquet/               │
                 │  ├── pos_map.json           │
                 │  ├── pos_map.bin            │
                 │  └── pos_map.txt            │
                 └─────────────────────────────┘
```

## 前置要求

- **Java** 8+
- **Scala** 2.12
- **Maven** 3.x
- **Apache Spark** 3.4.0
- **protoc** 3.6.0（编译 protobuf 用，可选）

## 快速开始

### 1. 构建项目

```bash
mvn clean package -DskipTests
```

### 2. 下载 ML-1M 数据集

```bash
curl -O https://files.grouplens.org/datasets/movielens/ml-1m.zip
unzip ml-1m.zip
```

### 3. 运行 Pipeline

#### 步骤 1：清洗原始数据
```bash
spark-submit --class processing.clean.ML1MCleanSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 2：提取用户行为序列
```bash
spark-submit --class processing.feature.ML1MUserMovieRateSequence \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 3：计算电影统计特征
```bash
spark-submit --class processing.feature.ML1MMovieStatFeature \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 4：关联所有特征
```bash
spark-submit --class processing.join.ML1MJoinSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 5：生成 TFRecord / Parquet 样本
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

### 或使用 Shell 脚本运行

```bash
# 编辑 bash/conf/env.sh 配置你的路径
bash bash/processing/clean/ML1MCleanSample.sh
bash bash/processing/feature/ML1MMovieStatFeature.sh
bash bash/processing/feature/ML1MUserMovieRateSequence.sh
bash bash/processing/join/ML1MJoinSample.sh
bash bash/pipeline/ML1MPipeline.sh
```

## 特征类型

### 原始特征

| 类别 | 特征 |
|------|------|
| 用户 | ID、性别、年龄、职业、邮编、评分次数、平均评分、评分方差、活跃天数 |
| 物品 | ID、标题、类型、类型数量、评分次数、平均评分、热度排名、上映年份 |
| 上下文 | 小时、时段（凌晨/上午/下午/晚上）、星期几、是否周末 |
| 行为 | 电影评分序列（全部/1天/3天/7天/15天）、类型评分序列 |

### 交叉特征（可配置）

- **二阶交叉**：类型 × 用户类型偏好、上映年份 × 年龄、热度 × 用户均分、类型 × 性别、类型 × 是否周末
- **三阶交叉**：年龄 × 性别 × 类型、上映年份 × 年龄 × 职业、类型 × 性别 × 职业

### 预测目标

- **多分类**：评分（1-5 星）作为类别目标
- **二分类**：评分 >= 3 为正样本，< 3 为负样本
- **回归**：原始评分值

## 输出格式

### TFRecord
二进制 protobuf 记录，TensorFlow Example 格式，针对 TensorFlow 模型训练优化。

### Parquet
列式存储格式，兼容 Spark 及多种大数据工具。

### 词表文件
- `pos_map.json` — 人类可读的结构化特征位置映射
- `pos_map.bin` — 带均值/标准差的二进制特征映射，用于在线归一化
- `pos_map.txt` — 字段维度汇总文本文件

## 项目模块

| 模块 | 说明 |
|------|------|
| `processing` | ETL pipeline：数据清洗、特征衍生、多表关联 |
| `featurizer` | ML 特征编码：类别/连续/交叉特征化器，基于哈希的嵌入索引 |
| `pipeline` | 编排：样本生成、词表管理、TFRecord/Parquet 输出 |
| `tfrecords` | 自定义 Spark SQL TFRecord 数据源 |
| `utils` | 日志、日期工具、protobuf 辅助函数 |

## 依赖项

- **Apache Spark** 3.4.0 (core, sql, mllib, hive)
- **Scala** 2.12.17
- **Protobuf** 3.6.0
- **Hadoop** 3.3.4
- **TensorFlow Hadoop**（内嵌，用于 TFRecord I/O）

## 贡献指南

欢迎贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

## 许可证

本项目基于 MIT 许可证开源 — 详见 [LICENSE](LICENSE) 文件。

## 参考资料

- [MovieLens 1M 数据集](https://grouplens.org/datasets/movielens/1m/)
- [TensorFlow Example 协议](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example)
- [TensorFlow Hadoop](https://github.com/tensorflow/ecosystem/tree/master/hadoop)
- [Spark TensorFlow Connector](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector)
