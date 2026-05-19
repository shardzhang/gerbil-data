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
│   ├── sample/                 # 样本生成脚本
│   └── feature/                # 特征提取脚本
├── docs/                       # 文档
├── proto/                      # TensorFlow Example protobuf 定义
├── sql/                        # Hive/Spark SQL 脚本
├── src/
│   └── main/
│       ├── java/               # Java 工具类（TensorFlow Hadoop I/O）
│       └── scala/
│           ├── driver/         # Pipeline 驱动（协调器）
│           ├── encoder/        # 特征编码器
│           │   └── vectorizer/ # 基础特征向量化框架
│           ├── feature/        # 特征提取模块
│           ├── sample/         # 样本清洗与关联
│           ├── tfrecords/      # 自定义 Spark SQL TFRecord 数据源
│           │   ├── serde/      # 序列化/反序列化
│           │   └── udf/        # 用户自定义函数
│           └── utils/          # 工具函数
├── pom.xml                     # Maven 构建配置
└── requirements.txt            # Python 依赖
```

### Pipeline 数据流

```
原始数据 (ratings.dat)
       │
       ▼
┌──────────────────┐
│  ML1MCleanSample │  清洗并验证原始评分数据
└────────┬─────────┘
         │
         ├──────────────────────────────┐
         ▼                              ▼
┌──────────────────┐     ┌─────────────────────────┐
│ML1MUserMovieRate │     │ ML1MMovieStatFeature    │
│用户行为序列提取   │     │ 电影统计特征计算         │
│(可配置时间窗口)   │     │ (评分次数、均分、热度)   │
└────────┬─────────┘     └──────────┬──────────────┘
         │                          │
         └──────────┬───────────────┘
                    ▼
         ┌──────────────────┐
         │  ML1MJoinSample  │  关联用户 + 物品 + 行为特征
         └────────┬─────────┘
                  │
                  ▼
         ┌──────────────────┐
         │  ML1MDataDriver  │  特征编码并生成
         │                  │  TFRecord / Parquet 样本
         └──────────────────┘
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
spark-submit --class sample.ML1MCleanSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 2：提取用户行为序列
```bash
spark-submit --class feature.ML1MUserMovieRate \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 3：计算电影统计特征
```bash
spark-submit --class feature.ML1MMovieStatFeature \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 4：关联所有特征
```bash
spark-submit --class sample.ML1MJoinSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /path/to/ml-1m
```

#### 步骤 5：生成 TFRecord / Parquet 样本
```bash
spark-submit --class driver.ML1MDataDriver \
  --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --yesterday <date> \
  --window <window_size> \
  --parts <num_partitions> \
  --feature_threshold <threshold> \
  --target_threshold <threshold> \
  --sample_ratio <ratio> \
  --input_dir /path/to/ml-1m \
  --output_dir /path/to/output
```

### 或使用 Shell 脚本运行

```bash
# 编辑 bash/conf/env.sh 配置你的路径
bash bash/sample/ML1MCleanSample.sh
bash bash/sample/ML1MJoinSample.sh
bash bash/feature/ML1MMovieStatFeature.sh
bash bash/feature/ML1MUserMovieRate.sh
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
- `nn_pos_map.json` — 人类可读的结构化特征位置映射
- `nn_pos_map.bin` — 带均值/标准差的二进制特征映射，用于在线归一化

## 项目模块

| 模块 | 说明 |
|------|------|
| `sample` | 数据清洗、特征关联、训练样本定义 |
| `feature` | 特征提取（行为序列、物品统计） |
| `encoder` | 特征编码 pipeline 和向量化框架 |
| `driver` | Pipeline 协调器和输出生成 |
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
