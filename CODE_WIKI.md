# Gerbil Data - 代码维基

## 1. 项目概述

Gerbil Data 是一个数据处理和样本生成的 pipeline，用于使用 MovieLens 1M (ML-1M) 数据集构建推荐系统。该项目处理原始数据、提取特征，并以 TFRecord 和 Parquet 格式生成训练样本，供机器学习模型使用。

### 主要功能
- 原始数据清洗与验证
- 特征提取（用户、物品、上下文、行为序列）
- 统计特征计算
- 交叉特征生成
- TFRecord 和 Parquet 样本输出
- 特征词表管理

## 2. 项目架构

```
gerbil-data/
├── bash/                    # 执行任务的 Shell 脚本
├── docs/                    # 文档
├── proto/                   # TensorFlow Example 的 Protobuf 定义
├── sql/                     # SQL 脚本
├── src/
│   └── main/
│       ├── java/            # Java 工具类（TensorFlow 生态）
│       └── scala/           # Scala 主实现
│           ├── driver/      # 驱动程序
│           ├── encoder/     # 特征编码器
│           ├── feature/     # 特征提取模块
│           ├── sample/      # 样本生成与清洗
│           └── utils/       # 工具函数
└── pom.xml                  # Maven 配置
```

## 3. 主要模块和职责

### 3.1 样本模块 ([`src/main/scala/sample/`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/))
- **[`ML1MCleanSample.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MCleanSample.scala)**: 清洗原始评分数据、过滤无效记录、去重，并输出 CSV 文件。
- **[`ML1MJoinSample.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MJoinSample.scala)**: 将清洗后的样本与用户画像、物品特征和用户行为序列关联，形成完整的训练样本。
- **[`ML1MTrainSample.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MTrainSample.scala)**: 定义训练样本类及其从关联数据中解析的逻辑。

### 3.2 特征模块 ([`src/main/scala/feature/`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/feature/))
- **[`ML1MUserMovieRate.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/feature/ML1MUserMovieRate.scala)**: 构造带时间窗口的用户电影评分序列。
- **[`ML1MMovieStatFeature.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/feature/ML1MMovieStatFeature.scala)**: 计算电影统计数据（平均评分、评分人数、流行度排名）。

### 3.3 编码器模块 ([`src/main/scala/encoder/`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/encoder/))
- **[`FeatureEncoder4ML1M.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/encoder/FeatureEncoder4ML1M.scala)**: 将 ML-1M 样本编码为特征，包括原始特征、交叉特征和目标值。

### 3.4 驱动模块 ([`src/main/scala/driver/`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/driver/))
- **[`ML1MDataDriver.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/driver/ML1MDataDriver.scala)**: 主驱动程序，负责协调特征编码、词表构建以及 TFRecord/Parquet 生成。

### 3.5 工具模块 ([`src/main/scala/utils/`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/utils/))
- **[`LogUtils.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/utils/LogUtils.scala)**: 为 Spark Shell 提供彩色日志工具。
- **[`TFRecord.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/utils/TFRecord.scala)**: 用于构建 TensorFlow Example 协议缓冲区的辅助函数。
- **[`ParquetRecord.scala`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/utils/ParquetRecord.scala)**: 用于构建 Parquet 记录的辅助函数。

## 4. 依赖项

### 核心库
- **Apache Spark**: 3.4.0 (core, sql, mllib, hive)
- **Scala**: 2.12.17
- **TensorFlow Hadoop**: 用于 TFRecord 读写
- **Protobuf**: 3.6.0 (用于 TensorFlow Example 协议)
- **FastJSON**: 用于 JSON 解析
- **Apache Hadoop**: 3.3.4

### 构建系统
- 带 Scala 插件的 Maven
- Shade 插件用于依赖重定位 (protobuf, guava)

## 5. 关键类和函数

### 5.1 [`ML1MTrainSample`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MTrainSample.scala)
包含所有特征的主要训练样本类：
- 用户特征：`user_id`、`gender`、`age`、`occupation`、`zip_code`、`user_rate_cnt`、`user_avg_rate` 等
- 物品特征：`item_id`、`movie_title`、`movie_genres`、`movie_rate_count`、`movie_avg_rate`、`movie_hot_rank` 等
- 上下文特征：`time_hour`、`time_area`、`week_day`
- 行为序列：带时间窗口的 `user_movie_rates`、`user_genres_rates`
- 目标值：`target` (多分类)、`label` (二分类)、`rating` (回归)

### 5.2 [`FeatureEncoder4ML1M`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/encoder/FeatureEncoder4ML1M.scala)
将样本编码为特征，包括：
- **原始特征**：用户 ID、性别、年龄、电影 ID、类型等
- **交叉特征**：二阶和三阶特征组合
- **目标值**：评分 (回归)、标签 (二分类)、目标 (多分类)

### 5.3 [`ML1MDataDriver`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/driver/ML1MDataDriver.scala)
主驱动程序，包含：
- `feature_encoder`: 返回配置好的 FeatureEncoder4ML1M
- `get_pos_info_from_a_sample`: 从样本中提取特征位置
- `parse_a_sample_tfrecord`: 将样本解析为 TensorFlow Example
- `parse_a_sample_parquet`: 将样本解析为 ParquetRecord
- `save_pos_map`/`restore_pos_map`: 保存/加载特征词表
- `run`: 主处理 pipeline

## 6. 数据流程

1. **原始数据 → 清洗样本** ([`ML1MCleanSample`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MCleanSample.scala)): 清洗 ratings.dat
2. **清洗样本 → 特征提取**:
   - 用户行为序列 ([`ML1MUserMovieRate`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/feature/ML1MUserMovieRate.scala))
   - 电影统计数据 ([`ML1MMovieStatFeature`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/feature/ML1MMovieStatFeature.scala))
3. **关联所有特征** ([`ML1MJoinSample`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/sample/ML1MJoinSample.scala)): 组合用户、物品和行为特征
4. **编码并生成样本** ([`ML1MDataDriver`](file:///Users/dazhang/PycharmProject/gerbil-data/src/main/scala/driver/ML1MDataDriver.scala)): 编码为 TFRecord 和 Parquet

## 7. 如何运行项目

### 前置要求
- JDK 8+
- Scala 2.12
- Maven
- Apache Spark 3.4.0

### 构建项目
```bash
mvn clean package -DskipTests
```

### 运行步骤 1：清洗原始数据
```bash
# 使用 bash/sample/ML1MCleanSample.sh 中的 bash 脚本
# 或直接使用 spark-submit 运行：
spark-submit --class sample.ML1MCleanSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <ml-1m_dataset_path>
```

### 运行步骤 2：提取用户行为序列
```bash
spark-submit --class feature.ML1MUserMovieRate \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <ml-1m_dataset_path>
```

### 运行步骤 3：计算电影统计数据
```bash
spark-submit --class feature.ML1MMovieStatFeature \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <ml-1m_dataset_path>
```

### 运行步骤 4：关联特征
```bash
spark-submit --class sample.ML1MJoinSample \
  target/gerbil-data-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <ml-1m_dataset_path>
```

### 运行步骤 5：生成 TFRecord/Parquet 样本
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
  --base_dir <output_directory>
```

## 8. 特征类型

### 原始特征
- 用户：ID、性别、年龄、职业、邮编、评分统计
- 物品：ID、标题、类型、评分人数、平均评分、热门排名、上映年份
- 上下文：小时、时段、星期几、周末标志
- 行为：电影评分序列、类型评分序列

### 交叉特征
- 二阶：类型 × 用户类型偏好、年份 × 年龄等
- 三阶：类型 × 年龄 × 性别等

## 9. 输出格式

### TFRecord
- TensorFlow Example 格式
- 为 TensorFlow 模型高效的二进制存储

### Parquet
- 列式存储
- 模式在 `parquet_schema` 方法中定义

### 词表文件
- `nn_pos_map.json`：人类可读的结构化特征映射
- `nn_pos_map.bin`：带均值/标准差用于归一化的二进制特征映射

## 10. 参考资料
- [MovieLens 1M 数据集](https://grouplens.org/datasets/movielens/1m/)
- [TensorFlow Example pb协议](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example)
- [TensorFlow Hadoop](https://github.com/tensorflow/ecosystem/tree/master/hadoop)
- [Spark TensorFlow Connector](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector)
