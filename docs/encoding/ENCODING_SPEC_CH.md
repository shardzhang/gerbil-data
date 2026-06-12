# ML-1M 特征编码规范

参考实现：Scala Spark 流水线（`featurizer/` 包）。
目标平台：C++ 在线推理服务。

本文档定义了 C++ 特征编码器必须复现的每一个计算步骤，以确保在线编码与离线训练数据**比特级完全一致**。

---

## 1. 特征注册表

特征定义在 `features.yaml` 中（参见 [features.yaml](features.yaml)）。

### 结构

```yaml
pkg: featurizer.ml1m          # Scala 包前缀（与 C++ 无关）
features:
  - name: <string>             # 特征名，用作 TFRecord 字段后缀
    index: <int>               # 唯一数值特征索引
    type: <int>                # 1 = 类别型, 0 = 连续型
    class: <string>            # Scala 类名（与 C++ 无关）
    enabled: <bool>            # true = 启用
cross_features:
  - name: <string>
    index: <int>
    depends: [<name>, ...]     # 需要交叉的类别特征名列表
    enabled: <bool>
```

### 类型语义

| type | 含义 | 编码路径 |
|------|------|----------|
| 1    | 类别型（离散） | Hash(f_index \|\| feature_id) → pos_map 查找 → 嵌入索引 |
| 0    | 连续型（数值） | feature_id 直接用作位置（无哈希）→ pos_map 查找 → 嵌入索引 |

---

## 2. MurmurHash3 — 跨语言约定

### 算法

`MurmurHash3_x64_128`（128 位，x64 架构变体）。

- **参考实现（Java）**：`utils.MurmurHash3.murmurhash3_x64_128()`
  （[源代码](../../src/main/java/utils/MurmurHash3.java)）
- **种子（Seed）**：`0x3c074a61`（常量，定义在 `RawFeature.SEED`）
- **输出**：128 位哈希，分为两个 64 位值 `(val1, val2)`
- **使用的值**：`val1`（128 位哈希的前 64 位）
- **取模归约**：`hash % dim`，若为负数则加 `dim`
  ```java
  long hash = p.val1 % dim;
  if (hash < 0) hash += dim;
  ```

### 输入键格式

所有特征哈希使用如下构建的**二进制键**：

```
[f_index: 4 bytes LE] [feature_id: 8 bytes LE]
```

总键长度 = **12 字节**。

- `f_index`：32 位有符号整数，**小端**字节序
- `feature_id`：64 位有符号长整数，**小端**字节序

### C++ 验证

本项目使用的 Java 实现基于
[yonik/java_util](https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java)，
它是 Austin Appleby 最终 C++ 版本（[smhasher](https://github.com/aappleby/smhasher)）的直接移植。

来自 `smhasher` 的 C++ 实现产生比特级一致的结果：

```cpp
// C++ — 预期与 Java 完全一致
#include "MurmurHash3.h"

uint64_t val1, val2;
MurmurHash3_x64_128(key, len, seed, &val1, &val2);
// val1 是 C++ 函数写入的第一个 uint64_t
```

---

## 3. 特征 ID 计算

每个特征都有一个 `parse(sample)` 方法，生成一个
`(raw_string, feature_id, value)` 三元组列表。对于每个启用的特征，
C++ 必须精确地复现这些规则。

### 3.1 简单整数特征

特征 ID 是解析为整数的原始值。

| 特征 | 索引 | 字段 | 规则 |
|------|------|------|------|
| `user_age` | 2 | `sample.age` | `toInt` |
| `user_occupation` | 4 | `sample.occupation` | `toInt` |
| `user_movie_rate` | 301 | `(itemId, rating)` 对 | `itemId.toInt`, value = `rating.toFloat` |
| `user_movie_rate_1day` | 302 | 同上 | 同上 |
| `user_movie_rate_3day` | 303 | 同上 | 同上 |
| `user_movie_rate_7day` | 304 | 同上 | 同上 |
| `user_movie_rate_15day` | 305 | 同上 | 同上 |

### 3.2 分桶整数特征

原始值通过匹配规则映射到桶 ID。

#### UserRateStd（索引 = 6, 7, 8, 9）

```scala
val buck = sample.user_rate_std match {
  case 0.0F       => 1
  case x if x < 1 => 2
  case _          => 3
}
```

#### UserMovieRateCnt（索引 = 10, 11, 12, 13）

```scala
val buck = sample.user_rate_cnt match {
  case 0          => 1
  case x if x < 5 => 2
  case _          => 3
}
```

#### UserAvgRate（索引 = 14, 15, 16, 17）

```scala
val buck = sample.user_avg_rate match {
  case x if x < 1.0 => 1
  case x if x < 2.0 => 2
  case x if x < 3.0 => 3
  case x if x < 3.5 => 4
  case x if x < 4.0 => 5
  case _            => 6
}
```

#### MovieRateCount（索引 = 104）

```scala
val buck = sample.movie_rate_count match {
  case 0               => 1
  case x if x <= 2     => 2
  case x if x <= 5     => 3
  case x if x <= 10    => 4
  case x if x <= 20    => 5
  case x if x <= 50    => 6
  case x if x <= 100   => 7
  case x if x <= 200   => 8
  case x if x <= 500   => 9
  case x if x <= 1000  => 10
  case _               => 11
}
```

#### MovieAvgRate（索引 = 105）

```scala
val buck = sample.movie_avg_rate match {
  case x if x <= 0.0 => 1
  case x if x <= 1.0 => 2
  case x if x <= 2.0 => 3
  case x if x <= 2.5 => 4
  case x if x <= 3.0 => 5
  case x if x <= 3.5 => 6
  case x if x <= 4.0 => 7
  case x if x <= 4.5 => 8
  case _             => 9
}
```

#### MovieGenreCnt（索引 = 106）

```scala
val buck = sample.movie_genre_cnt match {
  case 1             => 1
  case 2             => 2
  case 3             => 3
  case 4             => 4
  case 5             => 5
  case x if x <= 7   => 6
  case x if x <= 10  => 7
  case _             => 8
}
```

#### MovieHotRank（索引 = 107）

```scala
val buck = sample.movie_hot_rank match {
  case x if x <= 0    => 0
  case x if x <= 100  => 1
  case x if x <= 500  => 2
  case x if x <= 1000 => 3
  case x if x <= 2000 => 4
  case x if x <= 3000 => 5
  case x if x <= 5000 => 6
  case _              => 7
}
```

#### MoviePublishYear（索引 = 108）

```scala
val year = sample.movie_publish_year
val buck = year match {
  case x if x < 1960 => 1
  case x if x < 1970 => 2
  case x if x < 1980 => 3
  case x if x < 1990 => 4
  case x if x < 2000 => 5
  case _             => 6
}
```

#### ContextTimeArea（索引 = 202）

```scala
val buck = sample.time_hour / 4  // 整数除法：0-5
```

#### ContextIsWeekend（索引 = 204）

```scala
val buck = sample.week_day match {
  case 6 | 7 => 1  // 周六/周日
  case _     => 0
}
```

### 3.3 枚举映射特征

#### UserGender（索引 = 3）

```scala
val buck = sample.gender match {
  case "M" => 1
  case "F" => 2
  case _   => 0
}
```

### 3.4 字符串哈希特征（对原始字符串应用 MurmurHash3）

对于原始值为任意字符串的特征，`feature_id` 按如下方式计算：

```scala
val p = new MurmurHash3.LongPair()
MurmurHash3.murmurhash3_x64_128(string.getBytes("UTF-8"), 0, string.length, SEED, p)
feature_id = p.val1  // 使用第一个 64 位字
```

键 = 字符串的 `UTF-8 字节`（不是 Java 内部的 char[] 表示）。

| 特征 | 索引 | 字符串来源 |
|------|------|------------|
| `movie_title` | 102 | 标题的每个单词（空格分割，移除尾部 `(year)` 后） |
| `movie_genres` | 103 | 每个类型字符串（小写） |
| `user_genres_rate` | 306 | 类型名称 |
| `user_genres_rate_1day` | 307 | 类型名称 |
| `user_genres_rate_3day` | 308 | 类型名称 |
| `user_genres_rate_7day` | 309 | 类型名称 |
| `user_genres_rate_15day` | 310 | 类型名称 |
| `user_genres_rate_cnts` | 312 | 类型名称 |
| `user_genres_rate_cnt_1days` | 313 | 类型名称 |
| `user_genres_rate_cnt_3days` | 314 | 类型名称 |
| `user_genres_rate_cnt_7days` | 315 | 类型名称 |
| `user_genres_rate_cnt_15days` | 316 | 类型名称 |
| `user_top3_genres` | 317 | 类型名称 |
| `user_watch_same_genre` | 351 | 参见第 3.6 节 |

### 3.5 连续特征

`feature_id` = `1L`（始终为 1）。数值存储在 `value_list` 中。

| 特征 | 索引 | 值来源 |
|------|------|--------|
| `user_rate_std_continue` | 18 | `sample.user_rate_std` |
| `user_rate_std_7day_continue` | 19 | `sample.user_rate_std_7day` |
| `user_rate_std_15day_continue` | 21 | `sample.user_rate_std_15day` |
| `user_rate_std_30day_continue` | 22 | `sample.user_rate_std_30day` |
| `user_avg_rate_continue` | 23 | `sample.user_avg_rate` |
| `user_avg_rate_7day_continue` | 24 | `sample.user_avg_rate_7day` |
| `user_avg_rate_15day_continue` | 25 | `sample.user_avg_rate_15day` |
| `user_avg_rate_30day_continue` | 26 | `sample.user_avg_rate_30day` |
| `movie_avg_rate_continue` | 109 | `sample.movie_avg_rate.toFloat` |
| `user_same_genre_avg_rate_continue` | 357 | `sample.same_genre_avg_rate` |

### 3.6 同类型特征（索引 351–357）

这些特征计算当前电影的类型与用户最近观看电影类型之间的重叠。
实现细节在 `ML1MUserWatchSameGenre.scala` 中。

> **C++ 实现者注意**：这些特征需要访问用户最近的电影观看历史（类型级别），
> 必须作为请求上下文的一部分提供。

### 3.7 Top-3 类型（索引 317）

`sample.user_top3_genres` 在数据准备阶段预计算完成，为用户历史中
按评分计数排序的前 3 个类型。每个类型名称通过 MurmurHash3 哈希
（同第 3.4 节）。

---

## 4. 类别特征编码

对于 `feature_list` 中的每个类别特征值：

### 步骤 1：哈希

```scala
def computeHash(fea: Long, dim: Long): Long = {
  val bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
  bb.putInt(0, f_index)     // 4 字节，小端
  bb.putLong(4, fea)         // 8 字节，小端
  val p = new LongPair()
  MurmurHash3.murmurhash3_x64_128(bb.array(), 0, 12, SEED, p)
  var hash = p.val1 % dim
  if (hash < 0) hash += dim
  hash
}
```

### 步骤 2：Pos-map 查找

```scala
if (pos_map.contains((f_index, hash))) {
  val pos = pos_map((f_index, hash))  // 嵌入索引
  // 输出 (raw, pos, value)
}
```

不在 `pos_map` 中的值（训练中出现频率低）会被**静默丢弃**。

### 步骤 3：输出三元组

每个保留下来的特征值输出一个三元组：

```
(raw: String, index: Long, value: Float)
```

位置 0 的哨兵条目（始终存在）：
```
raw = "R:", index = 0L, value = 1.0F
```

---

## 5. 连续特征编码

对于每个连续特征值：

### 步骤 1：位置

位置 = `feature_list(i)`（对于单值连续特征始终为 `1L`）。

通过 pos-map 查找：
```scala
if (pos_map.contains((f_index, fea))) {
  val pos = pos_map((f_index, fea)).toLong
  // 输出
}
```

### 步骤 2：值

值 = `value_list(i)`（原始数值，编码时不进行归一化）。

> 归一化（均值/标准差）在模型层面处理（TF/PyTorch），不在特征编码器中。

---

## 6. 交叉特征编码

对于交叉特征，枚举所有组成特征的笛卡尔积。

### 哈希键

```
[f_index_1: 4B LE] [feature_id_1: 8B LE] [f_index_2: 4B LE] [feature_id_2: 8B LE] ...
```

总长度 = `(4 + 8) * N` 字节，其中 N = 组成特征的数量。

### 位置

与类别特征相同的 pos_map 查找，使用交叉特征的 `f_index`。

---

## 7. 输出格式

离线流水线在 TFRecord 中为每个特征生成三列：

| 列 | 类型 | 内容 |
|----|------|------|
| `{name}_raw` | `bytes` 列表 | 原始值字符串（人类可读） |
| `{name}_index` | `int64` 列表 | 嵌入位置索引 |
| `{name}_value` | `float` 列表 | 权重/数值 |

### 目标

存储为单个 `float` 值。目标是正样本的 `item_id`（多分类）。

---

## 8. 哈希维度

`max_dim = 1L << 60`（2^60），定义在 `ML1MPipeline.max_dim`。

仅在 pos-map 构建期间用于计算哈希值。pos-map 构建完成后，
每个特征的实际嵌入维度由 `pos_dim`（该特征的不同高频值的数量）决定。

---

## 9. C++ 参考实现检查清单

- [ ] MurmurHash3_x64_128：seed=`0x3c074a61`，小端 12 字节键
- [ ] 从 128 位输出中取 `val1`，对 `dim` 取模，负数修正
- [ ] 所有 44 个启用的特征的特征 ID 提取（参见第 3 节）
- [ ] 所有类别特征的分桶规则
- [ ] 通过 UTF-8 → MurmurHash3 对类型/电影标题特征进行字符串哈希
- [ ] pos_map.bin 加载（参见 [pos_map_bin_format.md](pos_map_bin_format.md)）
- [ ] pos_map 查找：`(f_index, hash) → pos`
- [ ] 交叉特征的笛卡尔积枚举
- [ ] 每个特征位置 0 的哨兵 `(R:, 0L, 1.0F)`

---

## 附录：特征快速参考

| 索引 | 名称 | 类型 | 特征 ID 来源 |
|------|------|------|-------------|
| 2 | user_age | categorical | `age.toInt` (1/18/25/35/45/50/56) |
| 3 | user_gender | categorical | enum(M=1, F=2, else=0) |
| 4 | user_occupation | categorical | `occupation.toInt` |
| 6 | user_rate_std | categorical | bucket(0→1, <1→2, else→3) |
| 7 | user_rate_std_7day | categorical | 同 6 |
| 8 | user_rate_std_15day | categorical | 同 6 |
| 9 | user_rate_std_30day | categorical | 同 6 |
| 10 | user_movie_rate_cnt | categorical | bucket(0→1, <5→2, else→3) |
| 11 | user_movie_rate_cnt_7day | categorical | 同 10 |
| 12 | user_movie_rate_cnt_15day | categorical | 同 10 |
| 13 | user_movie_rate_cnt_30day | categorical | 同 10 |
| 14 | user_avg_rate | categorical | bucket(<1→1, <2→2, <3→3, <3.5→4, <4→5, else→6) |
| 15 | user_avg_rate_7day | categorical | 同 14 |
| 16 | user_avg_rate_15day | categorical | 同 14 |
| 17 | user_avg_rate_30day | categorical | 同 14 |
| 18 | user_rate_std_continue | continuous | id=1L, value=raw float |
| 19 | user_rate_std_7day_continue | continuous | id=1L, value=raw float |
| 21 | user_rate_std_15day_continue | continuous | id=1L, value=raw float |
| 22 | user_rate_std_30day_continue | continuous | id=1L, value=raw float |
| 23 | user_avg_rate_continue | continuous | id=1L, value=raw float |
| 24 | user_avg_rate_7day_continue | continuous | id=1L, value=raw float |
| 25 | user_avg_rate_15day_continue | continuous | id=1L, value=raw float |
| 26 | user_avg_rate_30day_continue | continuous | id=1L, value=raw float |
| 102 | movie_title | categorical | hash(标题中去除年份后的每个单词) |
| 103 | movie_genres | categorical | hash(每个类型，小写) |
| 104 | movie_rate_count | categorical | bucket（11 级） |
| 105 | movie_avg_rate | categorical | bucket（9 级） |
| 106 | movie_genre_cnt | categorical | bucket（8 级） |
| 107 | item_hot_rank | categorical | bucket（7 级） |
| 108 | movie_publish_year | categorical | bucket（6 级） |
| 109 | movie_avg_rate_continue | continuous | id=1L, value=raw float |
| 201 | context_time_hour | categorical | `time_hour.toInt` (0-23) |
| 202 | context_time_area | categorical | `time_hour / 4` (0-5) |
| 203 | context_time_week | categorical | `week_day` (1-7) |
| 204 | context_is_weekend | categorical | enum(weekend→1, else→0) |
| 301-305 | user_movie_rate* | categorical | 每个 `itemId.toInt`, value=rating |
| 306-310 | user_genres_rate* | categorical | 每个 hash(类型名称), value=avg rating |
| 312-316 | user_genres_rate_cnt* | categorical | 每个 hash(类型名称), value=count |
| 317 | user_top3_genres | categorical | 每个 hash(类型名称) |
| 351-355 | user_watch_same_genre* | categorical | 每个 hash(item ID) |
| 356 | user_same_genre_avg_rate | categorical | bucket（6 级） |
| 357 | user_same_genre_avg_rate_continue | continuous | id=1L, value=raw float |
