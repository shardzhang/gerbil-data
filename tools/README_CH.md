# Tools — ML-1M 特征编码

## C++ 特征处理器 (`cpp_featurizer/`)

C++ 在线推理特征编码器，与 Scala 训练流程完全按位一致。

```bash
cd tools/cpp_featurizer
mkdir build && cd build
cmake .. && make
./ml1m_featurizer [pos_map_path]
```

- `pos_map_path` 可选；若提供，则加载 `pos_map.bin` 用于嵌入位置查找
- 向标准输出输出 CSV：`feature_name,field_index,field_type,raw,feature_id,hash,value`
- 包含 58 个常规特征 + 22 个交叉特征（所有交叉特征已注册但在生产配置中禁用）

### 文件

- `types.h` — 共享类型、常量、工具函数
- `sample.h` — `ML1MSample` 数据结构（58 个字段）
- `pos_map.h` — `pos_map.bin` 二进制加载器（支持 Java `writeUTF` 格式）
- `MurmurHash3.h/.cpp` — smhasher 实现（与 Java yonik MurmurHash3 按位一致）
- `feature.h` — `AbstractFeature` 基类 + `CrossFeature` + 全部 58 个具体实现
- `featurizer.h` — 编排器（注册、解析、编码）
- `main.cpp` — 入口点、测试样本、CSV 输出

## Scala 黄金数据生成器 (`DumpGoldenData.scala`)

用于验证 C++ 输出的独立黄金数据生成器。

```bash
# 1. 编译 MurmurHash3.java（来自主项目）
javac -d target/classes src/main/java/utils/MurmurHash3.java

# 2. 编译并运行
scalac -cp target/classes tools/DumpGoldenData.scala -d tools/
scala -cp target/classes:tools DumpGoldenData > /tmp/output.csv
```

输出格式与 C++ 特征处理器的 CSV 完全一致。

## pos_map.bin 生成器 (`DumpPosMapBin.scala`)

生成小型测试 `pos_map.bin`，用于验证 C++ 二进制读取器。

```bash
# 编译 MurmurHash3.java 后：
scalac -cp target/classes tools/DumpPosMapBin.scala -d tools/
scala -cp target/classes:tools DumpPosMapBin
```

输出：`/tmp/test_pos_map.bin`

### 二进制格式（与生产环境 `PosMapSerDe` 一致）

| 区域 | 字段 | 字节序 |
|---------|--------|------------|
| 头部 | `timestamp` (int64), `pos_map_size` (int32) | 小端序 |
| Pos-map 条目 | `field_name` (writeUTF), `field_index` (int32), `field_type` (int32), `dim` (int32), `hash` (int64), `pos` (int32), `mean` (float64), `std` (float64) | writeUTF = 大端序长度；其他均为小端序 |
| Target-map | `target_map_size` (int32)，后接 K 对 `raw_target` (int32) / `mapped_target` (int32) | 小端序 |

## 验证

比较 C++ 和 Scala 输出：

```bash
# 分别生成
scala -cp target/classes:tools DumpGoldenData > /tmp/scala_output.csv
./tools/cpp_featurizer/build/ml1m_featurizer > /tmp/cpp_output.csv

# 差异比较（仅比较哈希列——浮点数格式可能不同）
diff <(awk -F, '{print $1,$2,$3,$5,$6}' /tmp/scala_output.csv) \
     <(awk -F, '{print $1,$2,$3,$5,$6}' /tmp/cpp_output.csv)
```
