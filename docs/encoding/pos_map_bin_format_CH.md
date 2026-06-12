# pos_map.bin — 二进制格式规范

该文件由 `PosMapSerDe.saveToBin()`（Scala）生成，供 C++ 在线推理服务加载特征词表。

---

## 概述

`.bin` 文件包含三个连续的部分：

1. **头部（Header）** — 时间戳 + pos_map 条目数
2. **Pos-map 条目** — 每个高频特征值一条
3. **Target-map 条目** — 每个训练目标一条

所有多字节值均为**小端序**（由 `LittleEndianDataOutputStream` 写入）。

---

## 第 1 节：头部

| 偏移 | 大小 | 类型 | 字段 | 说明 |
|------|------|------|------|------|
| 0 | 8 | `int64` | `timestamp` | 流水线执行日期，整数格式（`yyyyMMdd`，如 `20260610`） |
| 8 | 4 | `int32` | `pos_map_size` | Pos-map 条目数（N） |

---

## 第 2 节：Pos-map 条目

接下来的 `N` 个条目，每个的大小**因条目而异**（由于 UTF-8 字符串字段）。

### 条目布局

| 偏移 | 大小 | 类型 | 字段 | 说明 |
|------|------|------|------|------|
| 0 | 2 + M | `UTF` | `field_name` | 修正的 UTF-8 字符串（见下方说明）。前 2 字节 = 长度 M，后 M 字节为 UTF-8 数据 |
| 2+M | 4 | `int32` | `field_index` | 特征索引（如 `user_age` 为 2） |
| 6+M | 4 | `int32` | `field_type` | 特征类型：1 = 类别型，0 = 连续型 |
| 10+M | 4 | `int32` | `dim` | 该特征的嵌入维度（最大分配位置 + 1） |
| 14+M | 8 | `int64` | `hash` | 该特征值的 MurmurHash3_x64_128 输出（`val1`） |
| 22+M | 4 | `int32` | `pos` | 分配的嵌入位置索引 |
| 26+M | 8 | `float64` | `mean` | 连续型：观测值的均值。类别型：0.0 |
| 34+M | 8 | `float64` | `std` | 连续型：观测值的标准差。类别型：1.0 |

**每条总计**：`34 + M` 字节，其中 M = `field_name` 的 UTF-8 字节长度。

### UTF-8 字符串格式

由 Java 的 `DataOutputStream.writeUTF()` 写入 — 修正的 UTF-8，带 2 字节长度前缀（无符号）。
这与标准长度前缀 UTF-8 **不同**。主要区别：

- 字符 `\u0001`–`\u007F` → 1 字节（ASCII）
- 字符 `\u0080`–`\u07FF` → 2 字节
- 字符 `\u0800`–`\uFFFF` → 3 字节
- 空字符 `\u0000` → 2 字节（`0xC0 0x80`）

### C++ 读取代码

```cpp
#include <fstream>
#include <string>
#include <unordered_map>
#include <cstdint>

struct PosMapEntry {
    std::string field_name;
    int32_t     field_index;
    int32_t     field_type;
    int32_t     dim;
    int64_t     hash;
    int32_t     pos;
    double      mean;
    double      std;
};

std::string read_java_utf(std::ifstream& in) {
    uint16_t len;
    in.read(reinterpret_cast<char*>(&len), 2);
    // len 为 BIG-ENDIAN（DataOutputStream.writeUTF 的编码方式）
    len = (len >> 8) | (len << 8);  // 字节交换为本机序
    std::string s(len, '\0');
    in.read(&s[0], len);
    return s;
}

// 读取 pos_map_size N，然后：
for (int i = 0; i < N; i++) {
    PosMapEntry e;
    e.field_name  = read_java_utf(in);
    in.read(reinterpret_cast<char*>(&e.field_index), 4);
    in.read(reinterpret_cast<char*>(&e.field_type),  4);
    in.read(reinterpret_cast<char*>(&e.dim),          4);
    in.read(reinterpret_cast<char*>(&e.hash),         8);
    in.read(reinterpret_cast<char*>(&e.pos),          4);
    in.read(reinterpret_cast<char*>(&e.mean),         8);
    in.read(reinterpret_cast<char*>(&e.std),          8);
    // 所有多字节值为 LITTLE-ENDIAN
}
```

---

## 第 3 节：Target-map

在所有 N 个 pos-map 条目之后：

| 偏移 | 大小 | 类型 | 字段 | 说明 |
|------|------|------|------|------|
| 0 | 4 | `int32` | `target_map_size` | Target-map 条目数（K） |

然后是 K 个条目：

| 偏移 | 大小 | 类型 | 字段 | 说明 |
|------|------|------|------|------|
| 0 | 4 | `int32` | `raw_target` | 原始目标 ID（item_id） |
| 4 | 4 | `int32` | `mapped_target` | 顺序目标索引（从 0 开始） |

---

## 完整文件布局

```
[timestamp:      int64   LE]   8 字节
[pos_map_size:   int32   LE]   4 字节
[pos_map_entry_0: ...  ]       可变
[pos_map_entry_1: ...  ]       可变
...
[pos_map_entry_N-1: ...]       可变
[target_map_size: int32  LE]   4 字节
[target_map_entry_0: ... ]     8 字节
[target_map_entry_1: ... ]     8 字节
...
[target_map_entry_K-1:...]     8 字节
```

---

## 示例：解码输出

```
[Header]
  timestamp: 20260610
  pos_map_size: 12345

[Entry 0]
  field_name:  "user_age"
  field_index: 2
  field_type:  1
  dim:         8
  hash:        837429183746
  pos:         5
  mean:        0.0
  std:         1.0

[Entry 1]
  field_name:  "user_rate_std_continue"
  field_index: 18
  field_type:  0
  dim:         2
  hash:        1
  pos:         1
  mean:        3.25
  std:         1.1

...

[Target-map]
  target_map_size: 3662
  [0] raw_target: 2858  →  mapped_target: 0
  [1] raw_target: 260   →  mapped_target: 1
  ...
```

---

## 注意

1. **唯一性**：每个 `(field_index, hash)` 对最多出现一次。
2. **类别型均值/标准差**：始终为 `(0.0, 1.0)` — 归一化在模型层面处理，不在特征编码器中。
3. **连续型均值/标准差**：从训练数据计算得出 — 是该特征原始数值的总体均值和标准差。
4. **dim**：`dim` 字段是该特征的最大分配位置 + 1，即该特征的有效词表/嵌入大小。
5. **排序**：条目按迭代器顺序写入（不保证排序，但通常按 `field_index` 分组并按 `pos` 排序）。
6. **字节序**：在 Linux 上使用 `le16toh()`、`le32toh()`、`le64toh()`；在 macOS 上使用 `OSReadLittleInt32()` / `OSReadLittleInt64()`。
