# pos_map.bin — Binary Format Specification

This file is produced by `PosMapSerDe.saveToBin()` (Scala) and consumed by
the C++ online inference service to load the feature vocabulary.

---

## Overview

The `.bin` file contains three sections concatenated:

1. **Header** — timestamp + pos_map entry count
2. **Pos-map entries** — one per frequent feature value
3. **Target-map entries** — one per training target

All multi-byte values are **little-endian** (written by
`LittleEndianDataOutputStream`).

---

## Section 1: Header

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 8 | `int64` | `timestamp` | Pipeline execution date as integer (`yyyyMMdd` format, e.g. `20260610`) |
| 8 | 4 | `int32` | `pos_map_size` | Number of pos-map entries (N) |

---

## Section 2: Pos-map Entries

The next `N` entries follow, each of fixed size **variable per entry**
(due to the UTF-8 string field).

### Per-entry layout

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 2 + M | `UTF` | `field_name` | Modified UTF-8 string (see note below). First 2 bytes = length M, then M bytes of UTF-8 data |
| 2+M | 4 | `int32` | `field_index` | Feature index (e.g. 2 for `user_age`) |
| 6+M | 4 | `int32` | `field_type` | Feature type: 1 = categorical, 0 = continuous |
| 10+M | 4 | `int32` | `dim` | Embedding dimension for this feature (max assigned position + 1) |
| 14+M | 8 | `int64` | `hash` | MurmurHash3_x64_128 output (`val1`) for this feature value |
| 22+M | 4 | `int32` | `pos` | Assigned embedding position index |
| 26+M | 8 | `float64` | `mean` | For continuous: mean of observed values. For categorical: 0.0 |
| 34+M | 8 | `float64` | `std` | For continuous: std of observed values. For categorical: 1.0 |

**Total per entry**: `34 + M` bytes, where `M` = UTF-8 byte length of `field_name`.

### UTF-8 string format

Written by Java's `DataOutputStream.writeUTF()` — modified UTF-8 with
2-byte length prefix (unsigned). This is NOT the same as standard
length-prefixed UTF-8. Key differences:

- Characters `\u0001`–`\u007F` → 1 byte (ASCII)
- Characters `\u0080`–`\u07FF` → 2 bytes
- Characters `\u0800`–`\uFFFF` → 3 bytes
- Null character `\u0000` → 2 bytes (`0xC0 0x80`)

### C++ reading code

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
    // len is in BIG-ENDIAN (that's how DataOutputStream.writeUTF encodes it)
    len = (len >> 8) | (len << 8);  // byteswap to native
    std::string s(len, '\0');
    in.read(&s[0], len);
    return s;
}

// Reading pos_map_size N, then:
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
    // all multi-byte values are LITTLE-ENDIAN
}
```

---

## Section 3: Target-map

After all N pos-map entries:

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `int32` | `target_map_size` | Number of target-map entries (K) |

Then K entries:

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `int32` | `raw_target` | Original target ID (item_id) |
| 4 | 4 | `int32` | `mapped_target` | Sequential target index (0-based) |

---

## Complete file layout

```
[timestamp:      int64   LE]   8 bytes
[pos_map_size:   int32   LE]   4 bytes
[pos_map_entry_0: ...  ]       variable
[pos_map_entry_1: ...  ]       variable
...
[pos_map_entry_N-1: ...]       variable
[target_map_size: int32  LE]   4 bytes
[target_map_entry_0: ... ]     8 bytes
[target_map_entry_1: ... ]     8 bytes
...
[target_map_entry_K-1:...]     8 bytes
```

---

## Example: Decoded output

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

## Notes

1. **Uniqueness**: Each `(field_index, hash)` pair appears at most once.
2. **Categorical mean/std**: Always `(0.0, 1.0)` — normalization is done at
   the model level, not in the feature encoder.
3. **Continuous mean/std**: Computed from training data — these are the
   population mean and standard deviation of the raw numerical values for
   that feature.
4. **dim**: The `dim` field is the maximum assigned position + 1 for the
   feature, i.e. the effective vocabulary/embedding size for that feature.
5. **Ordering**: Entries are written in iterator order (not guaranteed to be
   sorted, but typically grouped by `field_index` and sorted by `pos`).
6. **Byte endianness**: Use `le16toh()`, `le32toh()`, `le64toh()` on Linux,
   or `OSReadLittleInt32()` / `OSReadLittleInt64()` on macOS.
