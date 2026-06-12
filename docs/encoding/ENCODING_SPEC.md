# ML-1M Feature Encoding Specification

Reference implementation: Scala Spark pipeline (`featurizer/` package).
Target platform: C++ online inference service.

This document defines every computation that the C++ featurizer must reproduce
so that online encoding is **bit-exact identical** to offline training data.

---

## 1. Feature Registry

Features are defined in `features.yaml` (see [features.yaml](features.yaml)).

### Schema

```yaml
pkg: featurizer.ml1m          # Scala package prefix (irrelevant for C++)
features:
  - name: <string>             # feature name, used as TFRecord field suffix
    index: <int>               # unique numeric feature index
    type: <int>                # 1 = categorical, 0 = continuous
    class: <string>            # Scala class name (irrelevant for C++)
    enabled: <bool>            # true = active
cross_features:
  - name: <string>
    index: <int>
    depends: [<name>, ...]     # list of categorical feature names to cross
    enabled: <bool>
```

### Type semantics

| type | Meaning | Encoding path |
|------|---------|---------------|
| 1    | Categorical (discrete) | Hash(f_index \|\| feature_id) → pos_map lookup → embedding index |
| 0    | Continuous (numerical) | feature_id used directly as position (no hash) → pos_map lookup → embedding index |

---

## 2. MurmurHash3 — Cross-language Contract

### Algorithm

`MurmurHash3_x64_128` (128-bit, x64 architecture variant).

- **Reference (Java)**: `utils.MurmurHash3.murmurhash3_x64_128()`
  ([source](../../src/main/java/utils/MurmurHash3.java))
- **Seed**: `0x3c074a61` (constant, defined in `RawFeature.SEED`)
- **Output**: 128-bit hash split into two 64-bit values `(val1, val2)`
- **Used value**: `val1` (the first 64 bits of the 128-bit hash)
- **Modulo reduction**: `hash % dim`, if negative, add `dim`
  ```java
  long hash = p.val1 % dim;
  if (hash < 0) hash += dim;
  ```

### Input key format

All feature hashing uses a **binary key** constructed as:

```
[f_index: 4 bytes LE] [feature_id: 8 bytes LE]
```

Total key length = **12 bytes**.

- `f_index`: 32-bit signed integer in **little-endian** byte order
- `feature_id`: 64-bit signed long in **little-endian** byte order

### C++ validation

The Java implementation used in this project is based on
[yonik/java_util](https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java)
which is a direct port of Austin Appleby's final C++ version
([smhasher](https://github.com/aappleby/smhasher)).

The C++ implementation from `smhasher` produces bit-identical results:

```cpp
// C++ — expected to match Java exactly
#include "MurmurHash3.h"

uint64_t val1, val2;
MurmurHash3_x64_128(key, len, seed, &val1, &val2);
// val1 is the first uint64_t written by the C++ function
```

---

## 3. Feature ID Computation

Each feature has a `parse(sample)` method that produces a list of
`(raw_string, feature_id, value)` triples. These rules must be replicated
exactly in C++ for every enabled feature.

### 3.1 Simple integer features

Feature ID is the raw value parsed as integer.

| Feature | Index | Field | Rule |
|---------|-------|-------|------|
| `user_age` | 2 | `sample.age` | `toInt` |
| `user_occupation` | 4 | `sample.occupation` | `toInt` |
| `user_movie_rate` | 301 | `(itemId, rating)` pairs | `itemId.toInt`, value = `rating.toFloat` |
| `user_movie_rate_1day` | 302 | same | same |
| `user_movie_rate_3day` | 303 | same | same |
| `user_movie_rate_7day` | 304 | same | same |
| `user_movie_rate_15day` | 305 | same | same |

### 3.2 Bucketed integer features

Raw value is mapped to a bucket ID via match rules.

#### UserRateStd (index=6, 7, 8, 9)

```scala
val buck = sample.user_rate_std match {
  case 0.0F       => 1
  case x if x < 1 => 2
  case _          => 3
}
```

#### UserMovieRateCnt (index=10, 11, 12, 13)

```scala
val buck = sample.user_rate_cnt match {
  case 0          => 1
  case x if x < 5 => 2
  case _          => 3
}
```

#### UserAvgRate (index=14, 15, 16, 17)

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

#### MovieRateCount (index=104)

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

#### MovieAvgRate (index=105)

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

#### MovieGenreCnt (index=106)

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

#### MovieHotRank (index=107)

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

#### MoviePublishYear (index=108)

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

#### ContextTimeArea (index=202)

```scala
val buck = sample.time_hour / 4  // integer division: 0-5
```

#### ContextIsWeekend (index=204)

```scala
val buck = sample.week_day match {
  case 6 | 7 => 1  // Saturday/Sunday
  case _     => 0
}
```

### 3.3 Enum-mapped feature

#### UserGender (index=3)

```scala
val buck = sample.gender match {
  case "M" => 1
  case "F" => 2
  case _   => 0
}
```

### 3.4 String-hashed features (MurmurHash3 of raw string)

For features where the raw value is an arbitrary string, `feature_id` is
computed as:

```scala
val p = new MurmurHash3.LongPair()
MurmurHash3.murmurhash3_x64_128(string.getBytes("UTF-8"), 0, string.length, SEED, p)
feature_id = p.val1  // use the first 64-bit word
```

Key = `UTF-8 bytes` of the string (not Java's internal char[] representation).

| Feature | Index | String source |
|---------|-------|---------------|
| `movie_title` | 102 | Each title word (space-split, after removing trailing `(year)`) |
| `movie_genres` | 103 | Each genre string (lowercased) |
| `user_genres_rate` | 306 | genre name |
| `user_genres_rate_1day` | 307 | genre name |
| `user_genres_rate_3day` | 308 | genre name |
| `user_genres_rate_7day` | 309 | genre name |
| `user_genres_rate_15day` | 310 | genre name |
| `user_genres_rate_cnts` | 312 | genre name |
| `user_genres_rate_cnt_1days` | 313 | genre name |
| `user_genres_rate_cnt_3days` | 314 | genre name |
| `user_genres_rate_cnt_7days` | 315 | genre name |
| `user_genres_rate_cnt_15days` | 316 | genre name |
| `user_top3_genres` | 317 | genre name |
| `user_watch_same_genre` | 351 | See §3.6 |

### 3.5 Continuous features

`feature_id` = `1L` (always 1). The numerical value is stored in `value_list`.

| Feature | Index | Value source |
|---------|-------|--------------|
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

### 3.6 Same-genre features (index 351–357)

These features compute overlap between the current movie's genres and the user's
recently watched movie genres. Implementation details are in
`ML1MUserWatchSameGenre.scala`.

> **Note for C++ implementer**: These require access to the user's recent movie
> watch history (genre-level), which must be provided as part of the request
> context.

### 3.7 Top-3 genres (index 317)

`sample.user_top3_genres` is pre-computed during data preparation as the top-3
genres by rating count from the user's history. Each genre name is hashed
via MurmurHash3 (same as §3.4).

---

## 4. Categorical Feature Encoding

For each categorical feature value in `feature_list`:

### Step 1: Hash

```scala
def computeHash(fea: Long, dim: Long): Long = {
  val bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
  bb.putInt(0, f_index)     // 4 bytes, LE
  bb.putLong(4, fea)         // 8 bytes, LE
  val p = new LongPair()
  MurmurHash3.murmurhash3_x64_128(bb.array(), 0, 12, SEED, p)
  var hash = p.val1 % dim
  if (hash < 0) hash += dim
  hash
}
```

### Step 2: Pos-map lookup

```scala
if (pos_map.contains((f_index, hash))) {
  val pos = pos_map((f_index, hash))  // embedding index
  // emit (raw, pos, value)
}
```

Values not in `pos_map` (low frequency in training) are **silently dropped**.

### Step 3: Output triple

Each surviving feature value emits a triple:

```
(raw: String, index: Long, value: Float)
```

Sentinel entry at position 0 (always present):
```
raw = "R:", index = 0L, value = 1.0F
```

---

## 5. Continuous Feature Encoding

For each continuous feature value:

### Step 1: Position

Position = `feature_list(i)` (always `1L` for single-valued continuous).

With pos-map lookup:
```scala
if (pos_map.contains((f_index, fea))) {
  val pos = pos_map((f_index, fea)).toLong
  // emit
}
```

### Step 2: Value

Value = `value_list(i)` (the raw numerical value, no normalization at encoding
time).

> Normalization (mean/std) is handled at the model level (TF/PyTorch), not
> in the feature encoder.

---

## 6. Cross Feature Encoding

For cross features, the Cartesian product of all constituent features is
enumerated.

### Hash key

```
[f_index_1: 4B LE] [feature_id_1: 8B LE] [f_index_2: 4B LE] [feature_id_2: 8B LE] ...
```

Total length = `(4 + 8) * N` bytes, where N = number of constituent features.

### Position

Same pos_map lookup as categorical features, using the cross feature's
`f_index`.

---

## 7. Output Format

The offline pipeline produces three columns per feature in TFRecord:

| Column | Type | Content |
|--------|------|---------|
| `{name}_raw` | `bytes` list | Raw value strings (human-readable) |
| `{name}_index` | `int64` list | Embedding position indices |
| `{name}_value` | `float` list | Weights / numerical values |

### Target

Stored as a single `float` value. The target is the `item_id` of the
positive sample (multi-class classification).

---

## 8. Hash Dimension

`max_dim = 1L << 60` (2^60), defined in `ML1MPipeline.max_dim`.

This is used only for computing hash values during pos-map building. After
pos-map build, the actual embedding dimension per feature is determined by
`pos_dim` (the number of distinct frequent values for that feature).

---

## 9. C++ Reference Implementation Checklist

- [ ] MurmurHash3_x64_128: seed=`0x3c074a61`, little-endian 12-byte key
- [ ] `val1` from 128-bit output, modulo `dim`, negative fixup
- [ ] Feature ID extraction for all 44 enabled features (see §3)
- [ ] Bucketing rules for all categorical features
- [ ] String hashing via UTF-8 → MurmurHash3 for genre/movie-title features
- [ ] pos_map.bin loading (see [pos_map_bin_format.md](pos_map_bin_format.md))
- [ ] pos_map lookup: `(f_index, hash) → pos`
- [ ] Cross product enumeration for cross features
- [ ] Sentinel `(R:, 0L, 1.0F)` at position 0 of every feature

---

## Appendix: Features Quick Reference

| Index | Name | Type | Feature ID source |
|-------|------|------|-------------------|
| 2 | user_age | categorical | `age.toInt` (1/18/25/35/45/50/56) |
| 3 | user_gender | categorical | enum(M=1, F=2, else=0) |
| 4 | user_occupation | categorical | `occupation.toInt` |
| 6 | user_rate_std | categorical | bucket(0→1, <1→2, else→3) |
| 7 | user_rate_std_7day | categorical | same as 6 |
| 8 | user_rate_std_15day | categorical | same as 6 |
| 9 | user_rate_std_30day | categorical | same as 6 |
| 10 | user_movie_rate_cnt | categorical | bucket(0→1, <5→2, else→3) |
| 11 | user_movie_rate_cnt_7day | categorical | same as 10 |
| 12 | user_movie_rate_cnt_15day | categorical | same as 10 |
| 13 | user_movie_rate_cnt_30day | categorical | same as 10 |
| 14 | user_avg_rate | categorical | bucket(<1→1, <2→2, <3→3, <3.5→4, <4→5, else→6) |
| 15 | user_avg_rate_7day | categorical | same as 14 |
| 16 | user_avg_rate_15day | categorical | same as 14 |
| 17 | user_avg_rate_30day | categorical | same as 14 |
| 18 | user_rate_std_continue | continuous | id=1L, value=raw float |
| 19 | user_rate_std_7day_continue | continuous | id=1L, value=raw float |
| 21 | user_rate_std_15day_continue | continuous | id=1L, value=raw float |
| 22 | user_rate_std_30day_continue | continuous | id=1L, value=raw float |
| 23 | user_avg_rate_continue | continuous | id=1L, value=raw float |
| 24 | user_avg_rate_7day_continue | continuous | id=1L, value=raw float |
| 25 | user_avg_rate_15day_continue | continuous | id=1L, value=raw float |
| 26 | user_avg_rate_30day_continue | continuous | id=1L, value=raw float |
| 102 | movie_title | categorical | hash(each word in title sans year) |
| 103 | movie_genres | categorical | hash(each genre, lowercased) |
| 104 | movie_rate_count | categorical | bucket (11-level) |
| 105 | movie_avg_rate | categorical | bucket (9-level) |
| 106 | movie_genre_cnt | categorical | bucket (8-level) |
| 107 | item_hot_rank | categorical | bucket (7-level) |
| 108 | movie_publish_year | categorical | bucket (6-level) |
| 109 | movie_avg_rate_continue | continuous | id=1L, value=raw float |
| 201 | context_time_hour | categorical | `time_hour.toInt` (0-23) |
| 202 | context_time_area | categorical | `time_hour / 4` (0-5) |
| 203 | context_time_week | categorical | `week_day` (1-7) |
| 204 | context_is_weekend | categorical | enum(weekend→1, else→0) |
| 301-305 | user_movie_rate* | categorical | `itemId.toInt` each, value=rating |
| 306-310 | user_genres_rate* | categorical | hash(genre name) each, value=avg rating |
| 312-316 | user_genres_rate_cnt* | categorical | hash(genre name) each, value=count |
| 317 | user_top3_genres | categorical | hash(genre name) each |
| 351-355 | user_watch_same_genre* | categorical | hash(item ID) each |
| 356 | user_same_genre_avg_rate | categorical | bucket (6-level) |
| 357 | user_same_genre_avg_rate_continue | continuous | id=1L, value=raw float |
