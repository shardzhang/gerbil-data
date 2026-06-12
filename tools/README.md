# Tools — ML-1M Feature Encoding

## C++ Featurizer (`cpp_featurizer/`)

C++ online inference feature encoder, bit-exact with the Scala training pipeline.

```bash
cd tools/cpp_featurizer
mkdir build && cd build
cmake .. && make
./ml1m_featurizer [pos_map_path]
```

- `pos_map_path` is optional; if provided, loads `pos_map.bin` for embedding position lookup
- Outputs CSV to stdout: `feature_name,field_index,field_type,raw,feature_id,hash,value`
- Includes 58 regular features + 22 cross features (all cross features registered but disabled in production config)

### Files

- `types.h` — Shared types, constants, utility functions
- `sample.h` — `ML1MSample` data structure (58 fields)
- `pos_map.h` — `pos_map.bin` binary loader (supports Java `writeUTF` format)
- `MurmurHash3.h/.cpp` — smhasher implementation (bit-exact with Java yonik MurmurHash3)
- `feature.h` — `AbstractFeature` base class + `CrossFeature` + all 58 concrete implementations
- `featurizer.h` — Orchestrator (register, parse, encode)
- `main.cpp` — Entry point, test sample, CSV output

## Scala Golden Data Generator (`DumpGoldenData.scala`)

Standalone golden data generator for verifying C++ output.

```bash
# 1. Compile MurmurHash3.java (from main project)
javac -d target/classes src/main/java/utils/MurmurHash3.java

# 2. Compile & run
scalac -cp target/classes tools/DumpGoldenData.scala -d tools/
scala -cp target/classes:tools DumpGoldenData > /tmp/output.csv
```

Output format matches the C++ featurizer CSV exactly.

## pos_map.bin Generator (`DumpPosMapBin.scala`)

Generates a small test `pos_map.bin` for validating the C++ binary reader.

```bash
# After compiling MurmurHash3.java:
scalac -cp target/classes tools/DumpPosMapBin.scala -d tools/
scala -cp target/classes:tools DumpPosMapBin
```

Output: `/tmp/test_pos_map.bin`

### Binary format (matching production `PosMapSerDe`)

| Section | Fields | Endianness |
|---------|--------|------------|
| Header | `timestamp` (int64), `pos_map_size` (int32) | Little-endian |
| Pos-map entries | `field_name` (writeUTF), `field_index` (int32), `field_type` (int32), `dim` (int32), `hash` (int64), `pos` (int32), `mean` (float64), `std` (float64) | writeUTF = big-endian length; all others = little-endian |
| Target-map | `target_map_size` (int32), then K pairs of `raw_target` (int32) / `mapped_target` (int32) | Little-endian |

## Validation

To compare C++ and Scala outputs:

```bash
# Generate both
scala -cp target/classes:tools DumpGoldenData > /tmp/scala_output.csv
./tools/cpp_featurizer/build/ml1m_featurizer > /tmp/cpp_output.csv

# Diff (hash columns only — float formatting may differ)
diff <(awk -F, '{print $1,$2,$3,$5,$6}' /tmp/scala_output.csv) \
     <(awk -F, '{print $1,$2,$3,$5,$6}' /tmp/cpp_output.csv)
```
