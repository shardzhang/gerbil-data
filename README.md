# gerbil-data
Data processing and sample generation for GERBIL recommender systems.

`gerbil-data` is the data component of **GERBIL**  
(**G**eneral **E**fficient **R**ecommender for **B**enchmarking, **I**nference, and **L**earning).

It works together with:

- `gerbil-train`: offline training and evaluation
- `gerbil-serving`: online inference and model serving

---

## Overview

`gerbil-data` is designed for building reliable and reusable data pipelines for recommender systems.

It is responsible for turning raw data into structured datasets, features, and samples that can be consumed by downstream training and serving components.

Typical workflow:

1. Ingest raw user / item / interaction data
2. Clean and normalize source tables
3. Build training and evaluation datasets
4. Generate features and samples
5. Export artifacts for `gerbil-train` and `gerbil-serving`

---

## Features

- Raw data ingestion and preprocessing
- Dataset construction for recommendation tasks
- Feature engineering and feature schema generation
- Training / validation / test split generation
- Positive / negative sample generation
- Reusable data pipeline components
- Export-ready artifacts for downstream systems

---

## Repository Structure

```text
gerbil-data/
├─ README.md
├─ pyproject.toml
├─ configs/
├─ scripts/
├─ gerbil_data/
│  ├─ cli/
│  ├─ ingestion/
│  ├─ preprocess/
│  ├─ dataset/
│  ├─ features/
│  ├─ sampling/
│  ├─ export/
│  └─ utils/
└─ tests/
```

---

## Installation

### Option 1: Install from source

```bash
git clone https://github.com/<your-org>/gerbil-data.git
cd gerbil-data
pip install -e .
```

### Option 2: Install dependencies manually

```bash
pip install -r requirements.txt
```

> Replace these commands according to your actual dependency management setup  
> such as `poetry`, `pdm`, or `uv`.

---

## Quick Start

### 1. Prepare raw data

Example raw input layout:

```text
data/raw/
├─ users.parquet
├─ items.parquet
└─ interactions.parquet
```

### 2. Run preprocessing

```bash
python -m gerbil_data.cli.preprocess \
  --config configs/pipeline/preprocess_ml1m.yaml
```

### 3. Generate features and samples

```bash
python -m gerbil_data.cli.build_dataset \
  --config configs/pipeline/build_ml1m.yaml
```

### 4. Export outputs

```bash
python -m gerbil_data.cli.export \
  --config configs/pipeline/export_ml1m.yaml
```

---

## What `gerbil-data` Produces

Typical outputs include:

- cleaned user / item / interaction tables
- train / validation / test datasets
- feature tables
- feature schema definitions
- vocabularies / ID mappings
- positive / negative training samples
- metadata for training and serving

Example output layout:

```text
data/processed/ml1m/
├─ train.parquet
├─ valid.parquet
├─ test.parquet
├─ user_features.parquet
├─ item_features.parquet
├─ feature_schema.yaml
├─ vocab/
│  ├─ user_id.json
│  └─ item_id.json
└─ metadata.json
```

---

## Supported Responsibilities

Depending on your implementation, `gerbil-data` can support:

- user / item / interaction table ingestion
- missing value handling
- filtering and deduplication
- feature extraction
- sequence generation
- time-based or random data split
- negative sampling
- dataset packaging for training
- export for online serving

---

## Data Pipeline

A typical recommendation data pipeline looks like this:

```text
raw data
  └─> preprocessing
       └─> feature engineering
            └─> sample generation
                 └─> dataset split
                      └─> export
```

---

## Integration with Other GERBIL Repositories

### `gerbil-train`

`gerbil-data` provides:

- processed training datasets
- evaluation datasets
- feature schemas
- sample generation outputs

These artifacts are consumed by `gerbil-train` for offline training and evaluation.

### `gerbil-serving`

`gerbil-data` can also provide:

- feature dictionaries
- ID mappings
- schema definitions
- metadata required by online inference services

These artifacts may be consumed by `gerbil-serving` to ensure consistent online feature handling.

---

## Configuration

`gerbil-data` is expected to be config-driven.

Recommended config layout:

```text
configs/
├─ source/
│  └─ ml1m.yaml
├─ feature/
│  └─ base.yaml
├─ sampling/
│  └─ pointwise.yaml
├─ split/
│  └─ random.yaml
├─ export/
│  └─ parquet.yaml
└─ pipeline/
   └─ build_ml1m.yaml
```

Example pipeline config:

```yaml
source: configs/source/ml1m.yaml
feature: configs/feature/base.yaml
sampling: configs/sampling/pointwise.yaml
split: configs/split/random.yaml
export: configs/export/parquet.yaml

output_dir: data/processed/ml1m
```

This design makes pipelines easier to reuse and compare.

---

## Example Use Cases

`gerbil-data` may be used for:

- CTR training data preparation
- Top-K ranking dataset generation
- sequential recommendation sequence building
- batch feature generation
- offline / online feature consistency preparation

---

## Development

### Run tests

```bash
pytest tests/
```

### Format code

```bash
black .
```

### Lint code

```bash
ruff check .
```

> Replace with your actual formatter / linter commands if needed.

---

## Example End-to-End Workflow

```text
gerbil-data  --->  gerbil-train  --->  gerbil-serving
   raw data         train/eval         online inference
   features         model export       request serving
   samples          checkpoints        deployment
```

---

## Roadmap

- [ ] Add unified source data reader
- [ ] Add configurable feature pipeline
- [ ] Add negative sampling strategies
- [ ] Add sequence dataset builders
- [ ] Add export adapters for training and serving
- [ ] Add data quality validation checks

---

## Contributing

Contributions are welcome.

If you want to contribute:

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Open a pull request

---

## License

Specify your license here, for example:

```text
Apache-2.0
```

---

## Citation

If you use this project in research or production, please cite or reference GERBIL appropriately.

```bibtex
@misc{gerbil-data,
  title={gerbil-data: Data processing and sample generation for recommender systems},
  author={GERBIL Contributors},
  year={2026}
}
```