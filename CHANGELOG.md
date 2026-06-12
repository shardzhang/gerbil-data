# Changelog

All notable changes to this project will be documented in this file.

## [1.0.1] - 2026-06-12

### Added

- Bilingual READMEs (English + Chinese) for `dag/`, `tools/`, `docs/dataset/ml_1m/`

### Changed

- Root README architecture tree now includes `dag/` directory
- Root README features section rewritten with layered module-by-module architecture description

## [1.0.0] - 2026-06-10

### Added

- Initial release of gerbil-data
- ML-1M data cleaning pipeline (`ML1MCleanSample`)
- User behavior sequence extraction with configurable time windows (`ML1MUserMovieRate`)
- Movie statistical feature computation (`ML1MMovieStatFeature`)
- Feature joining and training sample generation (`ML1MJoinSample`)
- Feature encoding framework with categorical, continuous, and cross features
- TFRecord (TensorFlow Example) output format
- Parquet columnar output format
- Feature vocabulary management (JSON and binary position maps)
- Multiple prediction targets: multi-class, binary, and regression
- Comprehensive test suite for TFRecord serde operations
