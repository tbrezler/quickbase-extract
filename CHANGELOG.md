# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-22

### Added

- `ensure_cache_freshness()` function for automatic cache freshness checking and refresh orchestration
- Separate configurable thresholds for metadata vs. data cache staleness
- `METADATA_STALE_HOURS`, `DATA_STALE_HOURS`, `FORCE_CACHE_REFRESH` environment variables
- `is_cache_empty()` and `get_cache_age_hours()` methods to CacheManager (support both metadata and data cache types)
- Force refresh capability via `FORCE_CACHE_REFRESH=true` environment variable
- Cache freshness management documentation and examples

### Changed

- `sync_from_s3_once()` now supports `FORCE_CACHE_REFRESH` environment variable
- CacheManager now provides cache inspection methods for both metadata and data directories

### Removed

- `cache_freshness.py` module — functionality consolidated into `cache_manager.py` (use `ensure_cache_freshness()` instead)
- `check_cache_freshness()`, `get_cache_files()`, `get_cache_summary()` functions — use `CacheManager` methods directly or `ensure_cache_freshness()` for orchestration

### Deprecated

- Importing cache freshness tools from separate module — import `ensure_cache_freshness` from `cache_manager` instead

## [0.1.0] - 2026-04-19

### Added

- Initial release
- Parallel report fetching with configurable worker threads
- Local and S3-backed caching for reduced API calls
- Automatic retry logic with exponential backoff for rate limits
- AWS Lambda support with S3 cache synchronization
- Cache monitoring tools for freshness checks
- Type hints and TypedDict support for better IDE integration
- Comprehensive error handling and detailed logging
- Data transformation with field ID to label conversion
- Support for multiple Quickbase applications

### Features

- **Parallel Processing**: Fetch multiple reports concurrently for improved performance
- **Smart Caching**: Local and S3-backed caching to minimize API calls
- **Automatic Retries**: Built-in retry logic with exponential backoff for rate limits
- **Lambda Ready**: First-class support for AWS Lambda with S3 cache sync
- **Type Safe**: Full type hints with TypedDict for better IDE support
- **Cache Monitoring**: Tools to check cache freshness and manage stale data
- **Robust Error Handling**: Comprehensive error handling with detailed logging
- **Data Transformation**: Automatically converts field IDs to human-readable labels
