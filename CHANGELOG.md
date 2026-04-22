# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2024-01-XX

### Breaking Changes

- **Removed `client.py` module**: Users must create Quickbase clients directly using `quickbase-api` package
  - Removed: `get_qb_client()` function
  - Migration: Replace `get_qb_client(realm, token)` with `quickbase_api.client(realm=realm, user_token=token)`

- **Removed `get_cache_manager()` singleton**: Users must create `CacheManager` instances explicitly
  - `cache_root` parameter is now required (no defaults)
  - Migration: Replace `get_cache_manager(cache_root)` with `CacheManager(cache_root=cache_root)`

- **Removed `refresh_all()` function**: Use `ensure_cache_freshness()` for cache management
  - Migration: Replace `refresh_all(client, configs)` with `ensure_cache_freshness(client, configs, cache_mgr)`

- **All functions now require `CacheManager` instance**: Changed from optional `cache_root` parameter
  - Affected functions: `get_data()`, `get_data_parallel()`, `load_data()`, `load_data_batch()`,
    `get_report_metadata()`, `get_report_metadata_parallel()`, `load_report_metadata()`,
    `load_report_metadata_batch()`
  - Migration: Pass `cache_mgr` instead of `cache_root=path`

### Added

- `CacheManager` now accepts `s3_prefix` parameter for flexible S3 path configuration
- `ensure_cache_freshness()` function in new `cache_orchestration` module for orchestrating cache freshness checks
- Independent cache refresh: metadata and data refresh separately based on their staleness
- `CacheManager.is_cache_empty()` method to check if cache directory is empty
- `CacheManager.get_cache_age_hours()` method to get age of oldest file in cache
- Environment-first cache path structure: `{project}/{env}/cache/` instead of `cache/{env}/`

### Changed

- Cache structure now follows pattern: `my_project/dev/cache/report_metadata/...`
  - Previously: `.quickbase-cache/dev/report_metadata/...`
- `ensure_cache_freshness()` refreshes only stale caches instead of always refreshing both
- `sync_from_s3_once()` now requires `cache_mgr` parameter instead of using singleton
- S3 sync operations now use `s3_prefix` for path construction instead of auto-adding environment

### Fixed

- Cache freshness checks now work independently for metadata vs data
- S3 sync no longer double-adds environment prefix to paths

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
