# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-22

### Added

- `ensure_cache_freshness()` function in new `cache_orchestration` module for orchestrating cache freshness checks
- Independent cache refresh: metadata and data refresh separately based on their staleness
- `METADATA_STALE_HOURS`, `DATA_STALE_HOURS`, `FORCE_CACHE_REFRESH` environment variables
- Cache freshness management documentation and examples
- `CacheManager.is_cache_empty()` method to check if cache directory is empty
- `CacheManager.get_cache_age_hours()` method to get age of oldest file in cache
- `CacheManager` now accepts `s3_prefix` parameter for flexible S3 path configuration
- Environment-first cache path structure: `{project}/{env}/cache/` instead of `cache/{env}/`

### Changed

- **All functions now require `CacheManager` instance**: Changed from optional `cache_root` parameter
- Cache structure now follows pattern: `my_project/dev/cache/report_metadata/...`
  - Previously: `.quickbase-cache/dev/report_metadata/...`
- `ensure_cache_freshness()` refreshes only stale caches instead of always refreshing both
- `sync_from_s3_once()` now requires `cache_mgr` parameter instead of using singleton
- `sync_from_s3_once()` now supports `FORCE_CACHE_REFRESH` environment variable
- S3 sync operations now use `s3_prefix` for path construction instead of auto-adding environment
- CacheManager now provides cache inspection methods for both metadata and data directories

### Removed

- `cache_freshness.py` module — functionality consolidated into `cache_manager.py` (use `ensure_cache_freshness()` instead)
- `check_cache_freshness()`, `get_cache_files()`, `get_cache_summary()` functions — use `CacheManager` methods directly or `ensure_cache_freshness()` for orchestration
- `refresh_all()` function**: Use `ensure_cache_freshness()` for cache management
- `client.py` module**: Users must create Quickbase clients directly using `quickbase-api` package
- `get_cache_manager()` singleton**: Users must create `CacheManager` instances explicitly

### Fixed

- Cache freshness checks now work independently for metadata vs data
- S3 sync no longer double-adds environment prefix to paths

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
