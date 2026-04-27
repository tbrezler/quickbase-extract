# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-04-27

### Fixed

- `complete_cache_refresh()` function now properly exported from `quickbase_extract` package for use in Lambda handlers

## [0.3.0] - 2026-04-27

### Added

- `complete_cache_refresh()` function in `cache_sync` module for selective cache refresh in development
- Support for granular cache refresh control:
  - `force_all=True`: Refresh both metadata and data caches
  - `force_metadata=True`: Refresh only metadata cache
  - `force_data=True`: Refresh only data cache
- Cache refresh workflow: clear /tmp → fetch fresh from Quickbase → update S3 → re-sync to /tmp
- Manual development toggles in Lambda handlers for testing cache refresh without API changes
- Comprehensive test suite for complete cache refresh functionality

### Changed

- Lambda workers can now force complete cache refresh for development/debugging by toggling code variables
- Cache refresh strategy clarified: selective refresh based on what changed (metadata vs data)

### Fixed

- Dev workflow now supports forcing cache refresh without modifying Lambda event API
- Ensures /tmp and S3 are synchronized after cache refresh operations

## [0.2.1] - 2026-04-25

### Fixed

- `sync_from_s3()` now preserves S3 `LastModified` timestamps via `os.utime()`, so `get_cache_age_hours()` returns accurate ages after S3 restore (previously always returned ~0 on Lambda cold start)
- Renamed misleading `should_sync` variable to `already_synced` in `sync_from_s3_once()` for clarity

### Removed

- `FORCE_CACHE_REFRESH` environment variable support from `sync_from_s3_once()` — use `force=True` parameter instead

## [0.2.0] - 2026-04-22

### Added

- `ensure_cache_freshness()` function in new `cache_orchestration` module for orchestrating cache freshness checks
- Independent cache refresh: metadata and data refresh separately based on their staleness
- `METADATA_STALE_HOURS`, `DATA_STALE_HOURS`, `FORCE_CACHE_REFRESH` environment variables
- `cache_all_data` parameter in `ensure_cache_freshness()` to cache all reports regardless of subset
- `report_configs_all` and `report_configs_to_cache` parameters to separate metadata refresh from data caching
- Cache freshness management documentation and examples
- `CacheManager.is_cache_empty()` method to check if cache directory is empty
- `CacheManager.get_cache_age_hours()` method to get age of oldest file in cache
- `CacheManager` now accepts `s3_prefix` parameter for flexible S3 path configuration
- Environment-first cache path structure: `{project}/{env}/cache/` instead of `cache/{env}/`
- `ReportConfig` NamedTuple in new `config.py` module for type-safe report identification
- `_extract_report_names()` helper function in `report_data.py` to reduce code duplication
- `_replace_ask_placeholders()` function for dynamic filter manipulation with runtime ask_values
- Support for per-report ask_values in `get_data_parallel()` for dynamic filtering
- Multiple organization strategies for report configs (by app, by function, by environment, by tags)
- Environment-specific caching strategies (dev/staging/prod with different subsets)

### Changed

- **All functions now require `CacheManager` instance**: Changed from optional `cache_root` parameter
- Cache structure now follows pattern: `my_project/dev/cache/report_metadata/...`
  - Previously: `.quickbase-cache/dev/report_metadata/...`
- `ensure_cache_freshness()` refreshes only stale caches instead of always refreshing both
- `sync_from_s3_once()` now requires `cache_mgr` parameter instead of using singleton
- `sync_from_s3_once()` now supports `FORCE_CACHE_REFRESH` environment variable
- S3 sync operations now use `s3_prefix` for path construction instead of auto-adding environment
- CacheManager now provides cache inspection methods for both metadata and data directories
- **All functions now require `ReportConfig` instances instead of dicts**
- `fetch_report_metadata_api()` now returns only essential fields (no full `report` object)

### Removed

- `cache_freshness.py` module — functionality consolidated into `cache_manager.py` (use `ensure_cache_freshness()` instead)
- `check_cache_freshness()`, `get_cache_files()`, `get_cache_summary()` functions — use `CacheManager` methods directly or `ensure_cache_freshness()` for orchestration
- `refresh_all()` function**: Use `ensure_cache_freshness()` for cache management
- `client.py` module**: Users must create Quickbase clients directly using `quickbase-api` package
- `get_cache_manager()` singleton**: Users must create `CacheManager` instances explicitly
- `find_report()` function from utils - no longer needed with `ReportConfig`
- Dict-based report config format - all configs must use `ReportConfig` NamedTuple
- Nested `report` object from metadata - simplified structure

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
