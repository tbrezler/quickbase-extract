# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### Added

- `ReportConfig` NamedTuple in new `config.py` module for type-safe report identification
- `_extract_report_names()` helper function in `report_data.py` to reduce code duplication
- `_replace_ask_placeholders()` function for dynamic filter manipulation with runtime ask_values
- Support for per-report ask_values in `get_data_parallel()` for dynamic filtering
- `cache_all_data` parameter in `ensure_cache_freshness()` to cache all reports regardless of subset
- `report_configs_all` and `report_configs_to_cache` parameters to separate metadata refresh from data caching
- Comprehensive documentation on organizing report configs with dict-based strategies (Option 2)
- Multiple organization strategies for report configs (by app, by function, by environment, by tags)
- Environment-specific caching strategies (dev/staging/prod with different subsets)
- Performance optimization guide with benchmarking examples
- Advanced usage patterns for dynamic filters, batch processing, and data pipelines

### Changed

- **BREAKING: All functions now require `ReportConfig` instances instead of dicts**
  - `get_report_metadata(client, cache_manager, report_config)` - parameter order changed, now takes `ReportConfig`
  - `get_data(client, cache_manager, report_config, report_metadata)` - now uses `ReportConfig`
  - `load_data(cache_manager, report_config, report_metadata)` - now uses `ReportConfig`
  - `get_data_parallel(client, cache_manager, report_configs, report_metadata)` - now takes list of `ReportConfig`
  - `load_data_batch(cache_manager, report_configs, report_metadata)` - now takes list of `ReportConfig`

- **BREAKING: Metadata structure simplified**
  - Removed nested `"report"` object from cached metadata
  - Moved `sort_by` and `group_by` to top level (previously `report.query.sortBy`, etc.)
  - Metadata now: `{"table_id", "field_label", "fields", "filter", "sort_by", "group_by", "app_name", "table_name", "report_name"}`

- **BREAKING: `ensure_cache_freshness()` signature changed**
  - `report_configs_all` - all reports for metadata refresh (was `report_configs`)
  - `report_configs_to_cache` - optional subset for data caching (was `report_descriptions`)
  - Added `cache_all_data` parameter for environment-specific caching
  - Removed `report_descriptions` (now use `report_configs_to_cache`)

- **BREAKING: Metadata is now keyed by `ReportConfig` instead of description string**
  - Old: `metadata["customers"]` (string key)
  - New: `metadata[ReportConfig(...)]` (NamedTuple key)

- `fetch_report_metadata_api()` now returns only essential fields (no full `report` object)
  - Returns: `{"table_id", "field_label", "fields", "filter", "sort_by", "group_by"}`

- `ensure_cache_freshness()` now checks metadata and data independently
  - Only refreshes caches that are stale (more efficient)
  - Metadata refresh can complete even if data refresh fails

- Improved error handling in `_replace_ask_placeholders()` with clearer validation messages

- Updated all examples and documentation to use `ReportConfig`

- Lambda handler examples now show proper cache management with `sync_from_s3_once()`

### Removed

- `find_report()` function from utils - no longer needed with `ReportConfig`
- Dict-based report config format - all configs must use `ReportConfig` NamedTuple
- Nested `report` object from metadata - simplified structure
- `report_descriptions` parameter from `ensure_cache_freshness()` - use `report_configs_to_cache`

### Fixed

- Cache freshness checks now work independently for metadata vs data
- Ask placeholder validation now catches both missing and unused values
- Type hints now correctly reflect `ReportConfig` usage throughout
- Metadata dict keys are now consistent (ReportConfig instances)

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
