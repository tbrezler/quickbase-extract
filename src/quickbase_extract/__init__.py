"""Quickbase Extract - Extract and cache Quickbase report data.

A Python package for efficiently retrieving, transforming, and caching data
from Quickbase reports with built-in error handling, retry logic, and S3 support
for Lambda environments.

Quick Start:
    >>> import quickbase_api
    >>> from quickbase_extract import CacheManager, load_report_metadata_batch
    >>> from quickbase_extract.cache_orchestration import ensure_cache_freshness
    >>> from quickbase_extract.report_data import get_data_parallel
    >>>
    >>> # Initialize client
    >>> client = quickbase_api.client(realm="example.quickbase.com", user_token="...")
    >>>
    >>> # Initialize cache manager
    >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
    >>>
    >>> # Ensure cache is fresh
    >>> ensure_cache_freshness(client, report_configs, cache_mgr)
    >>>
    >>> # Load metadata and fetch data
    >>> metadata = load_report_metadata_batch(report_configs, cache_mgr)
    >>> data = get_data_parallel(client, metadata, ["report1", "report2"], cache_mgr, cache=True)
"""

import logging
from importlib.metadata import version

# API operations with error handling
from quickbase_extract.api_handlers import QuickbaseOperationError, handle_delete, handle_query, handle_upsert

# Cache management
from quickbase_extract.cache_manager import CacheManager

# Cache orchestration
from quickbase_extract.cache_orchestration import ensure_cache_freshness

# Cache sync
from quickbase_extract.cache_sync import complete_cache_refresh, is_cache_synced, sync_from_s3_once

# Config
from quickbase_extract.config import ReportConfig

# Report data
from quickbase_extract.report_data import get_data, get_data_parallel, load_data, load_data_batch

# Report metadata
from quickbase_extract.report_metadata import (
    filter_metadata_by_table,
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    load_report_metadata_batch,
)

# Utilities
from quickbase_extract.utils import normalize_name

__version__ = version("quickbase-extract")

# Configure logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "CacheManager",
    "QuickbaseOperationError",
    "ReportConfig",
    "__version__",
    "complete_cache_refresh",
    "ensure_cache_freshness",
    "filter_metadata_by_table",
    "get_data",
    "get_data_parallel",
    "get_report_metadata",
    "get_report_metadata_parallel",
    "handle_delete",
    "handle_query",
    "handle_upsert",
    "is_cache_synced",
    "load_data",
    "load_data_batch",
    "load_report_metadata",
    "load_report_metadata_batch",
    "normalize_name",
    "sync_from_s3_once",
]
