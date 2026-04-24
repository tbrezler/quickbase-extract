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

# API operations with error handling
from quickbase_extract.api_handlers import QuickbaseOperationError, handle_delete, handle_query, handle_upsert

# Cache management
from quickbase_extract.cache_manager import CacheManager

# Cache orchestration
from quickbase_extract.cache_orchestration import ensure_cache_freshness

# Cache sync
from quickbase_extract.cache_sync import is_cache_synced, sync_from_s3_once

# Config
from quickbase_extract.config import ReportConfig

# Report data retrieval
from quickbase_extract.report_data import get_data, get_data_parallel, load_data, load_data_batch

# Report metadata
from quickbase_extract.report_metadata import (
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    load_report_metadata_batch,
)

# Utilities
from quickbase_extract.utils import normalize_name

__version__ = "0.2.0"

# Configure logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    # Version
    "__version__",
    # Cache management
    "CacheManager",
    "ensure_cache_freshness",
    "sync_from_s3_once",
    "is_cache_synced",
    # Config
    "ReportConfig",
    # API operations
    "QuickbaseOperationError",
    "handle_delete",
    "handle_query",
    "handle_upsert",
    # Report metadata
    "get_report_metadata",
    "get_report_metadata_parallel",
    "load_report_metadata",
    "load_report_metadata_batch",
    # Report data
    "get_data",
    "get_data_parallel",
    "load_data",
    "load_data_batch",
    # Utilities
    "normalize_name",
]
