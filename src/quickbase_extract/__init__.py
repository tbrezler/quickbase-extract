"""Quickbase Extract - Extract and cache Quickbase report data.

A Python package for efficiently retrieving, transforming, and caching data
from Quickbase reports with built-in error handling, retry logic, and S3 support
for Lambda environments.
"""

import logging

# API operations with error handling
from quickbase_extract.api_handlers import QuickbaseOperationError, handle_delete, handle_query, handle_upsert

# Cache monitoring
from quickbase_extract.cache_freshness import check_cache_freshness, get_cache_files, get_cache_summary

# Cache management
from quickbase_extract.cache_manager import CacheManager, get_cache_manager
from quickbase_extract.cache_sync import sync_from_s3_once

# Client
from quickbase_extract.client import get_qb_client

# Report data retrieval
from quickbase_extract.report_data import get_data, get_data_parallel, load_data

# Report metadata
from quickbase_extract.report_metadata import (
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    refresh_all,
)

# Utilities
from quickbase_extract.utils import find_report, normalize_name

__version__ = "0.1.0"

# Configure logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    # Version
    "__version__",
    # Client
    "get_qb_client",
    # Cache management
    "CacheManager",
    "get_cache_manager",
    "sync_from_s3_once",
    # Cache monitoring
    "check_cache_freshness",
    "get_cache_files",
    "get_cache_summary",
    # API operations
    "QuickbaseOperationError",
    "handle_delete",
    "handle_query",
    "handle_upsert",
    # Report metadata
    "get_report_metadata",
    "get_report_metadata_parallel",
    "load_report_metadata",
    "refresh_all",
    # Report data
    "get_data",
    "get_data_parallel",
    "load_data",
    # Utilities
    "find_report",
    "normalize_name",
]
