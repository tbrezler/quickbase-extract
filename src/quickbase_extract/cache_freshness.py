"""Cache monitoring and freshness detection.

Inspects cached JSON files, checks their age, and identifies stale cache entries.
Works with both local and Lambda environments via CacheManager.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TypedDict

from quickbase_extract.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

# Cache freshness thresholds (in hours)
# Metadata rarely changes, so longer threshold is acceptable
DEFAULT_METADATA_STALE_HOURS = 168  # 7 days
# Data should be refreshed more frequently
DEFAULT_DATA_STALE_HOURS = 24  # 1 day
# General default when cache type isn't specified
DEFAULT_STALE_THRESHOLD_HOURS = 36


class CacheFileInfo(TypedDict):
    """Information about a cached file."""

    file: str
    path: Path
    size_bytes: int
    size_mb: float
    modified: datetime
    age_hours: float


class CacheSummary(TypedDict):
    """Summary statistics for cache directory."""

    cache_dir: str
    total_files: int
    total_size_mb: float
    oldest_file: str | None
    oldest_age_hours: float
    newest_file: str | None
    newest_age_hours: float


def get_cache_files(cache_root: Path | None = None) -> list[CacheFileInfo]:
    """Get all cached JSON files with their metadata.

    Scans report_data and report_metadata directories for JSON files.

    Args:
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        List of dicts with file path, size, and modification time.
        Sorted by age in descending order (oldest files first).

    Raises:
        FileNotFoundError: If cache directory doesn't exist.
        PermissionError: If cache directory is not readable.

    Example:
        >>> files = get_cache_files()
        >>> print(f"Oldest file: {files[0]['file']}, age: {files[0]['age_hours']}h")
    """
    if cache_root is None:
        cache_mgr = get_cache_manager()
        cache_root = cache_mgr.cache_root
    else:
        cache_root = Path(cache_root)

    if not cache_root.exists():
        raise FileNotFoundError(f"Cache directory does not exist: {cache_root}")

    if not cache_root.is_dir():
        raise NotADirectoryError(f"Cache path is not a directory: {cache_root}")

    try:
        files = []

        # Scan all JSON files in cache directory
        for json_file in cache_root.rglob("*.json"):
            try:
                stat = json_file.stat()
                age_hours = (time.time() - stat.st_mtime) / 3600

                files.append(
                    {
                        "file": str(json_file.relative_to(cache_root)),
                        "path": json_file,
                        "size_bytes": stat.st_size,
                        "size_mb": round(stat.st_size / (1024 * 1024), 2),
                        "modified": datetime.fromtimestamp(stat.st_mtime),
                        "age_hours": round(age_hours, 1),
                    }
                )
            except (OSError, ValueError) as e:
                # File might have been deleted or become inaccessible
                logger.warning(f"Failed to stat file {json_file}: {e}")
                continue

        # Sort by age descending (oldest first)
        return sorted(files, key=lambda x: x["age_hours"], reverse=True)

    except PermissionError as e:
        raise PermissionError(f"Cannot read cache directory {cache_root}: {e}") from e


def check_cache_freshness(
    threshold_hours: float = DEFAULT_STALE_THRESHOLD_HOURS,
    cache_root: Path | None = None,
) -> list[CacheFileInfo]:
    """Check for stale cache files.

    Args:
        threshold_hours: Files older than this are considered stale. Defaults to
            DEFAULT_STALE_THRESHOLD_HOURS (36 hours).
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        List of stale file info dicts, sorted by age descending (oldest first).
        Empty list if all files are fresh.

    Raises:
        FileNotFoundError: If cache directory doesn't exist.
        PermissionError: If cache directory is not readable.

    Example:
        >>> stale = check_cache_freshness(threshold_hours=24)
        >>> if stale:
        ...     print(f"Found {len(stale)} files older than 24 hours")
    """
    files = get_cache_files(cache_root=cache_root)
    stale = [f for f in files if f["age_hours"] > threshold_hours]

    if stale:
        logger.warning(
            f"Found {len(stale)} stale cache files (older than {threshold_hours}h). "
            f"Oldest: {stale[0]['file']} ({stale[0]['age_hours']}h)"
        )
    else:
        logger.info(f"All {len(files)} cache files are fresh (within {threshold_hours}h)")

    return stale


def get_cache_summary(cache_root: Path | None = None) -> CacheSummary:
    """Get a summary of the cache directory.

    Args:
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        Dict with total files, size, oldest/newest file info.

    Raises:
        FileNotFoundError: If cache directory doesn't exist.
        PermissionError: If cache directory is not readable.

    Example:
        >>> summary = get_cache_summary()
        >>> print(f"Cache: {summary['total_files']} files, {summary['total_size_mb']} MB")
        >>> print(f"Oldest: {summary['oldest_file']} ({summary['oldest_age_hours']}h)")
    """
    if cache_root is None:
        cache_mgr = get_cache_manager()
        cache_root = cache_mgr.cache_root
    else:
        cache_root = Path(cache_root)

    files = get_cache_files(cache_root=cache_root)

    if not files:
        return {
            "cache_dir": str(cache_root),
            "total_files": 0,
            "total_size_mb": 0.0,
            "oldest_file": None,
            "oldest_age_hours": 0.0,
            "newest_file": None,
            "newest_age_hours": 0.0,
        }

    total_size = sum(f["size_bytes"] for f in files)
    oldest = files[0]  # First item in descending age order
    newest = files[-1]  # Last item in descending age order

    return {
        "cache_dir": str(cache_root),
        "total_files": len(files),
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "oldest_file": oldest["file"],
        "oldest_age_hours": oldest["age_hours"],
        "newest_file": newest["file"],
        "newest_age_hours": newest["age_hours"],
    }
