"""Cache monitoring and freshness detection.

Inspects cached JSON files, checks their age, and identifies stale cache entries.
Works with both local and Lambda environments via CacheManager.
"""

import logging
import time
from datetime import datetime
from pathlib import Path

from quickbase_extract.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

# Cache is considered stale if older than this many hours
DEFAULT_STALE_THRESHOLD_HOURS = 36


def get_cache_files(cache_root: Path = None) -> list[dict]:
    """Get all cached JSON files with their metadata.

    Scans report_data and report_metadata directories for JSON files.

    Args:
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        List of dicts with file path, size, and modification time.
        Sorted by age (oldest first).
    """
    if cache_root is None:
        cache_mgr = get_cache_manager()
        cache_root = cache_mgr.cache_root
    else:
        cache_root = Path(cache_root)

    files = []
    cache_base_dir = cache_root

    # Scan all JSON files in report_data and report_metadata directories
    for json_file in cache_base_dir.rglob("*.json"):
        stat = json_file.stat()
        age_hours = (time.time() - stat.st_mtime) / 3600

        files.append(
            {
                "file": str(json_file.relative_to(cache_base_dir)),
                "path": json_file,
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime),
                "age_hours": round(age_hours, 1),
            }
        )

    # Remove duplicates (shouldn't happen, but safe)
    seen = set()
    unique_files = []
    for f in files:
        if f["file"] not in seen:
            seen.add(f["file"])
            unique_files.append(f)

    return sorted(unique_files, key=lambda x: x["age_hours"], reverse=True)


def check_cache_freshness(
    threshold_hours: float = DEFAULT_STALE_THRESHOLD_HOURS, cache_root: Path = None
) -> list[dict]:
    """Check for stale cache files.

    Args:
        threshold_hours: Files older than this are considered stale. Defaults to
            DEFAULT_STALE_THRESHOLD_HOURS.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        List of stale file info dicts. Empty list if all fresh.
    """
    files = get_cache_files(cache_root=cache_root)
    stale = [f for f in files if f["age_hours"] > threshold_hours]
    return stale


def get_cache_summary(cache_root: Path = None) -> dict:
    """Get a summary of the cache directory.

    Args:
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        Dict with total files, size, oldest/newest file info.
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
            "total_size_mb": 0,
            "oldest_file": None,
            "oldest_age_hours": 0,
            "newest_file": None,
            "newest_age_hours": 0,
        }

    total_size = sum(f["size_bytes"] for f in files)
    oldest = files[0]  # Sorted by age descending
    newest = files[-1]

    return {
        "cache_dir": str(cache_root),
        "total_files": len(files),
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "oldest_file": oldest["file"],
        "oldest_age_hours": oldest["age_hours"],
        "newest_file": newest["file"],
        "newest_age_hours": newest["age_hours"],
    }