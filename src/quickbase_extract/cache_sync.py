"""S3-backed cache sync for Lambda environments."""

import logging

from quickbase_extract.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

# Module-level flag to track if we've synced this Lambda invocation
_CACHE_SYNCED = False


def sync_from_s3_once() -> None:
    """Download cache from S3 to /tmp on cold start only.

    Only syncs if cache hasn't been synced in this invocation.
    Subsequent calls are no-ops.

    On Lambda, automatically detects if CACHE_BUCKET is configured.
    If not configured, does nothing (local caching only).

    Raises:
        Exception: If S3 operations fail.
    """
    global _CACHE_SYNCED

    if _CACHE_SYNCED:
        logger.debug("Cache already synced in this invocation, skipping.")
        return

    cache_mgr = get_cache_manager()
    cache_mgr.sync_from_s3()
    _CACHE_SYNCED = True