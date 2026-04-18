"""S3-backed cache sync for Lambda environments."""

import logging

from quickbase_extract.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

# Module-level flag to track if we've synced this Lambda invocation
_CACHE_SYNCED = False


def sync_from_s3_once(force: bool = False) -> None:
    """Download cache from S3 to /tmp on Lambda cold start.

    Only syncs if cache hasn't been synced in this invocation.
    Subsequent calls are no-ops unless force=True.

    On Lambda, the sync flag persists across warm invocations within the same
    container, so warm starts skip the sync (Lambda /tmp persists). Only cold
    starts trigger a sync.

    On local environments, automatically detects if CACHE_BUCKET is configured.
    If not configured, does nothing (local caching only).

    Args:
        force: If True, sync even if already synced in this invocation.
            Defaults to False.

    Raises:
        Exception: If S3 operations fail.

    Example:
        >>> # In Lambda handler initialization
        >>> sync_from_s3_once()  # Syncs on cold start
        >>> sync_from_s3_once()  # No-op on same invocation
        >>>
        >>> # Force re-sync if needed
        >>> sync_from_s3_once(force=True)
    """
    global _CACHE_SYNCED

    if _CACHE_SYNCED and not force:
        logger.debug("Cache already synced in this invocation, skipping")
        return

    cache_mgr = get_cache_manager()
    cache_mgr.sync_from_s3()  # Handles Lambda detection internally
    _CACHE_SYNCED = True
    logger.info("Cache synced from S3")


def is_cache_synced() -> bool:
    """Check if cache has been synced in this invocation.

    Returns:
        True if cache has been synced, False otherwise.

    Example:
        >>> if not is_cache_synced():
        ...     print("Cache needs syncing")
    """
    return _CACHE_SYNCED


def _reset_cache_sync() -> None:
    """Reset the cache sync flag. For testing only.

    Example:
        >>> # In test teardown
        >>> _reset_cache_sync()
    """
    global _CACHE_SYNCED
    _CACHE_SYNCED = False
