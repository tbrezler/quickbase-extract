"""S3-backed cache sync for Lambda environments."""

import logging
import shutil

from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.cache_orchestration import ensure_cache_freshness

logger = logging.getLogger(__name__)

# Module-level flag to track if we've synced this Lambda invocation
_CACHE_SYNCED = False


def complete_cache_refresh(
    client,
    cache_manager: CacheManager,
    report_configs: list,
    force_all: bool = False,
    force_metadata: bool = False,
    force_data: bool = False,
) -> None:
    """Completely refresh cache: clear /tmp, fetch from Quickbase, update S3, re-sync to /tmp.

    This is a dev/debugging utility for forcing a cache refresh when report
    metadata or configurations change. Clears specified local /tmp cache,
    fetches fresh data from Quickbase, writes to S3, and re-syncs to /tmp.

    Only the cache types specified by force flags are cleared and refreshed.
    For example, if only force_metadata=True, only metadata cache is touched.

    Args:
        cache_manager: CacheManager instance for cache operations.
        client: Quickbase API client for fetching fresh data.
        report_configs: List of all ReportConfig instances to refresh.
        force_all: If True, refresh both metadata and data. Overrides individual flags.
            Defaults to False.
        force_metadata: If True (and force_all is False), refresh only metadata.
            Defaults to False.
        force_data: If True (and force_all is False), refresh only data.
            Defaults to False.

    Raises:
        Exception: If cache clearing or refresh operations fail.

    Example:
        >>> # Refresh only metadata
        >>> complete_cache_refresh(
        ...     cache_manager=cache_mgr,
        ...     client=qb_client,
        ...     report_configs=report_config,
        ...     force_metadata=True,
        ... )
        >>>
        >>> # Refresh all
        >>> complete_cache_refresh(
        ...     cache_manager=cache_mgr,
        ...     client=qb_client,
        ...     report_configs=report_config,
        ...     force_all=True,
        ... )
    """
    global _CACHE_SYNCED

    # Determine which caches to refresh (force_all overrides individual flags)
    should_refresh_metadata = force_all or force_metadata
    should_refresh_data = force_all or force_data

    if not (should_refresh_metadata or should_refresh_data):
        logger.debug("No cache refresh flags set, skipping complete cache refresh")
        return

    refresh_types = []
    if should_refresh_metadata:
        refresh_types.append("metadata")
    if should_refresh_data:
        refresh_types.append("data")

    logger.warning(
        f"Starting complete cache refresh for: {', '.join(refresh_types)} "
        "(clearing /tmp, refreshing from Quickbase, updating S3...)"
    )

    # Step 1: Clear specified /tmp cache directories
    if should_refresh_metadata:
        metadata_dir = cache_manager.cache_root / "report_metadata"
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)
            logger.info(f"Cleared metadata cache directory: {metadata_dir}")

    if should_refresh_data:
        data_dir = cache_manager.cache_root / "report_data"
        if data_dir.exists():
            shutil.rmtree(data_dir)
            logger.info(f"Cleared data cache directory: {data_dir}")

    # Step 2: Reset sync flag so fresh data will be fetched
    _CACHE_SYNCED = False
    logger.debug("Reset cache sync flag")

    # Step 3: Fetch fresh data from Quickbase and write to S3
    logger.info("Fetching fresh data from Quickbase...")
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_manager,
        report_configs_all=report_configs,
        force_all=force_all,
        force_metadata=force_metadata,
        force_data=force_data,
    )

    # Step 4: Re-sync /tmp from S3 for cleared caches
    logger.info("Re-syncing /tmp from S3...")
    sync_from_s3_once(cache_manager, force=True)

    logger.warning(
        f"Complete cache refresh finished for {', '.join(refresh_types)}: "
        "/tmp and S3 now have fresh data from Quickbase"
    )


def sync_from_s3_once(cache_manager: CacheManager, force: bool = False) -> None:
    """Download cache from S3 to /tmp on Lambda cold start.

    Only syncs if cache hasn't been synced in this invocation.
    Subsequent calls are no-ops unless force=True.

    On Lambda, the sync flag persists across warm invocations within the same
    container, so warm starts skip the sync (Lambda /tmp persists). Only cold
    starts trigger a sync.

    On local environments, automatically detects if CACHE_BUCKET is configured.
    If not configured, does nothing (local caching only).

    Args:
        cache_manager: CacheManager instance for cache operations.
        force: If True, sync even if already synced in this invocation.
            Defaults to False.

    Raises:
        Exception: If S3 operations fail.

    Example:
        >>> # In Lambda handler initialization
        >>> cache_manager = CacheManager(
        ...     cache_root=Path("/tmp/my_project/dev/cache"),
        ...     s3_bucket="mit-bio-quickbase",
        ...     s3_prefix="my_project/dev/cache",
        ... )
        >>> sync_from_s3_once(cache_manager)  # Syncs on cold start
        >>> sync_from_s3_once(cache_manager)  # No-op on same invocation
        >>>
        >>> # Force re-sync if needed (programmatically)
        >>> sync_from_s3_once(cache_manager, force=True)
    """
    global _CACHE_SYNCED

    # Check for force refresh
    already_synced = _CACHE_SYNCED and not force

    if already_synced:
        logger.debug("Cache already synced in this invocation, skipping")
        return

    cache_manager.sync_from_s3()  # Handles Lambda detection internally
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
