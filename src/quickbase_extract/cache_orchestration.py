"""Cache orchestration and freshness management.

Coordinates cache freshness checks and selective refresh operations for
metadata and data caches. Ensures caches are up-to-date before processing.
"""

import logging
import os

from quickbase_extract.cache_manager import DEFAULT_DATA_STALE_HOURS, DEFAULT_METADATA_STALE_HOURS, CacheManager
from quickbase_extract.report_data import get_data_parallel
from quickbase_extract.report_metadata import get_report_metadata_parallel, load_report_metadata_batch

logger = logging.getLogger(__name__)


def ensure_cache_freshness(
    client,
    report_configs: list,
    cache_mgr: CacheManager,
    metadata_stale_hours: float | None = None,
    data_stale_hours: float | None = None,
    force: bool = False,
) -> None:
    """Ensure cache is fresh; refresh metadata and/or data if empty or stale.

    Checks metadata and data caches independently. Refreshes only the caches
    that are empty or stale, avoiding unnecessary API calls. Gracefully handles
    refresh failures (logs but does not re-raise).

    This is the primary orchestration function for cache freshness management.
    Use it in your Lambda handlers or initialization code to ensure cache
    is ready before processing.

    Args:
        client: Quickbase API client (required for refresh)
        report_configs: List of report configuration dicts
        cache_mgr: CacheManager instance for cache operations
        metadata_stale_hours: Threshold (hours) for metadata staleness.
            If not provided, reads from METADATA_STALE_HOURS env var,
            falls back to DEFAULT_METADATA_STALE_HOURS (168 hours / 7 days).
        data_stale_hours: Threshold (hours) for data staleness.
            If not provided, reads from DATA_STALE_HOURS env var,
            falls back to DEFAULT_DATA_STALE_HOURS (24 hours).
        force: If True, skips all checks and refreshes immediately.
            Can also be triggered via FORCE_CACHE_REFRESH environment variable.

    Environment Variables:
        METADATA_STALE_HOURS: Threshold for metadata cache staleness (in hours).
        DATA_STALE_HOURS: Threshold for data cache staleness (in hours).
        FORCE_CACHE_REFRESH: If set to "true" (case-insensitive), forces a
            cache refresh even if cache appears fresh.

    Example:
        >>> from quickbase_extract import ensure_cache_freshness, get_qb_client, CacheManager
        >>> from config.reports import get_reports
        >>>
        >>> client = get_qb_client(realm="...", user_token="...")
        >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> report_configs = get_reports()
        >>>
        >>> ensure_cache_freshness(
        ...     client=client,
        ...     report_configs=report_configs,
        ...     cache_mgr=cache_mgr,
        ...     metadata_stale_hours=720,  # 30 days
        ... )
    """

    # Resolve thresholds from arguments, environment, or defaults
    if metadata_stale_hours is None:
        metadata_stale_hours = float(os.environ.get("METADATA_STALE_HOURS", DEFAULT_METADATA_STALE_HOURS))
    if data_stale_hours is None:
        data_stale_hours = float(os.environ.get("DATA_STALE_HOURS", DEFAULT_DATA_STALE_HOURS))

    # Check for force refresh via environment variable
    force_env = os.environ.get("FORCE_CACHE_REFRESH", "").lower() == "true"
    should_force = force or force_env

    # Check metadata cache
    metadata_empty = cache_mgr.is_cache_empty("metadata")
    metadata_age = cache_mgr.get_cache_age_hours("metadata")
    metadata_stale = metadata_age > metadata_stale_hours
    metadata_needs_refresh = should_force or metadata_empty or metadata_stale

    # Check data cache
    data_empty = cache_mgr.is_cache_empty("data")
    data_age = cache_mgr.get_cache_age_hours("data")
    data_stale = data_age > data_stale_hours
    data_needs_refresh = should_force or data_empty or data_stale

    # Early exit if nothing needs refreshing
    if not metadata_needs_refresh and not data_needs_refresh:
        logger.debug(
            f"Cache is fresh: metadata {metadata_age}h (threshold: {metadata_stale_hours}h), "
            f"data {data_age}h (threshold: {data_stale_hours}h)"
        )
        return

    # Refresh metadata if needed
    if metadata_needs_refresh:
        reasons = []
        if should_force:
            reasons.append("force=True")
        if metadata_empty:
            reasons.append("metadata empty")
        elif metadata_stale:
            reasons.append(f"metadata stale ({metadata_age}h > {metadata_stale_hours}h)")

        logger.warning(f"Metadata cache refresh needed: {'; '.join(reasons)}")

        try:
            get_report_metadata_parallel(client, report_configs, cache_mgr)
            logger.info("Metadata cache refresh completed successfully")
        except Exception as e:
            logger.error(f"Metadata cache refresh failed: {e}", exc_info=True)
            # Don't return - still attempt data refresh if needed

    # Refresh data if needed
    if data_needs_refresh:
        reasons = []
        if should_force:
            reasons.append("force=True")
        if data_empty:
            reasons.append("data empty")
        elif data_stale:
            reasons.append(f"data stale ({data_age}h > {data_stale_hours}h)")

        logger.warning(f"Data cache refresh needed: {'; '.join(reasons)}")

        try:
            # Load metadata (either just refreshed or already cached)
            metadata = load_report_metadata_batch(report_configs, cache_mgr)
            report_descriptions = [config["Description"] for config in report_configs]

            # Fetch and cache all data
            get_data_parallel(
                client,
                metadata,
                report_descriptions,
                cache_mgr=cache_mgr,
                cache=True,
            )
            logger.info("Data cache refresh completed successfully")
        except Exception as e:
            logger.error(f"Data cache refresh failed: {e}", exc_info=True)
