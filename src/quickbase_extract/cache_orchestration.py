"""Cache orchestration and freshness management.

Coordinates cache freshness checks and selective refresh operations for
metadata and data caches. Ensures caches are up-to-date before processing.
"""

import logging
import os

from quickbase_extract.cache_manager import DEFAULT_DATA_STALE_HOURS, DEFAULT_METADATA_STALE_HOURS, CacheManager
from quickbase_extract.config import ReportConfig
from quickbase_extract.report_data import get_data_parallel
from quickbase_extract.report_metadata import get_report_metadata_parallel, load_report_metadata_batch

logger = logging.getLogger(__name__)


def ensure_cache_freshness(
    client,
    cache_manager: CacheManager,
    report_configs_all: list[ReportConfig],
    report_configs_to_cache: list[ReportConfig] | None = None,
    metadata_stale_hours: float | None = None,
    data_stale_hours: float | None = None,
    cache_all_data: bool = False,
    force_metadata: bool = False,
    force_data: bool = False,
    force_all: bool = False,
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
        cache_manager: CacheManager instance for cache operations
        report_configs_all: List of all ReportConfig instances to refresh
            metadata for
        report_configs_to_cache: Optional subset of ReportConfig instances to
            cache data for. If cache_all_data is True, this parameter is ignored
            and all reports' data is cached instead.
        metadata_stale_hours: Threshold (hours) for metadata staleness.
            If not provided, reads from METADATA_STALE_HOURS env var,
            falls back to DEFAULT_METADATA_STALE_HOURS (168 hours / 7 days).
        data_stale_hours: Threshold (hours) for data staleness.
            If not provided, reads from DATA_STALE_HOURS env var,
            falls back to DEFAULT_DATA_STALE_HOURS (24 hours).
        cache_all_data: If True, caches data for all reports (ignores
            report_configs_to_cache). Defaults to False.
        force_all: If True, refreshes both metadata and data immediately,
            overriding all other flags and staleness checks.
        force_metadata: If True (and force_all is False), refreshes metadata
            immediately regardless of staleness.
        force_data: If True (and force_all is False), refreshes data
            immediately regardless of staleness.

    Environment Variables:
        METADATA_STALE_HOURS: Threshold for metadata cache staleness (in hours).
        DATA_STALE_HOURS: Threshold for data cache staleness (in hours).
        FORCE_ALL_CACHE_REFRESH: If set to "true" (case-insensitive), forces a
            cache refresh of metadata and data even if cache appears fresh.

    Example:
        >>> from quickbase_extract import ensure_cache_freshness, get_qb_client, CacheManager
        >>> from config.reports import get_all_reports, get_reports_to_cache
        >>>
        >>> client = get_qb_client(realm="...", user_token="...")
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>>
        >>> # Cache only specific reports' data
        >>> ensure_cache_freshness(
        ...     client=client,
        ...     cache_manager=cache_manager,
        ...     report_configs_all=get_all_reports(),
        ...     report_configs_to_cache=get_reports_to_cache(),
        ... )
        >>>
        >>> # Cache all reports' data
        >>> ensure_cache_freshness(
        ...     client=client,
        ...     cache_manager=cache_manager,
        ...     report_configs_all=get_all_reports(),
        ...     cache_all_data=True,
        ... )
    """

    # Resolve thresholds from arguments, environment, or defaults
    if metadata_stale_hours is None:
        metadata_stale_hours = float(os.environ.get("METADATA_STALE_HOURS", DEFAULT_METADATA_STALE_HOURS))
    if data_stale_hours is None:
        data_stale_hours = float(os.environ.get("DATA_STALE_HOURS", DEFAULT_DATA_STALE_HOURS))

    # Check for force refresh via environment variable
    force_all_env = os.environ.get("FORCE_ALL_CACHE_REFRESH", "").lower() == "true"
    should_force = force_all or force_all_env

    # Determine which reports to cache data for
    if cache_all_data:
        reports_for_data_cache = report_configs_all
    else:
        reports_for_data_cache = report_configs_to_cache

    # Determine if data caching is enabled
    data_caching_enabled = reports_for_data_cache is not None

    if not data_caching_enabled:
        logger.debug("Data caching is disabled")

    # Check metadata cache
    metadata_empty = cache_manager.is_cache_empty("metadata")
    metadata_age = cache_manager.get_cache_age_hours("metadata")
    metadata_stale = metadata_age > metadata_stale_hours

    # Check data cache (only if data caching is enabled)
    data_empty = None
    data_age = None
    data_stale = None
    if data_caching_enabled:
        data_empty = cache_manager.is_cache_empty("data")
        data_age = cache_manager.get_cache_age_hours("data")
        data_stale = data_age > data_stale_hours

    # Determine refresh needs (force_all overrides individual flags)
    if should_force:
        metadata_needs_refresh = True
        data_needs_refresh = data_caching_enabled  # Only refresh data if caching is enabled
    else:
        metadata_needs_refresh = force_metadata or metadata_empty or metadata_stale
        data_needs_refresh = (
            (data_caching_enabled and (force_data or data_empty or data_stale)) if data_caching_enabled else False
        )

    # Early exit if nothing needs refreshing
    if not metadata_needs_refresh and not data_needs_refresh:
        log_msg = f"Cache is fresh: metadata {metadata_age}h " f"(threshold: {metadata_stale_hours}h)"
        if data_caching_enabled:
            log_msg += f", data {data_age}h (threshold: {data_stale_hours}h)"
        logger.debug(log_msg)
        return

    # Refresh metadata if needed
    if metadata_needs_refresh:
        reasons = []
        if should_force or force_metadata:
            reasons.append("force=True")
        if metadata_empty:
            reasons.append("metadata empty")
        elif metadata_stale:
            reasons.append(f"metadata stale ({metadata_age}h > {metadata_stale_hours}h)")

        logger.warning(f"Metadata cache refresh needed: {'; '.join(reasons)}")

        try:
            get_report_metadata_parallel(
                client,
                cache_manager=cache_manager,
                report_configs=report_configs_all,
                cache=True,
            )
            logger.info("Metadata cache refresh completed successfully")
        except Exception as e:
            logger.error(f"Metadata cache refresh failed: {e}", exc_info=True)
            # Don't return - still attempt data refresh if needed

    # Refresh data if needed
    if data_needs_refresh:
        reasons = []
        if should_force or force_data:
            reasons.append("force=True")
        if data_empty:
            reasons.append("data empty")
        elif data_stale:
            reasons.append(f"data stale ({data_age}h > {data_stale_hours}h)")

        logger.warning(f"Data cache refresh needed: {'; '.join(reasons)}")

        try:
            # Load metadata (either just refreshed or already cached)
            metadata = load_report_metadata_batch(cache_manager, reports_for_data_cache)

            # Fetch and cache data for selected reports
            get_data_parallel(
                client,
                cache_manager=cache_manager,
                report_configs=reports_for_data_cache,
                report_metadata=metadata,
                cache=True,
            )
            logger.info("Data cache refresh completed successfully")
        except Exception as e:
            logger.error(f"Data cache refresh failed: {e}", exc_info=True)
