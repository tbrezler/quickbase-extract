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


class CacheRefreshError(Exception):
    """Raised when cache refresh operations fail."""

    pass


def _get_missing_reports(
    cache_manager: CacheManager, report_configs: list[ReportConfig], cache_type: str = "metadata"
) -> list[ReportConfig]:
    """Get list of reports that are missing from cache.

    Args:
        cache_manager: CacheManager instance.
        report_configs: List of ReportConfig to check.
        cache_type: Type of cache to check. Options: "metadata", "data".

    Returns:
        List of ReportConfig objects that are missing from cache.
    """
    missing = []
    for config in report_configs:
        if cache_type == "metadata":
            has_cache = cache_manager.has_report_metadata(config.app_name, config.table_name, config.report_name)
        else:  # data
            has_cache = cache_manager.has_report_data(config.app_name, config.table_name, config.report_name)

        if not has_cache:
            logger.debug(f"Missing {cache_type} for report: {config.app_name}/{config.table_name}/{config.report_name}")
            missing.append(config)
    return missing


def _determine_refresh_needs(
    cache_empty: bool,
    cache_age: float,
    stale_hours: float,
    missing_reports: list[ReportConfig],
    all_reports: list[ReportConfig],
    force: bool,
    cache_type: str,
) -> tuple[bool, list[ReportConfig], list[str]]:
    """Determine if cache needs refresh and which reports to refresh.

    Args:
        cache_empty: Whether the cache is empty.
        cache_age: Age of the cache in hours.
        stale_hours: Staleness threshold in hours.
        missing_reports: Reports missing from cache.
        all_reports: All reports that could be refreshed.
        force: Whether to force refresh regardless of staleness.
        cache_type: Type of cache ("metadata" or "data").

    Returns:
        Tuple of (needs_refresh, reports_to_refresh, reasons).
    """
    if force:
        return (True, all_reports, ["force=True"])

    if cache_empty:
        return (True, all_reports, [f"{cache_type} empty"])

    cache_stale = cache_age > stale_hours
    if cache_stale:
        return (True, all_reports, [f"{cache_type} stale ({cache_age:.1f}h > {stale_hours}h)"])

    if missing_reports:
        return (True, missing_reports, [f"{len(missing_reports)} report(s) missing {cache_type}"])

    return (False, [], [])


def _refresh_metadata_cache(
    client,
    cache_manager: CacheManager,
    reports_to_refresh: list[ReportConfig],
    reasons: list[str],
) -> None:
    """Refresh metadata cache for specified reports.

    Args:
        client: Quickbase API client.
        cache_manager: CacheManager instance.
        reports_to_refresh: Reports to refresh metadata for.
        reasons: List of reasons for refresh (for logging).

    Raises:
        CacheRefreshError: If metadata refresh fails.
    """
    logger.warning(f"Metadata cache refresh needed: {'; '.join(reasons)}")

    try:
        get_report_metadata_parallel(
            client,
            cache_manager=cache_manager,
            report_configs=reports_to_refresh,
            cache=True,
        )
        logger.info("Metadata cache refresh completed successfully")
    except Exception as e:
        logger.error(f"Metadata cache refresh failed: {e}", exc_info=True)
        raise CacheRefreshError(f"Failed to refresh metadata cache: {e}") from e


def _refresh_data_cache(
    client,
    cache_manager: CacheManager,
    reports_to_refresh: list[ReportConfig],
    reasons: list[str],
) -> None:
    """Refresh data cache for specified reports.

    Args:
        client: Quickbase API client.
        cache_manager: CacheManager instance.
        reports_to_refresh: Reports to refresh data for.
        reasons: List of reasons for refresh (for logging).

    Raises:
        CacheRefreshError: If data refresh fails.
    """
    logger.warning(f"Data cache refresh needed: {'; '.join(reasons)}")

    try:
        # Load metadata (either just refreshed or already cached)
        metadata = load_report_metadata_batch(cache_manager, reports_to_refresh)

        # Fetch and cache data for selected reports
        get_data_parallel(
            client,
            cache_manager=cache_manager,
            report_configs=reports_to_refresh,
            report_metadata=metadata,
            cache=True,
        )
        logger.info("Data cache refresh completed successfully")
    except Exception as e:
        logger.error(f"Data cache refresh failed: {e}", exc_info=True)
        raise CacheRefreshError(f"Failed to refresh data cache: {e}") from e


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
    that are empty or stale, avoiding unnecessary API calls.

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

    Raises:
        CacheRefreshError: If cache refresh operations fail.

    Environment Variables:
        METADATA_STALE_HOURS: Threshold for metadata cache staleness (in hours).
        DATA_STALE_HOURS: Threshold for data cache staleness (in hours).

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

    # Determine which reports to cache data for
    reports_for_data_cache = report_configs_all if cache_all_data else report_configs_to_cache
    data_caching_enabled = reports_for_data_cache is not None

    if not data_caching_enabled:
        logger.debug("Data caching is disabled")

    # Resolve force flags (force_all overrides individual flags)
    force_metadata_refresh = force_all or force_metadata
    force_data_refresh = force_all or force_data

    # Check metadata cache state and determine refresh needs
    metadata_empty = cache_manager.is_cache_empty("metadata")
    metadata_age = cache_manager.get_cache_age_hours("metadata")
    metadata_missing = _get_missing_reports(cache_manager, report_configs_all, cache_type="metadata")

    metadata_needs_refresh, reports_to_refresh_metadata, metadata_reasons = _determine_refresh_needs(
        cache_empty=metadata_empty,
        cache_age=metadata_age,
        stale_hours=metadata_stale_hours,
        missing_reports=metadata_missing,
        all_reports=report_configs_all,
        force=force_metadata_refresh,
        cache_type="metadata",
    )

    # Check data cache state and determine refresh needs (only if data caching enabled)
    data_needs_refresh = False
    reports_to_refresh_data = []
    data_age = None

    if data_caching_enabled:
        data_empty = cache_manager.is_cache_empty("data")
        data_age = cache_manager.get_cache_age_hours("data")
        data_missing = _get_missing_reports(cache_manager, reports_for_data_cache, cache_type="data")

        data_needs_refresh, reports_to_refresh_data, data_reasons = _determine_refresh_needs(
            cache_empty=data_empty,
            cache_age=data_age,
            stale_hours=data_stale_hours,
            missing_reports=data_missing,
            all_reports=reports_for_data_cache,
            force=force_data_refresh,
            cache_type="data",
        )

    # Early exit if nothing needs refreshing
    if not metadata_needs_refresh and not data_needs_refresh:
        log_msg = f"Cache is fresh: metadata age {metadata_age:.1f}h (threshold: {metadata_stale_hours}h)"
        if data_caching_enabled:
            log_msg += f", data age {data_age:.1f}h (threshold: {data_stale_hours}h)"
        logger.debug(log_msg)
        return

    # Refresh metadata if needed
    if metadata_needs_refresh:
        _refresh_metadata_cache(client, cache_manager, reports_to_refresh_metadata, metadata_reasons)

    # Refresh data if needed
    if data_needs_refresh:
        _refresh_data_cache(client, cache_manager, reports_to_refresh_data, data_reasons)
