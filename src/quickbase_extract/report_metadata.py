"""Quickbase report metadata fetching and caching.

Retrieves table and report metadata from Quickbase (field mappings, report
configurations, filters) and caches them as JSON files for use by report_data.py.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.config import ReportConfig
from quickbase_extract.utils import normalize_name

logger = logging.getLogger(__name__)


def fetch_report_metadata_api(
    client,
    app_id: str,
    table_name: str,
    report_name: str,
) -> dict:
    """Fetch essential report metadata from Quickbase API.

    Retrieves table ID, field mappings, and report query configuration
    (fields, filter, sort, grouping) from Quickbase.

    Args:
        client: Quickbase API client.
        app_id: The Quickbase application ID.
        table_name: Name of the table containing the report.
        report_name: Name of the report to fetch.

    Returns:
        Dict containing:
            - table_id: The table's Quickbase ID
            - field_label: Mapping of field labels to field IDs
            - fields: List of field IDs to query
            - filter: Quickbase query filter string
            - sort_by: Sort configuration (list of dicts)
            - group_by: Grouping configuration (list of dicts)

    Raises:
        ValueError: If report_name is not found in the specified table.
        Exception: If any Quickbase API call fails.
    """
    table_id = client.get_table_id(app_id, table_name=table_name)
    field_label = client.get_field_label_id_map(table_id)
    reports = client.get_reports(table_id)

    # Find matching report(s)
    report_matches = [r for r in reports if r["name"] == report_name]

    if not report_matches:
        available = [r["name"] for r in reports]
        raise ValueError(
            f"Report '{report_name}' not found in table '{table_name}'. " f"Available reports: {available}"
        )

    # Warn if multiple matches (unlikely but possible)
    if len(report_matches) > 1:
        logger.warning(f"Multiple reports match '{report_name}' in table '{table_name}', " f"using first match")

    report_id = report_matches[0]["id"]
    report = client.get_report(table_id, report_id=report_id)

    # Extract only necessary query components
    query = report.get("query", {})
    fields = query.get("fields", [])

    # Filter field_label to only include fields used in this report
    filtered_field_label = {name: int(fid) for name, fid in field_label.items() if int(fid) in fields}

    return {
        "table_id": table_id,
        "field_label": filtered_field_label,
        "fields": query.get("fields", []),
        "filter": query.get("filter", ""),
        "sort_by": query.get("sortBy", []),
        "group_by": query.get("groupBy", []),
    }


def get_report_metadata(
    client,
    cache_manager: CacheManager,
    report_config: ReportConfig,
    cache: bool = True,
) -> None:
    """Fetch and cache table/report metadata from Quickbase.

    Queries Quickbase for table ID, field mappings, report configuration,
    and filter settings, then saves the result as a JSON file in the cache.

    The cached metadata includes normalized names, field mappings, and query
    configuration that can be used for subsequent data operations without
    additional API calls.

    Args:
        client: Quickbase API client.
        cache_manager: CacheManager instance for cache operations.
        report_config: ReportConfig identifying the report to fetch.
        cache: Whether to cache retrieved data. Defaults to True.

    Returns:
        None. Writes metadata to cache as JSON file.

    Raises:
        ValueError: If report is not found in the specified table.
        Exception: If any Quickbase API call fails.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = ReportConfig(
        ...     app_id="bq8xyx9z",
        ...     table_name="Accounts",
        ...     report_name="Python"
        ... )
        >>> get_report_metadata(qb_client, cache_manager, config)
    """
    logger.info(f"Fetching {report_config.app_id}: {report_config.table_name} - " f"{report_config.report_name}")

    # Fetch from API
    data = fetch_report_metadata_api(
        client,
        report_config.app_id,
        report_config.table_name,
        report_config.report_name,
    )

    # Build metadata structure with normalized names
    report_md = {
        "app_name": report_config.app_name,
        "table_name": normalize_name(report_config.table_name),
        "report_name": normalize_name(report_config.report_name),
        "table_id": data["table_id"],
        "field_label": data["field_label"],
        "fields": data["fields"],
        "filter": data["filter"],
        "sort_by": data["sort_by"],
        "group_by": data["group_by"],
    }

    # Cache if requested
    if cache:
        md_path = cache_manager.get_metadata_path(
            report_md["app_name"],
            report_md["table_name"],
            report_md["report_name"],
        )
        cache_manager.write_file(md_path, json.dumps(report_md, indent=4))
        logger.info(f"{report_config.app_id}/{report_config.table_name}_" f"{report_config.report_name}.json cached")
    else:
        logger.info(
            f"{report_config.app_id}: {report_config.table_name} - "
            f"{report_config.report_name} report metadata fetched but not cached"
        )


def get_report_metadata_parallel(
    client,
    cache_manager: CacheManager,
    report_configs: list[ReportConfig],
    cache: bool = True,
    max_workers: int = 8,
) -> None:
    """Fetch multiple report metadata in parallel using thread pool.

    Executes metadata fetching for multiple reports concurrently to improve
    performance when processing large numbers of reports. Uses a fail-fast
    approach: if any report fetch fails, all remaining tasks are cancelled
    and the exception is raised immediately.

    Args:
        client: Quickbase API client. Should be thread-safe for concurrent use.
        cache_manager: CacheManager instance for cache operations.
        report_config: List of ReportConfig instances to fetch.
        cache: Whether to cache retrieved data. Defaults to True.
        max_workers: Maximum number of concurrent threads. Default is 8.
            Adjust based on API rate limits and system resources.

    Returns:
        None. Each report's metadata is written to cache as a JSON file.

    Raises:
        ValueError: If any report is not found in its specified table.
        Exception: First exception encountered during parallel execution.
            All pending tasks are cancelled when an error occurs.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = [
        ...     ReportConfig("bq8xyx9z", "Accounts", "Python"),
        ...     ReportConfig("bq8xyx9z", "Contacts", "Active"),
        ... ]
        >>> get_report_metadata_parallel(qb_client, cache_manager, config)

    Note:
        - Ensure the Quickbase client can handle concurrent requests
        - Consider API rate limits when setting max_workers
        - All tasks are cancelled on first failure (fail-fast behavior)
    """
    if not report_configs:
        logger.warning("No report config provided, nothing to fetch")
        return

    total_reports = len(report_configs)
    logger.info(f"Starting parallel fetch for {total_reports} reports with {max_workers} " f"workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_config = {
            executor.submit(
                get_report_metadata,
                client,
                cache_manager=cache_manager,
                report_config=config,
                cache=cache,
            ): config
            for config in report_configs
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_config):
            config = future_to_config[future]
            try:
                future.result()  # Individual fetches are logged in get_report_metadata
            except Exception as e:
                # Cancel all remaining tasks
                executor.shutdown(wait=False, cancel_futures=True)
                logger.error(
                    f"Failed to fetch metadata for {config.app_id}/" f"{config.table_name}/{config.report_name}: {e}"
                )
                raise

    logger.info(f"Successfully completed parallel fetch for all {total_reports} reports")


def load_report_metadata(
    cache_manager: CacheManager,
    report_config: ReportConfig,
) -> dict:
    """Load cached report metadata from disk.

    Args:
        cache_manager: CacheManager instance for cache operations.
        report_config: ReportConfig identifying the report to load.

    Returns:
        Dict containing table ID, field mappings, query config, and filters.

    Raises:
        FileNotFoundError: If cached metadata does not exist.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = ReportConfig("bq8xyx9z", "Accounts", "Python")
        >>> metadata = load_report_metadata(cache_manager, config)
    """
    # Normalize names to match how they were saved
    app_name = normalize_name(report_config.app_name)
    table_name = normalize_name(report_config.table_name)
    report_name = normalize_name(report_config.report_name)

    md_path = cache_manager.get_metadata_path(app_name, table_name, report_name)

    if not md_path.exists():
        raise FileNotFoundError(
            f"Report metadata not found for {report_config}. " f"Run get_report_metadata() first. Expected: {md_path}"
        )

    return json.loads(cache_manager.read_file(md_path))


def load_report_metadata_batch(
    cache_manager: CacheManager,
    report_configs: list[ReportConfig],
) -> dict[ReportConfig, dict]:
    """Load metadata for all reports from cache.

    Sequentially loads cached metadata for each report configuration and
    returns a dict keyed by ReportConfig for easy lookup.

    Args:
        cache_manager: CacheManager instance for cache operations.
        report_config: List of ReportConfig instances to load.

    Returns:
        Dict mapping ReportConfig -> metadata dict.

    Raises:
        FileNotFoundError: If any report metadata is not cached.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = [
        ...     ReportConfig("bq8xyx9z", "Accounts", "Python"),
        ...     ReportConfig("bq8xyx9z", "Contacts", "Active"),
        ... ]
        >>> all_metadata = load_report_metadata_batch(cache_manager, config)
        >>> python_metadata = all_metadata[config[0]]
    """
    if not report_configs:
        return {}

    metadata: dict[ReportConfig, dict] = {}
    for config in report_configs:
        metadata[config] = load_report_metadata(cache_manager, config)

    return metadata
