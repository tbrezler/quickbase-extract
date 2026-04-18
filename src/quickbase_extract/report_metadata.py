"""Quickbase report metadata fetching and caching.

Retrieves table and report metadata from Quickbase (field mappings, report
configurations, filters) and caches them as JSON files for use by report_data.py.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.cache_manager import get_cache_manager
from quickbase_extract.utils import find_report, normalize_name

logger = logging.getLogger(__name__)


def fetch_report_data(client, app_id: str, table_name: str, report_name: str) -> dict:
    """Fetch report metadata from Quickbase API.

    Retrieves table ID, field mappings, and report configuration from Quickbase
    for the specified report.

    Args:
        client: Quickbase API client.
        app_id: The Quickbase application ID.
        table_name: Name of the table containing the report.
        report_name: Name of the report to fetch.

    Returns:
        Dict containing:
            - table_id: The table's Quickbase ID
            - field_label: Mapping of field labels to field IDs
            - report_id: The report's Quickbase ID
            - report: Full report object with query configuration

    Raises:
        ValueError: If report_name is not found in the specified table.
        Exception: If any Quickbase API call fails.
    """
    table_id = client.get_table_id(app_id, table_name=table_name)
    field_label = client.get_field_label_id_map(table_id)
    reports = client.get_reports(table_id)

    report_id = next((r["id"] for r in reports if r["name"] == report_name), None)

    if not report_id:
        available = [r["name"] for r in reports]
        raise ValueError(
            f"Report '{report_name}' not found in table '{table_name}'. " f"Available reports: {available}"
        )

    report = client.get_report(table_id, report_id=report_id)

    return {
        "table_id": table_id,
        "field_label": field_label,
        "report_id": report_id,
        "report": report,
    }


def get_report_metadata(client, report_config: dict, cache_root=None) -> None:
    """Fetch and cache table/report metadata from Quickbase.

    Queries Quickbase for table ID, field mappings, report configuration,
    and filter settings, then saves the result as a JSON file in the cache.

    The cached metadata includes normalized names, field mappings, report
    configuration, and query filters that can be used for subsequent data
    operations without additional API calls.

    Args:
        client: Quickbase API client.
        report_config: Dict with keys:
            - App: Application display name
            - App ID: Quickbase application ID
            - Table: Table name within the application
            - Report: Report name within the table
        cache_root: Optional cache root path. If not provided, uses
            CacheManager default.

    Returns:
        None. Writes metadata to cache as JSON file.

    Raises:
        ValueError: If report is not found in the specified table.
        KeyError: If report_config is missing required keys.
        Exception: If any Quickbase API call fails.

    Example:
        >>> config = {
        ...     "App": "Sales Tracker",
        ...     "App ID": "bq8xyx9z",
        ...     "Table": "Opportunities",
        ...     "Report": "Open Deals"
        ... }
        >>> get_report_metadata(qb_client, config)
    """
    app_name = report_config["App"]
    app_id = report_config["App ID"]
    table_name = report_config["Table"]
    report_name = report_config["Report"]

    logger.info(f"Fetching {app_name}: {table_name} - {report_name}")

    # Fetch from API
    data = fetch_report_data(client, app_id, table_name, report_name)

    # Build metadata structure with normalized names
    report_md = {
        "app_name": normalize_name(app_name),
        "table_name": normalize_name(table_name),
        "table_id": data["table_id"],
        "field_label": data["field_label"],
        "report_name": normalize_name(report_name),
        "report_id": data["report_id"],
        "report": data["report"],
        "fields": data["report"]["query"]["fields"],
        "filter": data["report"]["query"]["filter"],
    }

    # Cache the metadata
    cache_mgr = get_cache_manager(cache_root=cache_root)
    md_path = cache_mgr.get_metadata_path(report_md["app_name"], report_md["table_name"], report_md["report_name"])
    cache_mgr.write_file(md_path, json.dumps(report_md, indent=4))


def get_report_metadata_parallel(client, report_configs: list[dict], cache_root=None, max_workers: int = 8) -> None:
    """Fetch multiple report metadata in parallel using thread pool.

    Executes metadata fetching for multiple reports concurrently to improve
    performance when processing large numbers of reports. Uses a fail-fast
    approach: if any report fetch fails, all remaining tasks are cancelled
    and the exception is raised immediately.

    Args:
        client: Quickbase API client. Should be thread-safe for concurrent use.
        report_configs: List of dicts, each containing:
            - App: Application display name
            - App ID: Quickbase application ID
            - Table: Table name within the application
            - Report: Report name within the table
        cache_root: Optional cache root path. If not provided, uses
            CacheManager default.
        max_workers: Maximum number of concurrent threads. Default is 8.
            Adjust based on API rate limits and system resources.

    Returns:
        None. Each report's metadata is written to cache as a JSON file.

    Raises:
        ValueError: If any report is not found in its specified table.
        KeyError: If any report_config is missing required keys.
        Exception: First exception encountered during parallel execution.
            All pending tasks are cancelled when an error occurs.

    Example:
        >>> configs = [
        ...     {
        ...         "App": "Sales Tracker",
        ...         "App ID": "bq8xyx9z",
        ...         "Table": "Opportunities",
        ...         "Report": "Open Deals"
        ...     },
        ...     {
        ...         "App": "Sales Tracker",
        ...         "App ID": "bq8xyx9z",
        ...         "Table": "Contacts",
        ...         "Report": "Active Contacts"
        ...     }
        ... ]
        >>> get_report_metadata_parallel(qb_client, configs)

    Note:
        - Ensure the Quickbase client can handle concurrent requests
        - Consider API rate limits when setting max_workers
        - All tasks are cancelled on first failure (fail-fast behavior)
    """
    if not report_configs:
        logger.warning("No report configs provided, nothing to fetch")
        return

    total_reports = len(report_configs)
    logger.info(f"Starting parallel fetch for {total_reports} reports with {max_workers} workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_config = {
            executor.submit(
                get_report_metadata,
                client,
                report_config,
                cache_root,
            ): f"{report_config['App']}:{report_config['Table']}:{report_config['Report']}"
            for report_config in report_configs
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_config):
            config_key = future_to_config[future]
            try:
                future.result()  # Individual fetches are logged in get_report_metadata
            except Exception as e:
                # Cancel all remaining tasks
                executor.shutdown(wait=False, cancel_futures=True)
                logger.error(f"Failed to fetch metadata for {config_key}: {e}")
                raise

    logger.info(f"Successfully completed parallel fetch for all {total_reports} reports")


def load_report_metadata(report_desc: str, report_configs: list[dict], cache_root=None) -> dict:
    """Load cached report metadata from disk.

    Args:
        report_desc: Unique description of a specific table report.
        report_configs: List of report configuration dicts (used to find matching report).
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        Dict containing table ID, field mappings, report config, and filter.

    Raises:
        ValueError: If no report matches the description.
        FileNotFoundError: If cached metadata does not exist.
    """
    report = find_report(report_configs, report_desc)

    cache_mgr = get_cache_manager(cache_root=cache_root)
    # Must normalize names to match how they were saved
    md_path = cache_mgr.get_metadata_path(
        normalize_name(report["App"]), normalize_name(report["Table"]), normalize_name(report["Report"])
    )

    if not md_path.exists():
        raise FileNotFoundError(
            f"Report metadata not found for '{report_desc}'. " f"Run refresh_all() first. Expected: {md_path}"
        )

    return json.loads(cache_mgr.read_file(md_path))


def load_report_metadata_batch(report_configs: list[dict], cache_root=None) -> dict:
    """Load metadata for all reports from cache.

    Sequentially loads cached metadata for each report configuration.
    This is a simple wrapper around load_report_metadata for convenience.

    Args:
        report_configs: List of report configuration dicts, each containing
            a 'Description' key used as the lookup key.
        cache_root: Optional cache root path. If not provided, uses
            CacheManager default.

    Returns:
        Dict mapping report descriptions to their metadata dicts.

    Raises:
        ValueError: If any report description is not found in report_configs.
        FileNotFoundError: If any report metadata is not cached.

    Example:
        >>> configs = [{"Description": "sales_open_deals", ...}, ...]
        >>> all_metadata = load_all_report_metadata(configs)
        >>> sales_metadata = all_metadata["sales_open_deals"]
    """
    if not report_configs:
        return {}

    metadata = {}
    for config in report_configs:
        report_desc = config["Description"]
        metadata[report_desc] = load_report_metadata(report_desc, report_configs, cache_root=cache_root)
    return metadata


def refresh_all(client, report_configs: list[dict], cache_root=None) -> None:
    """Refresh all report metadata from Quickbase.

    Fetches fresh metadata from Quickbase API for all configured reports
    and updates the local cache. Uses parallel fetching for improved performance.

    Args:
        client: Quickbase API client.
        report_configs: List of report configuration dicts with keys:
            - App: Application display name
            - App ID: Quickbase application ID
            - Table: Table name
            - Report: Report name
        cache_root: Optional cache root path. If not provided, uses
            CacheManager default.

    Returns:
        None. Updates cached metadata files on disk.

    Raises:
        Exception: If any metadata fetch fails. Uses fail-fast behavior.

    Example:
        >>> refresh_all(qb_client, report_configs)
        # INFO: Refreshing metadata for 15 reports.
        # INFO: Report metadata refresh time: 3.421s
    """
    if not report_configs:
        logger.warning("No report configs provided, nothing to refresh")
        return

    logger.info(f"Refreshing metadata for {len(report_configs)} reports")
    report_md_start = time.time()

    # Use parallel fetching for performance
    get_report_metadata_parallel(client, report_configs, cache_root=cache_root)

    elapsed = round(time.time() - report_md_start, 3)
    logger.info(f"Report metadata refresh completed in {elapsed}s")
