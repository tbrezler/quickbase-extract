"""Quickbase data fetching, caching, and loading."""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.api_handlers import handle_query
from quickbase_extract.cache_manager import CacheManager

logger = logging.getLogger(__name__)


def _flatten_and_relabel_records(records: list[dict], field_label: dict, fields: list[int]) -> list[dict]:
    """Transform Quickbase records to flat dicts with field labels as keys.

    Args:
        records: List of records from Quickbase API (nested format).
        field_label: Dict mapping field labels to IDs.
        fields: List of field IDs in desired order.

    Returns:
        List of dicts with field labels as keys.
    """
    # Build reverse mapping: field ID -> label
    id_to_label = {v: k for k, v in field_label.items()}
    field_order = [str(f) for f in fields]

    final_list = []
    for record in records:
        # Flatten: {field_id: {value: actual}} -> {field_id: actual}
        flat = {fid: val["value"] for fid, val in record.items()}

        # Re-order to match report field order
        ordered = {fid: flat[fid] for fid in field_order if fid in flat}

        # Swap field IDs with labels
        labeled = {id_to_label[fid]: val for fid, val in ordered.items() if fid in id_to_label}

        final_list.append(labeled)

    return final_list


def get_data(
    client,
    report_metadata: dict,
    report_desc: str,
    cache_mgr: CacheManager,
    cache: bool = False,
) -> list[dict]:
    """Query a Quickbase table for data using cached report metadata.

    Args:
        client: Quickbase API client.
        report_metadata: Full metadata dict (from load_report_metadata_batch).
        report_desc: Unique description of a specific table report.
        cache_mgr: CacheManager instance for cache operations.
        cache: Whether to cache the retrieved data. Defaults to False.

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_desc not found in report_metadata.
        Exception: If Quickbase API query fails.

    Example:
        >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> metadata = load_report_metadata_batch(configs, cache_mgr)
        >>> data = get_data(client, metadata, "sales_open_deals", cache_mgr, cache=True)
        >>> print(f"Found {len(data)} records")
    """
    info = report_metadata[report_desc]

    app_name = info["app_name"]
    table_name = info["table_name"]
    report_name = info["report_name"]

    # Query Quickbase
    query_data = handle_query(
        client,
        info["table_id"],
        select=info["fields"],
        where=info["filter"],
        sort_by=info["report"]["query"]["sortBy"],
    )
    data = query_data["data"]

    # Transform records
    final_list = _flatten_and_relabel_records(data, info["field_label"], info["fields"])

    # Cache if requested
    if cache:
        data_path = cache_mgr.get_data_path(app_name, table_name, report_name)
        cache_mgr.write_file(data_path, json.dumps(final_list, indent=4))
        logger.info(f"{report_desc} data cached ({len(final_list)} records)")
    else:
        logger.info(f"{report_desc} data fetched but not cached ({len(final_list)} records)")

    return final_list


def get_data_parallel(
    client,
    report_metadata: dict,
    report_descriptions: list[str],
    cache_mgr: CacheManager,
    cache: bool = False,
    max_workers: int = 8,
) -> dict[str, list[dict]]:
    """Fetch multiple reports in parallel using cached report metadata.

    Executes data fetching for multiple reports concurrently to improve
    performance. Uses a fail-fast approach: if any report fetch fails,
    all remaining tasks are cancelled and the exception is raised immediately.

    Args:
        client: Quickbase API client. Should be thread-safe for concurrent use.
        report_metadata: Full metadata dict (from load_report_metadata_batch).
        report_descriptions: List of report descriptions to fetch.
        cache_mgr: CacheManager instance for cache operations.
        cache: Whether to cache retrieved data. Defaults to False.
        max_workers: Maximum number of concurrent threads. Default is 8.
            Adjust based on API rate limits and system resources.

    Returns:
        Dict mapping report_description -> list of record dicts.

    Raises:
        KeyError: If any report_desc not found in report_metadata.
        Exception: First exception encountered during parallel execution.
            All pending tasks are cancelled when an error occurs.

    Example:
        >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> metadata = load_report_metadata_batch(configs, cache_mgr)
        >>> descriptions = ["sales_open_deals", "sales_contacts"]
        >>> all_data = get_data_parallel(client, metadata, descriptions, cache_mgr, cache=True)
        >>> print(f"Fetched {len(all_data)} reports")

    Note:
        - Ensure the Quickbase client can handle concurrent requests
        - Consider API rate limits when setting max_workers
        - All tasks are cancelled on first failure (fail-fast behavior)
    """
    if not report_descriptions:
        logger.warning("No report descriptions provided, nothing to fetch")
        return {}

    total_reports = len(report_descriptions)
    logger.info(f"Starting parallel fetch for {total_reports} reports with {max_workers} workers")

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks (fixed argument order)
        future_to_report = {
            executor.submit(
                get_data,
                client,
                report_metadata,
                report_desc,
                cache_mgr,
                cache=cache,
            ): report_desc
            for report_desc in report_descriptions
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_report):
            report_desc = future_to_report[future]
            try:
                data = future.result()  # Individual fetches are logged in get_data
                results[report_desc] = data
            except Exception as e:
                # Cancel all remaining tasks
                executor.shutdown(wait=False, cancel_futures=True)
                logger.error(f"Failed to fetch {report_desc}: {e}")
                raise

    logger.info(f"Successfully completed parallel fetch for all {total_reports} reports")
    return results


def load_data(
    report_metadata: dict,
    report_desc: str,
    cache_mgr: CacheManager,
) -> list[dict]:
    """Load cached data for a Quickbase report.

    Args:
        report_metadata: Full metadata dict (from load_report_metadata_batch).
        report_desc: Unique description of a specific table report.
        cache_mgr: CacheManager instance for cache operations.

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_desc not found in report_metadata.
        FileNotFoundError: If cached data does not exist.

    Example:
        >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> metadata = load_report_metadata_batch(configs, cache_mgr)
        >>> data = load_data(metadata, "sales_open_deals", cache_mgr)
        >>> print(f"Loaded {len(data)} records from cache")
    """
    info = report_metadata[report_desc]
    app_name = info["app_name"]
    table_name = info["table_name"]
    report_name = info["report_name"]

    data_path = cache_mgr.get_data_path(app_name, table_name, report_name)

    if not data_path.exists():
        raise FileNotFoundError(f"Cached data not found for '{report_desc}'. Expected: {data_path}")

    return json.loads(cache_mgr.read_file(data_path))


def load_data_batch(
    report_metadata: dict,
    report_descriptions: list[str],
    cache_mgr: CacheManager,
) -> dict[str, list[dict]]:
    """Load cached data for multiple reports.

    Sequentially loads cached data for each report description.
    This is a batch wrapper around load_data for convenience.

    Args:
        report_metadata: Full metadata dict (from load_report_metadata_batch).
        report_descriptions: List of report descriptions to load.
        cache_mgr: CacheManager instance for cache operations.

    Returns:
        Dict mapping report_description -> list of record dicts.

    Raises:
        KeyError: If any report_desc not found in report_metadata.
        FileNotFoundError: If any cached data does not exist.

    Example:
        >>> cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> metadata = load_report_metadata_batch(configs, cache_mgr)
        >>> descriptions = ["sales_open_deals", "sales_contacts"]
        >>> all_data = load_data_batch(metadata, descriptions, cache_mgr)
        >>> print(f"Loaded {len(all_data)} reports from cache")
    """
    if not report_descriptions:
        return {}

    data = {}
    for report_desc in report_descriptions:
        data[report_desc] = load_data(report_metadata, report_desc, cache_mgr)
    return data
