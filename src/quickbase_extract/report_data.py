"""Quickbase data fetching, caching, and loading."""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.api_handlers import handle_query
from quickbase_extract.cache_manager import get_cache_manager

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
    report_desc: str,
    report_metadata: dict,
    cache: bool = False,
    cache_root=None,
    **kwargs,
) -> list[dict]:
    """Query a Quickbase table for data using cached report metadata.

    Args:
        client: Quickbase API client.
        report_desc: Unique description of a specific table report.
        report_metadata: Full metadata dict (from load_all_report_metadata).
        cache: Whether to cache the retrieved data. Defaults to False.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.
        **kwargs: Additional arguments for filtering or customization (project-specific).

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_desc not found in report_metadata.
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
        cache_mgr = get_cache_manager(cache_root=cache_root)
        data_path = cache_mgr.get_data_path(app_name, table_name, report_name)
        cache_mgr.write_file(data_path, json.dumps(final_list, indent=4))
        logger.info(f"{report_desc} data cached ({len(final_list)} records)")
    else:
        logger.info(f"{report_desc} data fetched but not cached ({len(final_list)} records)")

    return final_list


def get_data_parallel(
    client,
    report_descriptions: list[str],
    report_metadata: dict,
    cache: bool = False,
    cache_root=None,
    **kwargs,
) -> dict[str, list[dict]]:
    """Fetch multiple reports in parallel using cached report metadata.

    Args:
        client: Quickbase API client.
        report_descriptions: List of report descriptions to fetch.
        report_metadata: Full metadata dict (from load_all_report_metadata).
        cache: Whether to cache retrieved data. Defaults to False.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.
        **kwargs: Additional arguments passed to get_data() (e.g., custom filters).

    Returns:
        Dict mapping report_description -> data list.

    Raises:
        First exception encountered during parallel execution (fail-fast).
    """
    results = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all tasks
        future_to_report = {
            executor.submit(
                get_data,
                client,
                report_desc,
                report_metadata,
                cache=cache,
                cache_root=cache_root,
                **kwargs,
            ): report_desc
            for report_desc in report_descriptions
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_report):
            report_desc = future_to_report[future]
            try:
                data = future.result()
                results[report_desc] = data
            except Exception as e:
                # Cancel remaining tasks
                executor.shutdown(wait=False, cancel_futures=True)
                logger.error(f"Failed to fetch {report_desc}: {e}")
                raise

    return results


def load_data(report_desc: str, report_metadata: dict, cache_root=None) -> list[dict]:
    """Load cached data for a Quickbase report.

    Args:
        report_desc: Unique description of a specific table report.
        report_metadata: Full metadata dict (from load_all_report_metadata).
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_desc not found in report_metadata.
        FileNotFoundError: If cached data does not exist.
    """
    info = report_metadata[report_desc]
    app_name = info["app_name"]
    table_name = info["table_name"]
    report_name = info["report_name"]

    cache_mgr = get_cache_manager(cache_root=cache_root)
    data_path = cache_mgr.get_data_path(app_name, table_name, report_name)

    if not data_path.exists():
        raise FileNotFoundError(f"Cached data not found for '{report_desc}'. Expected: {data_path}")

    return json.loads(cache_mgr.read_file(data_path))
