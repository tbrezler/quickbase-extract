"""Quickbase report metadata fetching and caching.

Retrieves table and report metadata from Quickbase (field mappings, report
configurations, filters) and caches them as JSON files for use by report_data.py.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.api_handlers import handle_query
from quickbase_extract.cache_manager import get_cache_manager
from quickbase_extract.utils import find_report, normalize_name

logger = logging.getLogger(__name__)


def get_report_metadata(
    client, app_name: str, app_id: str, table_name: str, report_name: str, cache_root=None
) -> None:
    """Fetch and cache table/report metadata from Quickbase.

    Queries Quickbase for table ID, field mappings, report configuration,
    and filter settings, then saves the result as a JSON file.

    Args:
        client: Quickbase API client.
        app_name: The Quickbase app name.
        app_id: The Quickbase app ID.
        table_name: The name of the table being queried.
        report_name: The name of the report being used.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Raises:
        Exception: If any Quickbase API call fails.
    """
    logger.info(f"Fetching {app_name}: {table_name} - {report_name}")

    table_id = client.get_table_id(app_id, table_name=table_name)
    field_label = client.get_field_label_id_map(table_id)
    reports = client.get_reports(table_id)
    report_id = "".join([report["id"] for report in reports if report["name"] == report_name])

    if not report_id:
        available = [r["name"] for r in reports]
        raise ValueError(
            f"Report '{report_name}' not found in table '{table_name}'. "
            f"Available reports: {available}"
        )

    report = client.get_report(table_id, report_id=report_id)
    fields = report["query"]["fields"]
    filter_str = report["query"]["filter"]

    report_md = {
        "app_name": normalize_name(app_name),
        "table_name": normalize_name(table_name),
        "table_id": table_id,
        "field_label": field_label,
        "report_name": normalize_name(report_name),
        "report_id": report_id,
        "report": report,
        "fields": fields,
        "filter": filter_str,
    }

    # Get path from CacheManager and write
    cache_mgr = get_cache_manager(cache_root=cache_root)
    md_path = cache_mgr.get_metadata_path(app_name, table_name, report_name)
    cache_mgr.write_file(md_path, json.dumps(report_md, indent=4))


def get_report_metadata_parallel(
    client, report_configs: list[dict], cache_root=None
) -> dict[str, dict]:
    """Fetch multiple report metadata in parallel.

    Args:
        client: Quickbase API client.
        report_configs: List of dicts with keys: App, App ID, Table, Report.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Returns:
        Dict mapping "app:table:report" -> metadata dict.

    Raises:
        First exception encountered during parallel execution (fail-fast).
    """
    results = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all tasks
        future_to_config = {
            executor.submit(
                get_report_metadata,
                client,
                r["App"],
                r["App ID"],
                r["Table"],
                r["Report"],
                cache_root,
            ): f"{r['App']}:{r['Table']}:{r['Report']}"
            for r in report_configs
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_config):
            config_key = future_to_config[future]
            try:
                future.result()
                logger.info(f"Successfully fetched metadata: {config_key}")
            except Exception as e:
                # Cancel remaining tasks
                executor.shutdown(wait=False, cancel_futures=True)
                logger.error(f"Failed to fetch metadata for {config_key}: {e}")
                raise

    return results


def refresh_all(client, report_configs: list[dict], cache_root=None) -> None:
    """Refresh all report metadata from Quickbase.

    Args:
        client: Quickbase API client.
        report_configs: List of report configuration dicts with keys:
            App, App ID, Table, Report.
        cache_root: Optional cache root path. If not provided, uses CacheManager default.

    Raises:
        Exception: If any metadata fetch fails.
    """
    logger.info(f"Refreshing metadata for {len(report_configs)} reports.")
    report_md_start = time.time()

    # Use parallel fetching
    get_report_metadata_parallel(client, report_configs, cache_root=cache_root)

    elapsed = round(time.time() - report_md_start, 3)
    logger.info(f"Report metadata refresh time: {elapsed}s")


def load_report_metadata(
    report_desc: str, report_configs: list[dict], cache_root=None
) -> dict:
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

    app_name = normalize_name(report["App"])
    table_name = normalize_name(report["Table"])
    report_name = normalize_name(report["Report"])

    cache_mgr = get_cache_manager(cache_root=cache_root)
    md_path = cache_mgr.get_metadata_path(report["App"], report["Table"], report["Report"])

    if not md_path.exists():
        raise FileNotFoundError(
            f"Report metadata not found for '{report_desc}'. "
            f"Run refresh_all() first. Expected: {md_path}"
        )

    return json.loads(cache_mgr.read_file(md_path))