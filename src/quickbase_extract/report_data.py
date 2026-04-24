"""Quickbase data fetching, caching, and loading."""

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from quickbase_extract.api_handlers import handle_query
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.config import ReportConfig

logger = logging.getLogger(__name__)


def _replace_ask_placeholders(
    report_filter: str,
    ask_values: dict[str, str],
    report_config: ReportConfig,
) -> str:
    """Replace ask-the-user placeholders in a Quickbase filter with actual values.

    Args:
        report_filter: The filter string from report metadata (e.g., "{'25'.EX.'_ask1_'}").
        ask_values: Dict mapping placeholder keys to values (e.g., {"ask1": "abc123"}).
        report_config: ReportConfig for error messages.

    Returns:
        Modified filter string with placeholders replaced.

    Raises:
        ValueError: If required placeholders are missing values or unused values provided.

    Example:
        >>> filter_str = "{'25'.EX.'_ask1_'}AND{'10'.AF.'today'}"
        >>> config = ReportConfig("bq8xyx9z", "Accounts", "Python")
        >>> _replace_ask_placeholders(filter_str, {"ask1": "abc123"}, config)
        "{'25'.EX.'abc123'}AND{'10'.AF.'today'}"
    """
    # Find all placeholders in the filter (e.g., _ask1_, _ask2_)
    placeholders_in_filter = set(re.findall(r"_ask\d+_", report_filter))

    if not placeholders_in_filter:
        # No placeholders found - nothing to replace
        return report_filter

    # Validate: all placeholders in filter must have corresponding values
    missing_values = []
    for placeholder in placeholders_in_filter:
        # Convert _ask1_ to ask1 for lookup
        key = placeholder.strip("_")
        if key not in ask_values:
            missing_values.append(placeholder)

    if missing_values:
        raise ValueError(
            f"Report {report_config} filter requires values for {missing_values}, "
            f"but they were not provided in ask_values."
        )

    # Validate: all provided values must be used in filter
    unused_values = []
    for key in ask_values.keys():
        placeholder = f"_{key}_"
        if placeholder not in placeholders_in_filter:
            unused_values.append(key)

    if unused_values:
        raise ValueError(
            f"Report {report_config} received ask_values {unused_values} "
            f"that are not used in the filter. Available placeholders: {list(placeholders_in_filter)}"
        )

    # Replace placeholders with actual values
    modified_filter = report_filter
    for placeholder in placeholders_in_filter:
        key = placeholder.strip("_")
        value = ask_values[key]
        modified_filter = modified_filter.replace(placeholder, value)

    return modified_filter


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


def _extract_report_names(metadata_info: dict) -> tuple[str, str, str]:
    """Extract human-readable report names from metadata.

    Args:
        metadata_info: Metadata dict containing app_name, table_name, report_name.

    Returns:
        Tuple of (app_name, table_name, report_name).
    """
    return (
        metadata_info["app_name"],
        metadata_info["table_name"],
        metadata_info["report_name"],
    )


def get_data(
    client,
    cache_manager: CacheManager,
    report_config: ReportConfig,
    report_metadata: dict[ReportConfig, dict],
    cache: bool = False,
    ask_values: dict[str, str] | None = None,
) -> list[dict]:
    """Query a Quickbase table for data using cached report metadata.

    Args:
        client: Quickbase API client.
        cache_manager: CacheManager instance for cache operations.
        report_config: ReportConfig identifying the report to fetch.
        report_metadata: Dict mapping ReportConfig -> metadata dict
            (from load_report_metadata_batch).
        cache: Whether to cache the retrieved data. Defaults to False.
        ask_values: Optional dict for "ask the user" filter placeholders.
            Keys are like "ask1", "ask2", values are the replacements.
            Example: {"ask1": "abc123", "ask2": "2025-01-15"}

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_config not found in report_metadata.
        ValueError: If ask placeholders in filter are missing values.
        Exception: If Quickbase API query fails.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = ReportConfig("bq8xyx9z", "Accounts", "Python")
        >>> metadata = load_report_metadata_batch(cache_manager, [config])
        >>> data = get_data(
        ...     client, cache_manager, config, metadata,
        ...     cache=True, ask_values={"ask1": "abc123"}
        ... )
        >>> print(f"Found {len(data)} records")
    """
    info = report_metadata[report_config]
    app_name, table_name, report_name = _extract_report_names(info)

    # Process filter with ask values if provided
    report_filter = info["filter"]
    if ask_values is not None and ask_values != {}:
        original_filter = report_filter
        report_filter = _replace_ask_placeholders(report_filter, ask_values, report_config)
        logger.debug(
            f"{report_config.app_id}/{report_config.table_name}/"
            f"{report_config.report_name} filter modified: {original_filter} -> "
            f"{report_filter}"
        )

    # Query Quickbase
    query_data = handle_query(
        client,
        info["table_id"],
        select=info["fields"],
        where=report_filter,
        sort_by=info["sort_by"],
    )
    data = query_data["data"]

    # Transform records
    final_list = _flatten_and_relabel_records(data, info["field_label"], info["fields"])

    # Cache if requested
    if cache:
        data_path = cache_manager.get_data_path(app_name, table_name, report_name)
        cache_manager.write_file(data_path, json.dumps(final_list, indent=4))
        logger.info(
            f"{report_config.app_id}/{report_config.table_name}/"
            f"{report_config.report_name} data cached ({len(final_list)} records)"
        )
    else:
        logger.info(
            f"{report_config.app_id}/{report_config.table_name}/"
            f"{report_config.report_name} data fetched but not cached "
            f"({len(final_list)} records)"
        )

    return final_list


def get_data_parallel(
    client,
    cache_manager: CacheManager,
    report_configs: list[ReportConfig],
    report_metadata: dict,
    cache: bool = False,
    max_workers: int = 8,
    ask_values: dict[ReportConfig, dict[str, str] | None] | None = None,
) -> dict[ReportConfig, list[dict]]:
    """Fetch multiple reports in parallel using cached report metadata.

    Executes data fetching for multiple reports concurrently to improve
    performance. Uses a fail-fast approach: if any report fetch fails,
    all remaining tasks are cancelled and the exception is raised immediately.

    Args:
        client: Quickbase API client. Should be thread-safe for concurrent use.
        cache_manager: CacheManager instance for cache operations.
        report_configs: List of ReportConfig instances to fetch.
        report_metadata: Full metadata dict (from load_report_metadata_batch).
            Keyed by ReportConfig instances.
        cache: Whether to cache retrieved data. Defaults to False.
        max_workers: Maximum number of concurrent threads. Default is 8.
            Adjust based on API rate limits and system resources.
        ask_values: Optional dict mapping ReportConfig -> ask_values dict.
            Per-report "ask the user" filter values.
            Example: {
                ReportConfig("bq8x", "Accounts", "Python"): {"ask1": "abc"},
                ReportConfig("bq9y", "Contacts", "Active"): {"ask1": "def"}
            }

    Returns:
        Dict mapping ReportConfig -> list of record dicts.

    Raises:
        KeyError: If any report_config not found in report_metadata.
        ValueError: If ask placeholders in any filter are missing values.
        Exception: First exception encountered during parallel execution.
            All pending tasks are cancelled when an error occurs.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> configs = [
        ...     ReportConfig("bq8xyx9z", "Accounts", "Python"),
        ...     ReportConfig("bq9yza0a", "Contacts", "Active"),
        ... ]
        >>> metadata = load_report_metadata_batch(configs, cache_manager)
        >>> ask_vals = {configs[0]: {"ask1": "abc123"}}
        >>> all_data = get_data_parallel(
        ...     client, cache_manager, configs, metadata,
        ...     cache=True, ask_values=ask_vals
        ... )
        >>> print(f"Fetched {len(all_data)} reports")

    Note:
        - Ensure the Quickbase client can handle concurrent requests
        - Consider API rate limits when setting max_workers
        - All tasks are cancelled on first failure (fail-fast behavior)
    """
    if not report_configs:
        logger.warning("No report configs provided, nothing to fetch")
        return {}

    total_reports = len(report_configs)
    logger.info(f"Starting parallel fetch for {total_reports} reports with {max_workers} workers")

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with per-report ask_values
        future_to_config = {
            executor.submit(
                get_data,
                client,
                cache_manager=cache_manager,
                report_metadata=report_metadata,
                report_config=report_config,
                cache=cache,
                ask_values=ask_values.get(report_config) if ask_values else None,
            ): report_config
            for report_config in report_configs
        }

        # Process as they complete, fail fast on first error
        for future in as_completed(future_to_config):
            report_config = future_to_config[future]
            try:
                data = future.result()  # Individual fetches are logged in get_data
                results[report_config] = data
            except Exception as e:
                logger.error(
                    f"Failed to fetch {report_config.app_id}/{report_config.table_name}/"
                    f"{report_config.report_name}: {e}"
                )
                raise

    logger.info(f"Successfully completed parallel fetch for all {total_reports} reports")
    return results


def load_data(
    cache_manager: CacheManager,
    report_config: ReportConfig,
    report_metadata: dict[ReportConfig, dict],
) -> list[dict]:
    """Load cached data for a Quickbase report.

    Args:
        cache_manager: CacheManager instance for cache operations.
        report_config: ReportConfig identifying the report to load.
        report_metadata: Dict mapping ReportConfig -> metadata dict
            (from load_report_metadata_batch).

    Returns:
        List of dicts with field labels as keys.

    Raises:
        KeyError: If report_config not found in report_metadata.
        FileNotFoundError: If cached data does not exist.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> config = ReportConfig("bq8xyx9z", "Accounts", "Python")
        >>> metadata = load_report_metadata_batch(cache_manager, [config])
        >>> data = load_data(cache_manager, config, metadata)
        >>> print(f"Loaded {len(data)} records from cache")
    """
    info = report_metadata[report_config]
    app_name, table_name, report_name = _extract_report_names(info)

    data_path = cache_manager.get_data_path(app_name, table_name, report_name)

    if not data_path.exists():
        raise FileNotFoundError(f"Cached data not found for {report_config}. Expected: {data_path}")

    return json.loads(cache_manager.read_file(data_path))


def load_data_batch(
    cache_manager: CacheManager,
    report_configs: list[ReportConfig],
    report_metadata: dict[ReportConfig, dict],
) -> dict[ReportConfig, list[dict]]:
    """Load cached data for multiple reports.

    Sequentially loads cached data for each report configuration.
    This is a batch wrapper around load_data for convenience.

    Args:
        cache_manager: CacheManager instance for cache operations.
        report_configs: List of ReportConfig instances to load.
        report_metadata: Dict mapping ReportConfig -> metadata dict
            (from load_report_metadata_batch).

    Returns:
        Dict mapping ReportConfig -> list of record dicts.

    Raises:
        KeyError: If any report_config not found in report_metadata.
        FileNotFoundError: If any cached data does not exist.

    Example:
        >>> cache_manager = CacheManager(cache_root=Path("my_project/dev/cache"))
        >>> configs = [
        ...     ReportConfig("bq8xyx9z", "Accounts", "Python"),
        ...     ReportConfig("bq8xyx9z", "Contacts", "Active"),
        ... ]
        >>> metadata = load_report_metadata_batch(cache_manager, configs)
        >>> all_data = load_data_batch(cache_manager, configs, metadata)
        >>> print(f"Loaded {len(all_data)} reports from cache")
    """
    if not report_configs:
        return {}

    data: dict[ReportConfig, list[dict]] = {}
    for report_config in report_configs:
        data[report_config] = load_data(cache_manager, report_config, report_metadata)

    return data
