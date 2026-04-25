"""Error handling utilities for Quickbase operations.

Provides retry logic for rate-limited requests, standardized error handling,
and logging for Quickbase API operations.
"""

# ruff: noqa: BLE001

import logging
import random
import time

logger = logging.getLogger(__name__)


class QuickbaseOperationError(Exception):
    """Raised when a Quickbase API operation fails."""

    def __init__(self, operation: str, details: str = ""):
        self.operation = operation
        self.details = details
        super().__init__(f"Quickbase {operation} failed: {details}")


def handle_upsert(
    client,
    table_id: str,
    data: list[dict],
    description: str = "",
    max_retries: int = 3,
) -> dict:
    """Execute a Quickbase upsert with error handling, retry logic, and logging.

    Retries on rate limiting (429 errors) with exponential backoff and jitter.
    Wait time is capped at 60 seconds per retry.

    Args:
        client: Quickbase API client.
        table_id: Target table ID.
        data: List of record dicts to upsert.
        description: Human-readable description for logging. Defaults to empty string.
        max_retries: Maximum number of retry attempts. Defaults to 3.

    Returns:
        API response dict containing metadata about created/updated/unchanged records.

    Raises:
        QuickbaseOperationError: If the upsert fails after all retries.

    Example:
        >>> records = [{"6": {"value": "John"}, "7": {"value": "Doe"}}]
        >>> result = handle_upsert(client, "bq8xyx9z", records, "customer records")
    """
    for attempt in range(max_retries):
        try:
            result = client.upsert_records(table_id, data=data)

            created = result.get("metadata", {}).get("createdRecordIds", [])
            updated = result.get("metadata", {}).get("updatedRecordIds", [])
            unchanged = result.get("metadata", {}).get("unchangedRecordIds", [])

            logger.info(
                f"Upsert {description}: {len(created)} created, {len(updated)} updated, {len(unchanged)} unchanged"
            )

            return result

        except Exception as e:  # Need to catch all exceptions for retry logic
            error_str = str(e)

            # Retry on 429 (rate limit)
            if "429" in error_str and attempt < max_retries - 1:
                wait_time = min(2**attempt, 60) + random.uniform(0, 1)
                logger.warning(
                    f"Rate limited on upsert {description} (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Upsert {description} failed: {error_str}")
                raise QuickbaseOperationError("upsert", error_str) from e


def handle_delete(
    client,
    table_id: str,
    where: str,
    description: str = "",
    max_retries: int = 3,
) -> int:
    """Execute a Quickbase delete with error handling, logging, and rate limit retry.

    Only retries on rate limiting (429 errors) with exponential backoff and jitter.
    Other errors fail immediately for safety. Wait time is capped at 60 seconds per retry.

    Args:
        client: Quickbase API client.
        table_id: Target table ID.
        where: Quickbase filter string specifying records to delete.
        description: Human-readable description for logging. Defaults to empty string.
        max_retries: Maximum number of retry attempts for rate limits. Defaults to 3.

    Returns:
        Number of records deleted.

    Raises:
        QuickbaseOperationError: If the delete fails.

    Example:
        >>> deleted = handle_delete(client, "bq8xyx9z", "{3.EX.'test'}", "test records")

    Note:
        For safety, only 429 (rate limit) errors are retried. All other errors
        fail immediately to prevent unintended deletions.
    """
    for attempt in range(max_retries):
        try:
            deleted = client.delete_records(table_id, where=where)
            logger.info(f"Delete {description}: {deleted} records deleted")
            return deleted

        except Exception as e:  # Need to catch all exceptions for retry logic
            error_str = str(e)

            # Only retry on 429 (rate limit) - other errors are too risky to retry
            if "429" in error_str and attempt < max_retries - 1:
                wait_time = min(2**attempt, 60) + random.uniform(0, 1)
                logger.warning(
                    f"Rate limited on delete {description} (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Delete {description} failed: {error_str}")
                raise QuickbaseOperationError("delete", error_str) from e


def handle_query(
    client,
    table_id: str,
    *,
    select: list[int] = None,
    where: str = None,
    sort_by: list[dict] = None,
    group_by: list[dict] = None,
    options: dict = None,
    description: str = "",
    max_retries: int = 3,
) -> dict:
    """Execute a Quickbase query with error handling, retry logic, and logging.

    Retries on rate limiting (429 errors) with exponential backoff and jitter.
    Wait time is capped at 60 seconds per retry.

    Args:
        client: Quickbase API client.
        table_id: Target table ID.
        select: List of field IDs to return. If omitted, returns fields from
            the default report.
        where: A Quickbase query string (e.g., "{12.EX.'VPF'}").
        sort_by: Sort order, e.g., [{"fieldId": 6, "order": "ASC"}].
        group_by: Grouping, e.g., [{"fieldId": 6, "grouping": "equal-values"}].
        options: Additional options, e.g.,
            {"skip": 0, "top": 100, "compareWithAppLocalTime": False}.
        description: Human-readable description for logging. Defaults to empty string.
        max_retries: Maximum number of retry attempts. Defaults to 3.

    Returns:
        API response dict containing query results.

    Raises:
        QuickbaseOperationError: If the query fails after all retries.

    Example:
        >>> result = handle_query(
        ...     client,
        ...     "bq8xyx9z",
        ...     select=[6, 7, 8],
        ...     where="{12.EX.'Active'}",
        ...     description="active customers"
        ... )
    """
    for attempt in range(max_retries):
        try:
            result = client.query_for_data(
                table_id,
                select=select,
                where=where,
                sort_by=sort_by,
                group_by=group_by,
                options=options,
            )
            record_count = len(result.get("data", []))
            desc_str = f" {description}" if description else ""
            logger.info(f"Query{desc_str} returned {record_count} records")
            return result

        except Exception as e:  # Need to catch all exceptions for retry logic
            error_str = str(e)

            if "429" in error_str and attempt < max_retries - 1:
                wait_time = min(2**attempt, 60) + random.uniform(0, 1)
                desc_str = f" {description}" if description else f" table {table_id}"
                logger.warning(
                    f"Rate limited on query{desc_str} (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
            else:
                desc_str = f" {description}" if description else f" on table {table_id}"
                logger.error(f"Query{desc_str} failed: {error_str}")
                raise QuickbaseOperationError("query", error_str) from e
