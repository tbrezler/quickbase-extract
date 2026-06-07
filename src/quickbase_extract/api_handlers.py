"""Error handling utilities for Quickbase operations.

Provides standardized error handling and logging for Quickbase API operations.
Rate limit retry logic with respect for the retry-after header is handled at
the session level in quickbase-api's session.py.
"""

import logging

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
) -> dict:
    """Execute a Quickbase upsert with error handling and logging.

    Rate limit retries with respect for the retry-after header are handled
    at the session level.

    Args:
        client: Quickbase API client.
        table_id: Target table ID.
        data: List of record dicts to upsert.
        description: Human-readable description for logging. Defaults to empty string.

    Returns:
        API response dict containing metadata about created/updated/unchanged records.

    Raises:
        QuickbaseOperationError: If the upsert fails.

    Example:
        >>> records = [{"6": {"value": "John"}, "7": {"value": "Doe"}}]
        >>> result = handle_upsert(client, "bq8xyx9z", records, "customer records")
    """
    try:
        result = client.upsert_records(table_id, data=data)

        created = result.get("metadata", {}).get("createdRecordIds", [])
        updated = result.get("metadata", {}).get("updatedRecordIds", [])
        unchanged = result.get("metadata", {}).get("unchangedRecordIds", [])

        logger.info(f"Upsert {description}: {len(created)} created, {len(updated)} updated, {len(unchanged)} unchanged")

        return result

    except Exception as e:
        error_str = str(e)
        logger.error(f"Upsert {description} failed: {error_str}")
        raise QuickbaseOperationError("upsert", error_str) from e


def handle_delete(
    client,
    table_id: str,
    where: str,
    description: str = "",
) -> int:
    """Execute a Quickbase delete with error handling and logging.

    Rate limit retries with respect for the retry-after header are handled
    at the session level.

    Args:
        client: Quickbase API client.
        table_id: Target table ID.
        where: Quickbase filter string specifying records to delete.
        description: Human-readable description for logging. Defaults to empty string.

    Returns:
        Number of records deleted.

    Raises:
        QuickbaseOperationError: If the delete fails.

    Example:
        >>> deleted = handle_delete(client, "bq8xyx9z", "{3.EX.'test'}", "test records")

    Note:
        DELETE is safe to retry on any error — deleting already-deleted records
        is a no-op in Quickbase.
    """
    try:
        deleted = client.delete_records(table_id, where=where)
        logger.info(f"Delete {description}: {deleted} records deleted")
        return deleted

    except Exception as e:
        error_str = str(e)
        logger.error(f"Delete {description} failed: {error_str}")
        raise QuickbaseOperationError("delete", error_str) from e


def handle_query(
    client,
    table_id: str,
    *,
    select: list[int] | None = None,
    where: str | None = None,
    sort_by: list[dict] | None = None,
    group_by: list[dict] | None = None,
    options: dict | None = None,
    description: str = "",
) -> dict:
    """Execute a Quickbase query with error handling and logging.

    Rate limit retries with respect for the retry-after header are handled
    at the session level.

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

    Returns:
        API response dict containing query results.

    Raises:
        QuickbaseOperationError: If the query fails.

    Example:
        >>> result = handle_query(
        ...     client,
        ...     "bq8xyx9z",
        ...     select=[6, 7, 8],
        ...     where="{12.EX.'Active'}",
        ...     description="active customers"
        ... )
    """
    try:
        result = client.query_for_data(
            table_id,
            select=select,
            where=where,
            sort_by=sort_by,
            group_by=group_by,
            options=options,
        )
        return result

    except Exception as e:
        error_str = str(e)
        desc_str = f" {description}" if description else f" on table {table_id}"
        logger.error(f"Query{desc_str} failed: {error_str}")
        raise QuickbaseOperationError("query", error_str) from e
