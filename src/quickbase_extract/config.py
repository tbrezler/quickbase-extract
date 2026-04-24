"""Configuration classes for Quickbase report handling."""

from typing import NamedTuple


class ReportConfig(NamedTuple):
    """Minimal configuration identifying a Quickbase report.

    Attributes:
        app_id: Quickbase app ID (e.g., "bq8xyx9z").
        app_name: Normalized app name for cache/logging (e.g., "bif").
        table_name: Table name in Quickbase.
        report_name: Report name within the table.

    Example:
        >>> config = ReportConfig(
        ...     app_id="bq8xyx9z",
        ...     app_name="bif",
        ...     table_name="Accounts",
        ...     report_name="Python"
        ... )
    """

    app_id: str
    app_name: str
    table_name: str
    report_name: str
