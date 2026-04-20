"""Utility functions for Quickbase data extraction."""

import logging

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Convert a display name to a file-safe format.

    Args:
        name: Display name string (e.g., 'Billing Records')

    Returns:
        Normalized string (e.g., 'billing_records')
    """
    return name.lower().replace(" ", "_")


def find_report(reports: list[dict], report_desc: str) -> dict:
    """Find a report by description from the report list.

    Args:
        reports: List of report configuration dicts.
        report_desc: Description string to match.

    Returns:
        The first matching report dict.

    Raises:
        ValueError: If no report matches the description.
    """
    matches = [r for r in reports if r["Description"] == report_desc]

    if not matches:
        available = [r["Description"] for r in reports]
        raise ValueError(f"No report found matching description: '{report_desc}'. Available reports: {available}")

    if len(matches) > 1:
        logger.warning(f"Multiple reports match '{report_desc}', using first match")

    return matches[0]
