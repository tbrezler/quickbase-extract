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
