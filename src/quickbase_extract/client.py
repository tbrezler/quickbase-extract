"""Quickbase API client factory."""

import logging

import quickbase_api

logger = logging.getLogger(__name__)


def get_qb_client(realm: str, user_token: str):
    """Create and return a Quickbase API client.

    Args:
        realm: Quickbase realm (e.g., 'mit.quickbase.com').
        user_token: Quickbase user token (from environment or config).

    Returns:
        Quickbase API client instance.

    Raises:
        Exception: If client creation fails.
    """
    try:
        client = quickbase_api.client(realm=realm, user_token=user_token)
        logger.debug(f"Created Quickbase client for realm: {realm}")
        return client
    except Exception as e:
        logger.error(f"Failed to create Quickbase client: {e}")
        raise