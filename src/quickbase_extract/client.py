"""Quickbase API client factory."""

import logging
from typing import Any

import quickbase_api

logger = logging.getLogger(__name__)

# Cache for client instances (realm, user_token) -> client
_client_cache: dict[tuple[str, str], Any] = {}


def get_qb_client(realm: str, user_token: str, cache: bool = True) -> Any:
    """Create and return a Quickbase API client.

    Clients are cached by (realm, token) combination to avoid recreating
    connections. Use cache=False to force a new client instance.

    Args:
        realm: Quickbase realm (e.g., 'example.quickbase.com').
        user_token: Quickbase user token (from environment or config).
        cache: Whether to reuse cached client. Defaults to True.

    Returns:
        Quickbase API client instance.

    Raises:
        ValueError: If realm or user_token is empty.
        Exception: If client creation fails.
    """
    # Input validation
    if not realm or not realm.strip():
        raise ValueError("Realm cannot be empty")
    if not user_token or not user_token.strip():
        raise ValueError("User token cannot be empty")

    # Check cache
    cache_key = (realm, user_token)
    if cache and cache_key in _client_cache:
        logger.debug(f"Returning cached Quickbase client for realm: {realm}")
        return _client_cache[cache_key]

    # Create new client
    try:
        client = quickbase_api.client(realm=realm, user_token=user_token)
        logger.debug(f"Created Quickbase client for realm: {realm}")

        if cache:
            _client_cache[cache_key] = client

        return client
    except Exception as e:
        logger.error(f"Failed to create Quickbase client for realm {realm}: {e}")
        raise


def _reset_client_cache() -> None:
    """Clear the client cache. For testing only."""
    global _client_cache
    _client_cache = {}
