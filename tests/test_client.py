"""Unit tests for client module."""

from unittest.mock import MagicMock, patch

import pytest
from quickbase_extract.client import _reset_client_cache, get_qb_client


class TestGetQbClient:
    """Tests for get_qb_client function."""

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_creation_success(self, mock_client_factory):
        """Test successful client creation."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        result = get_qb_client(realm="test.quickbase.com", user_token="token123")

        assert result == mock_client
        mock_client_factory.assert_called_once_with(realm="test.quickbase.com", user_token="token123")

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_caching(self, mock_client_factory):
        """Test that clients are cached and reused."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # First call
        result1 = get_qb_client(realm="test.quickbase.com", user_token="token123")
        # Second call with same credentials
        result2 = get_qb_client(realm="test.quickbase.com", user_token="token123")

        # Should return same instance
        assert result1 is result2
        # Client factory should only be called once
        mock_client_factory.assert_called_once()

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_cache_bypass(self, mock_client_factory):
        """Test bypassing client cache."""
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_client_factory.side_effect = [mock_client1, mock_client2]

        # First call
        result1 = get_qb_client(realm="test.quickbase.com", user_token="token123", cache=True)
        # Second call with cache=False
        result2 = get_qb_client(realm="test.quickbase.com", user_token="token123", cache=False)

        # Should return different instances
        assert result1 is not result2
        # Client factory should be called twice
        assert mock_client_factory.call_count == 2

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_creation_with_different_realm(self, mock_client_factory):
        """Test client creation with different realm."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        get_qb_client(realm="custom.quickbase.com", user_token="token456")

        mock_client_factory.assert_called_once_with(realm="custom.quickbase.com", user_token="token456")

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_different_credentials_create_different_clients(self, mock_client_factory):
        """Test that different credentials create separate cached clients."""
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_client_factory.side_effect = [mock_client1, mock_client2]

        result1 = get_qb_client(realm="test.quickbase.com", user_token="token123")
        result2 = get_qb_client(realm="test.quickbase.com", user_token="token456")

        # Should be different instances
        assert result1 is not result2
        assert mock_client_factory.call_count == 2

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_creation_failure(self, mock_client_factory):
        """Test client creation failure."""
        mock_client_factory.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            get_qb_client(realm="test.quickbase.com", user_token="token123")

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_empty_realm_raises_error(self, mock_client_factory):
        """Test that empty realm raises ValueError."""
        with pytest.raises(ValueError, match="Realm cannot be empty"):
            get_qb_client(realm="", user_token="token123")

        mock_client_factory.assert_not_called()

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_empty_token_raises_error(self, mock_client_factory):
        """Test that empty token raises ValueError."""
        with pytest.raises(ValueError, match="User token cannot be empty"):
            get_qb_client(realm="test.quickbase.com", user_token="")

        mock_client_factory.assert_not_called()

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_whitespace_realm_raises_error(self, mock_client_factory):
        """Test that whitespace-only realm raises ValueError."""
        with pytest.raises(ValueError, match="Realm cannot be empty"):
            get_qb_client(realm="   ", user_token="token123")

        mock_client_factory.assert_not_called()

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_logs_on_success(self, mock_client_factory, caplog):
        """Test logging on successful client creation."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        get_qb_client(realm="test.quickbase.com", user_token="token123")

        assert "Created Quickbase client" in caplog.text
        assert "test.quickbase.com" in caplog.text

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_logs_cached_client(self, mock_client_factory, caplog):
        """Test logging when returning cached client."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # First call
        get_qb_client(realm="test.quickbase.com", user_token="token123")
        caplog.clear()

        # Second call (cached)
        get_qb_client(realm="test.quickbase.com", user_token="token123")

        assert "Returning cached" in caplog.text

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_logs_on_failure(self, mock_client_factory, caplog):
        """Test logging on client creation failure."""
        mock_client_factory.side_effect = Exception("Auth failed")

        with pytest.raises(Exception):
            get_qb_client(realm="test.quickbase.com", user_token="invalid")

        assert "Failed to create Quickbase client" in caplog.text


class TestResetClientCache:
    """Tests for _reset_client_cache function."""

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_reset_clears_cache(self, mock_client_factory):
        """Test that reset clears the client cache."""
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_client_factory.side_effect = [mock_client1, mock_client2]

        # Create and cache a client
        result1 = get_qb_client(realm="test.quickbase.com", user_token="token123")

        # Reset cache
        _reset_client_cache()

        # Create client again
        result2 = get_qb_client(realm="test.quickbase.com", user_token="token123")

        # Should be different instances
        assert result1 is not result2
        assert mock_client_factory.call_count == 2
