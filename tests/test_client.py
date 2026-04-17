"""Unit tests for client module."""

from unittest.mock import MagicMock, patch

import pytest
from quickbase_extract.client import get_qb_client


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
    def test_client_creation_with_different_realm(self, mock_client_factory):
        """Test client creation with different realm."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        get_qb_client(realm="custom.quickbase.com", user_token="token456")

        mock_client_factory.assert_called_once_with(realm="custom.quickbase.com", user_token="token456")

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_client_creation_failure(self, mock_client_factory):
        """Test client creation failure."""
        mock_client_factory.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            get_qb_client(realm="test.quickbase.com", user_token="token123")

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_logs_on_success(self, mock_client_factory, caplog):
        """Test logging on successful client creation."""
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        get_qb_client(realm="test.quickbase.com", user_token="token123")

        assert "Created Quickbase client" in caplog.text
        assert "test.quickbase.com" in caplog.text

    @patch("quickbase_extract.client.quickbase_api.client")
    def test_logs_on_failure(self, mock_client_factory, caplog):
        """Test logging on client creation failure."""
        mock_client_factory.side_effect = Exception("Auth failed")

        with pytest.raises(Exception):
            get_qb_client(realm="test.quickbase.com", user_token="invalid")

        assert "Failed to create Quickbase client" in caplog.text
