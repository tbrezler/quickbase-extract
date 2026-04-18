"""Unit tests for cache_sync module."""

from unittest.mock import MagicMock, patch

import pytest
from quickbase_extract.cache_sync import _reset_cache_sync, is_cache_synced, sync_from_s3_once


class TestSyncFromS3Once:
    """Tests for sync_from_s3_once function."""

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_sync_on_first_call(self, mock_get_cache_mgr, caplog):
        """Test that sync occurs on first call."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        sync_from_s3_once()

        mock_mgr.sync_from_s3.assert_called_once()
        assert "Cache synced from S3" in caplog.text

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_no_sync_on_second_call(self, mock_get_cache_mgr, caplog):
        """Test that sync is skipped on subsequent calls."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        # First call
        sync_from_s3_once()
        mock_mgr.sync_from_s3.reset_mock()
        caplog.clear()

        # Second call
        sync_from_s3_once()

        mock_mgr.sync_from_s3.assert_not_called()
        assert "already synced" in caplog.text

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_force_sync(self, mock_get_cache_mgr):
        """Test that force=True bypasses sync flag."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        # First call
        sync_from_s3_once()

        # Second call with force=True
        sync_from_s3_once(force=True)

        # Should be called twice
        assert mock_mgr.sync_from_s3.call_count == 2

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_sync_failure_raises_exception(self, mock_get_cache_mgr):
        """Test that sync failure raises exception."""
        mock_mgr = MagicMock()
        mock_mgr.sync_from_s3.side_effect = Exception("S3 error")
        mock_get_cache_mgr.return_value = mock_mgr

        with pytest.raises(Exception, match="S3 error"):
            sync_from_s3_once()

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_logs_success(self, mock_get_cache_mgr, caplog):
        """Test that successful sync is logged."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        sync_from_s3_once()

        assert "Cache synced from S3" in caplog.text


class TestIsCacheSynced:
    """Tests for is_cache_synced function."""

    def test_returns_false_initially(self):
        """Test that is_cache_synced returns False initially."""
        assert is_cache_synced() is False

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_returns_true_after_sync(self, mock_get_cache_mgr):
        """Test that is_cache_synced returns True after sync."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        sync_from_s3_once()

        assert is_cache_synced() is True

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_returns_false_after_reset(self, mock_get_cache_mgr):
        """Test that is_cache_synced returns False after reset."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        sync_from_s3_once()
        assert is_cache_synced() is True

        _reset_cache_sync()
        assert is_cache_synced() is False


class TestResetCacheSync:
    """Tests for _reset_cache_sync function."""

    @patch("quickbase_extract.cache_sync.get_cache_manager")
    def test_reset_allows_resync(self, mock_get_cache_mgr):
        """Test that reset allows syncing again."""
        mock_mgr = MagicMock()
        mock_get_cache_mgr.return_value = mock_mgr

        # First sync
        sync_from_s3_once()
        assert mock_mgr.sync_from_s3.call_count == 1

        # Reset
        _reset_cache_sync()

        # Should be able to sync again
        sync_from_s3_once()
        assert mock_mgr.sync_from_s3.call_count == 2
