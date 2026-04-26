"""Unit tests for cache_sync module."""

from unittest.mock import patch

import pytest
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.cache_sync import (
    _reset_cache_sync,
    is_cache_synced,
    sync_from_s3_once,
)


class TestSyncFromS3Once:
    """Tests for sync_from_s3_once function."""

    def test_sync_on_first_call(self, temp_cache_dir, caplog):
        """Test that sync occurs on first call."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3") as mock_sync:
            sync_from_s3_once(cache_mgr)

            mock_sync.assert_called_once()
            assert "Cache synced from S3" in caplog.text

    def test_no_sync_on_second_call(self, temp_cache_dir, caplog):
        """Test that sync is skipped on subsequent calls."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3") as mock_sync:
            # First call
            sync_from_s3_once(cache_mgr)
            mock_sync.reset_mock()
            caplog.clear()

            # Second call
            sync_from_s3_once(cache_mgr)

            mock_sync.assert_not_called()
            assert "already synced" in caplog.text

    def test_force_sync(self, temp_cache_dir):
        """Test that force=True bypasses sync flag."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3") as mock_sync:
            # First call
            sync_from_s3_once(cache_mgr)

            # Second call with force=True
            sync_from_s3_once(cache_mgr, force=True)

            # Should be called twice
            assert mock_sync.call_count == 2

    def test_sync_failure_raises_exception(self, temp_cache_dir):
        """Test that sync failure raises exception."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3", side_effect=Exception("S3 error")):
            with pytest.raises(Exception, match="S3 error"):
                sync_from_s3_once(cache_mgr)

    def test_logs_success(self, temp_cache_dir, caplog):
        """Test that successful sync is logged."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3"):
            sync_from_s3_once(cache_mgr)

            assert "Cache synced from S3" in caplog.text


class TestIsCacheSynced:
    """Tests for is_cache_synced function."""

    def test_returns_false_initially(self):
        """Test that is_cache_synced returns False initially."""
        _reset_cache_sync()  # Make sure we start fresh
        assert is_cache_synced() is False

    def test_returns_true_after_sync(self, temp_cache_dir):
        """Test that is_cache_synced returns True after sync."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3"):
            sync_from_s3_once(cache_mgr)

            assert is_cache_synced() is True

    def test_returns_false_after_reset(self, temp_cache_dir):
        """Test that is_cache_synced returns False after reset."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3"):
            sync_from_s3_once(cache_mgr)
            assert is_cache_synced() is True

            _reset_cache_sync()
            assert is_cache_synced() is False


class TestResetCacheSync:
    """Tests for _reset_cache_sync function."""

    def test_reset_allows_resync(self, temp_cache_dir):
        """Test that reset allows syncing again."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with patch.object(cache_mgr, "sync_from_s3") as mock_sync:
            # First sync
            sync_from_s3_once(cache_mgr)
            assert mock_sync.call_count == 1

            # Reset
            _reset_cache_sync()

            # Should be able to sync again
            sync_from_s3_once(cache_mgr)
            assert mock_sync.call_count == 2
            assert mock_sync.call_count == 2
