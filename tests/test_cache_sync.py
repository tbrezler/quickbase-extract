"""Unit tests for cache_sync module."""

from unittest.mock import MagicMock, patch

import pytest

from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.cache_sync import (
    _reset_cache_sync,
    complete_cache_refresh,
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


class TestCompleteRefresh:
    """Tests for complete_cache_refresh function."""

    def test_no_refresh_when_all_flags_false(self, temp_cache_dir, caplog):
        """Test that no refresh occurs when all flags are False."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness") as mock_ensure,
            patch("quickbase_extract.cache_sync.sync_from_s3_once") as mock_sync,
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=MagicMock(),
                report_configs=[],
                force_all=False,
                force_metadata=False,
                force_data=False,
            )

            mock_ensure.assert_not_called()
            mock_sync.assert_not_called()
            assert "No cache refresh flags set" in caplog.text

    def test_force_all_true_clears_both_directories(self, temp_cache_dir, caplog):
        """Test that force_all=True clears both metadata and data directories."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create test files in both directories
        metadata_dir = temp_cache_dir / "report_metadata" / "test_app"
        data_dir = temp_cache_dir / "report_data" / "test_app"
        metadata_dir.mkdir(parents=True)
        data_dir.mkdir(parents=True)
        (metadata_dir / "test.json").write_text('{"test": "data"}')
        (data_dir / "test_data.json").write_text('{"test": "data"}')

        assert metadata_dir.exists()
        assert data_dir.exists()

        mock_client = MagicMock()
        mock_report_configs = [MagicMock()]

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness") as mock_ensure,
            patch("quickbase_extract.cache_sync.sync_from_s3_once") as mock_sync,
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=mock_client,
                report_configs=mock_report_configs,
                force_all=True,
            )

            # Verify directories are deleted
            assert not metadata_dir.exists()
            assert not data_dir.exists()

            # Verify functions called with correct flags
            mock_ensure.assert_called_once()
            call_kwargs = mock_ensure.call_args[1]
            assert call_kwargs["force_all"] is True
            # Individual flags are passed as False, but force_all=True overrides them
            assert call_kwargs["force_metadata"] is False
            assert call_kwargs["force_data"] is False

            mock_sync.assert_called_once_with(cache_mgr, force=True)
            assert "Complete cache refresh finished for metadata, data:" in caplog.text

    def test_force_metadata_true_clears_only_metadata(self, temp_cache_dir, caplog):
        """Test that force_metadata=True clears only metadata directory."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create test files in both directories
        metadata_dir = temp_cache_dir / "report_metadata" / "test_app"
        data_dir = temp_cache_dir / "report_data" / "test_app"
        metadata_dir.mkdir(parents=True)
        data_dir.mkdir(parents=True)
        (metadata_dir / "test.json").write_text('{"test": "data"}')
        (data_dir / "test_data.json").write_text('{"test": "data"}')

        mock_client = MagicMock()
        mock_report_configs = [MagicMock()]

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness") as mock_ensure,
            patch("quickbase_extract.cache_sync.sync_from_s3_once"),
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=mock_client,
                report_configs=mock_report_configs,
                force_metadata=True,
            )

            # Verify only metadata directory is deleted
            assert not metadata_dir.exists()
            assert data_dir.exists()  # Data directory should still exist

            # Verify functions called with correct flags
            mock_ensure.assert_called_once()
            call_kwargs = mock_ensure.call_args[1]
            assert call_kwargs["force_metadata"] is True
            assert call_kwargs["force_data"] is False
            assert call_kwargs["force_all"] is False

            assert "Complete cache refresh finished for metadata:" in caplog.text

    def test_force_data_true_clears_only_data(self, temp_cache_dir, caplog):
        """Test that force_data=True clears only data directory."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create test files in both directories
        metadata_dir = temp_cache_dir / "report_metadata" / "test_app"
        data_dir = temp_cache_dir / "report_data" / "test_app"
        metadata_dir.mkdir(parents=True)
        data_dir.mkdir(parents=True)
        (metadata_dir / "test.json").write_text('{"test": "data"}')
        (data_dir / "test_data.json").write_text('{"test": "data"}')

        mock_client = MagicMock()
        mock_report_configs = [MagicMock()]

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness") as mock_ensure,
            patch("quickbase_extract.cache_sync.sync_from_s3_once"),
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=mock_client,
                report_configs=mock_report_configs,
                force_data=True,
            )

            # Verify only data directory is deleted
            assert metadata_dir.exists()  # Metadata directory should still exist
            assert not data_dir.exists()

            # Verify functions called with correct flags
            mock_ensure.assert_called_once()
            call_kwargs = mock_ensure.call_args[1]
            assert call_kwargs["force_metadata"] is False
            assert call_kwargs["force_data"] is True
            assert call_kwargs["force_all"] is False

            assert "Complete cache refresh finished for data:" in caplog.text

    def test_force_all_overrides_individual_flags(self, temp_cache_dir):
        """Test that force_all=True overrides individual flags."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create test files in both directories
        metadata_dir = temp_cache_dir / "report_metadata" / "test_app"
        data_dir = temp_cache_dir / "report_data" / "test_app"
        metadata_dir.mkdir(parents=True)
        data_dir.mkdir(parents=True)
        (metadata_dir / "test.json").write_text('{"test": "data"}')
        (data_dir / "test_data.json").write_text('{"test": "data"}')

        mock_client = MagicMock()
        mock_report_configs = [MagicMock()]

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness") as mock_ensure,
            patch("quickbase_extract.cache_sync.sync_from_s3_once"),
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=mock_client,
                report_configs=mock_report_configs,
                force_all=True,
                force_metadata=False,  # Even though these are False
                force_data=False,  # force_all should override
            )

            # Verify both directories deleted
            assert not metadata_dir.exists()
            assert not data_dir.exists()

            # Verify ensure_cache_freshness called with force_all=True
            call_kwargs = mock_ensure.call_args[1]
            assert call_kwargs["force_all"] is True

    def test_logs_starting_refresh(self, temp_cache_dir, caplog):
        """Test that cache refresh start is logged as warning."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness"),
            patch("quickbase_extract.cache_sync.sync_from_s3_once"),
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=MagicMock(),
                report_configs=[],
                force_all=True,
            )

            assert "Starting complete cache refresh for: metadata, data" in caplog.text

    def test_logs_completion(self, temp_cache_dir, caplog):
        """Test that cache refresh completion is logged as warning."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness"),
            patch("quickbase_extract.cache_sync.sync_from_s3_once"),
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=MagicMock(),
                report_configs=[],
                force_metadata=True,
            )

            assert "Complete cache refresh finished for metadata:" in caplog.text

    def test_sync_flag_reset_on_refresh(self, temp_cache_dir):
        """Test that _CACHE_SYNCED flag is reset during refresh."""
        from quickbase_extract import cache_sync

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Manually set the flag to True
        cache_sync._CACHE_SYNCED = True

        assert is_cache_synced() is True

        with (
            patch("quickbase_extract.cache_sync.ensure_cache_freshness"),
            patch("quickbase_extract.cache_sync.sync_from_s3_once") as mock_sync,
        ):
            complete_cache_refresh(
                cache_manager=cache_mgr,
                client=MagicMock(),
                report_configs=[],
                force_all=True,
            )

            # Verify sync_from_s3_once was called with force=True
            mock_sync.assert_called_once_with(cache_mgr, force=True)
