"""Unit tests for cache_orchestration module."""

import logging
import os
import time
from unittest.mock import MagicMock, patch

from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.cache_orchestration import ensure_cache_freshness


class TestEnsureCacheFreshness:
    """Tests for ensure_cache_freshness orchestration function."""

    def test_cache_fresh_no_refresh_needed(self, temp_cache_dir, monkeypatch, caplog):
        """Test that no refresh occurs when both caches are fresh."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        # Create fresh data cache
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = []

        caplog.set_level(logging.DEBUG)
        ensure_cache_freshness(
            client=mock_client,
            report_configs=report_configs,
            cache_mgr=cache_mgr,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        assert "Cache is fresh" in caplog.text

    def test_metadata_empty_triggers_refresh(self, temp_cache_dir, monkeypatch):
        """Test that refresh is called when metadata cache is empty."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create only data cache (metadata empty)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_refresh:
            ensure_cache_freshness(
                client=mock_client,
                report_configs=report_configs,
                cache_mgr=cache_mgr,
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

            # Metadata refresh should be called
            mock_refresh.assert_called_once()

    def test_metadata_stale_triggers_refresh(self, temp_cache_dir, monkeypatch, caplog):
        """Test that refresh is called when metadata is stale."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create old metadata files (10 days old)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")

        # Set modification time to 10 days ago
        old_time = time.time() - (10 * 24 * 3600)
        os.utime(metadata_file, (old_time, old_time))

        # Create fresh data cache
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_refresh:
            ensure_cache_freshness(
                client=mock_client,
                report_configs=report_configs,
                cache_mgr=cache_mgr,
                metadata_stale_hours=168,  # 7 days
                data_stale_hours=24,
            )

            # Metadata refresh should be called
            mock_refresh.assert_called_once()
            assert "metadata stale" in caplog.text.lower()

    def test_data_empty_triggers_refresh(self, temp_cache_dir, monkeypatch):
        """Test that refresh is called when data cache is empty."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create only metadata cache (data empty)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {"test": {}}

                ensure_cache_freshness(
                    client=mock_client,
                    report_configs=report_configs,
                    cache_mgr=cache_mgr,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                )

                # Data refresh should be called
                mock_data.assert_called_once()

    def test_data_stale_triggers_refresh(self, temp_cache_dir, monkeypatch, caplog):
        """Test that refresh is called when data is stale."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        # Create old data files (2 days old)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "table_report_data.json"
        data_file.write_text("{}")

        # Set modification time to 2 days ago
        old_time = time.time() - (2 * 24 * 3600)
        os.utime(data_file, (old_time, old_time))

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {"test": {}}

                ensure_cache_freshness(
                    client=mock_client,
                    report_configs=report_configs,
                    cache_mgr=cache_mgr,
                    metadata_stale_hours=168,
                    data_stale_hours=24,  # 1 day
                )

                # Data refresh should be called
                mock_data.assert_called_once()
                assert "data stale" in caplog.text.lower()

    def test_both_stale_refreshes_both(self, temp_cache_dir, monkeypatch):
        """Test that both caches are refreshed when both are stale."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create old metadata (10 days old)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")
        old_time_meta = time.time() - (10 * 24 * 3600)
        os.utime(metadata_file, (old_time_meta, old_time_meta))

        # Create old data (2 days old)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "table_report_data.json"
        data_file.write_text("{}")
        old_time_data = time.time() - (2 * 24 * 3600)
        os.utime(data_file, (old_time_data, old_time_data))

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {"test": {}}

                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                        metadata_stale_hours=168,  # 7 days
                        data_stale_hours=24,  # 1 day
                    )

                    # Both should be called
                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_force_refresh_skips_checks(self, temp_cache_dir, monkeypatch):
        """Test that force=True always refreshes regardless of cache state."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh cache (both metadata and data)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {"test": {}}

                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                        force=True,  # Force refresh
                    )

                    # Both should be called even though cache is fresh
                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_force_cache_refresh_env_var(self, temp_cache_dir, monkeypatch):
        """Test that FORCE_CACHE_REFRESH env var forces refresh."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("FORCE_CACHE_REFRESH", "true")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {"test": {}}

                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                    )

                    # Both should be called due to env var
                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_env_var_thresholds_override_defaults(self, temp_cache_dir, monkeypatch):
        """Test that METADATA_STALE_HOURS and DATA_STALE_HOURS env vars are used."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("METADATA_STALE_HOURS", "1")  # 1 hour
        monkeypatch.setenv("DATA_STALE_HOURS", "1")  # 1 hour

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create metadata files (2 hours old)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")
        old_time = time.time() - (2 * 3600)
        os.utime(metadata_file, (old_time, old_time))

        # Create data files (2 hours old)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "table_report_data.json"
        data_file.write_text("{}")
        os.utime(data_file, (old_time, old_time))

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {"test": {}}

                    # Don't provide thresholds - should read from env vars
                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                    )

                    # Should refresh because both caches are older than 1 hour
                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_metadata_refresh_failure_logged_not_raised(self, temp_cache_dir, monkeypatch, caplog):
        """Test that metadata refresh failure is logged but not re-raised."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create empty metadata cache (will trigger refresh)
        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            mock_meta.side_effect = Exception("Metadata refresh failed!")

            # Should not raise
            ensure_cache_freshness(
                client=mock_client,
                report_configs=report_configs,
                cache_mgr=cache_mgr,
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

            # But should log the error
            assert "Metadata cache refresh failed" in caplog.text

    def test_data_refresh_failure_logged_not_raised(self, temp_cache_dir, monkeypatch, caplog):
        """Test that data refresh failure is logged but not re-raised."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata but empty data cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {"test": {}}
                mock_data.side_effect = Exception("Data refresh failed!")

                # Should not raise
                ensure_cache_freshness(
                    client=mock_client,
                    report_configs=report_configs,
                    cache_mgr=cache_mgr,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                )

                # But should log the error
                assert "Data cache refresh failed" in caplog.text

    def test_metadata_fails_data_still_attempts(self, temp_cache_dir, monkeypatch, caplog):
        """Test that data refresh is attempted even if metadata refresh fails."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create empty caches (both will trigger refresh)
        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_meta.side_effect = Exception("Metadata failed!")
                    mock_load.return_value = {"test": {}}

                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                    )

                    # Data refresh should still be attempted
                    mock_data.assert_called_once()
                    assert "Metadata cache refresh failed" in caplog.text

    def test_only_metadata_stale_only_metadata_refreshed(self, temp_cache_dir, monkeypatch):
        """Test that only metadata is refreshed when only metadata is stale."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create old metadata (10 days old)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")
        old_time = time.time() - (10 * 24 * 3600)
        os.utime(metadata_file, (old_time, old_time))

        # Create fresh data cache
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                ensure_cache_freshness(
                    client=mock_client,
                    report_configs=report_configs,
                    cache_mgr=cache_mgr,
                    metadata_stale_hours=168,  # 7 days
                    data_stale_hours=24,  # 1 day
                )

                # Only metadata refresh should be called
                mock_meta.assert_called_once()
                mock_data.assert_not_called()

    def test_only_data_stale_only_data_refreshed(self, temp_cache_dir, monkeypatch):
        """Test that only data is refreshed when only data is stale."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        # Create old data (2 days old)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "table_report_data.json"
        data_file.write_text("{}")
        old_time = time.time() - (2 * 24 * 3600)
        os.utime(data_file, (old_time, old_time))

        mock_client = MagicMock()
        report_configs = [{"Description": "test"}]

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {"test": {}}

                    ensure_cache_freshness(
                        client=mock_client,
                        report_configs=report_configs,
                        cache_mgr=cache_mgr,
                        metadata_stale_hours=168,  # 7 days
                        data_stale_hours=24,  # 1 day
                    )

                    # Only data refresh should be called
                    mock_meta.assert_not_called()
                    mock_data.assert_called_once()
