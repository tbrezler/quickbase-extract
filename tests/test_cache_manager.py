"""Unit tests for cache_manager module."""

import logging
import os
import time
from unittest.mock import MagicMock, patch

import pytest
from quickbase_extract.cache_manager import (
    CacheManager,
    _reset_cache_manager,
    ensure_cache_freshness,
    get_cache_manager,
)


class TestCacheManagerInit:
    """Tests for CacheManager initialization."""

    def test_init_local_environment(self, temp_cache_dir, monkeypatch):
        """Test initialization in local environment."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.is_lambda is False
        assert mgr.environment == "dev"
        assert mgr.cache_root == temp_cache_dir

    def test_init_lambda_environment(self, monkeypatch):
        """Test initialization in Lambda environment."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("ENV", "prod")

        mgr = CacheManager()

        assert mgr.is_lambda is True
        assert mgr.environment == "prod"
        assert "/tmp/quickbase-extract/data" in str(mgr.cache_root)

    def test_init_with_cache_root_env_var(self, temp_cache_dir, monkeypatch):
        """Test initialization with QUICKBASE_CACHE_ROOT env var."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("QUICKBASE_CACHE_ROOT", str(temp_cache_dir))

        mgr = CacheManager()

        assert mgr.cache_root == temp_cache_dir

    def test_init_explicit_cache_root_takes_precedence(self, temp_cache_dir, monkeypatch):
        """Test that explicit cache_root takes precedence over env var."""
        other_dir = temp_cache_dir / "other"
        other_dir.mkdir()

        monkeypatch.setenv("QUICKBASE_CACHE_ROOT", str(other_dir))

        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.cache_root == temp_cache_dir

    def test_init_creates_cache_root(self, temp_cache_dir):
        """Test that cache root is created if it doesn't exist."""
        nested_dir = temp_cache_dir / "nested" / "cache"
        assert not nested_dir.exists()

        # Create CacheManager - this should create the directory
        CacheManager(cache_root=nested_dir)

        assert nested_dir.exists()

    def test_init_s3_client_on_lambda(self, monkeypatch):
        """Test that S3 client is created only on Lambda."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            # Create CacheManager - this should call boto3.client
            CacheManager()
            mock_boto.assert_called_once_with("s3")

    def test_init_no_s3_client_locally(self, temp_cache_dir, monkeypatch):
        """Test that S3 client is not created locally."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            CacheManager(cache_root=temp_cache_dir)
            mock_boto.assert_not_called()


class TestCacheManagerPaths:
    """Tests for path methods."""

    def test_get_metadata_path(self, temp_cache_dir):
        """Test metadata path generation."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        path = mgr.get_metadata_path("My App", "My Table", "Python")

        assert path.name == "my_table_python.json"
        assert "report_metadata" in str(path)
        assert "my_app" in str(path)  # Now includes app subdirectory

    def test_get_data_path(self, temp_cache_dir):
        """Test data path generation."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        path = mgr.get_data_path("My App", "My Table", "Python")

        assert path.name == "my_table_python_data.json"
        assert "report_data" in str(path)
        assert "my_app" in str(path)  # Now includes app subdirectory

    def test_metadata_path_creates_parent_dirs(self, temp_cache_dir):
        """Test that metadata path creation makes parent directories."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        path = mgr.get_metadata_path("App", "Table", "Report")

        assert path.parent.exists()

    def test_data_path_creates_parent_dirs(self, temp_cache_dir):
        """Test that data path creation makes parent directories."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        path = mgr.get_data_path("App", "Table", "Report")

        assert path.parent.exists()

    def test_path_normalization(self, temp_cache_dir):
        """Test that paths are normalized (spaces to underscores, lowercase)."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        path = mgr.get_metadata_path("Data Lake", "Employee Appointments", "Aureus")

        assert "data_lake" in str(path)  # App subdirectory
        assert "employee_appointments" in str(path)
        assert "aureus" in str(path)


class TestCacheManagerFileOperations:
    """Tests for file read/write operations."""

    def test_write_file(self, temp_cache_dir):
        """Test writing a file."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        test_file = temp_cache_dir / "test.json"
        content = '{"key": "value"}'

        mgr.write_file(test_file, content)

        assert test_file.exists()
        assert test_file.read_text() == content

    def test_read_file(self, temp_cache_dir):
        """Test reading a file."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        test_file = temp_cache_dir / "test.json"
        content = '{"key": "value"}'
        test_file.write_text(content)

        result = mgr.read_file(test_file)

        assert result == content

    def test_read_file_not_found(self, temp_cache_dir):
        """Test error when reading non-existent file."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        test_file = temp_cache_dir / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            mgr.read_file(test_file)

    def test_write_file_creates_parent_dirs(self, temp_cache_dir):
        """Test that write_file creates parent directories."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        test_file = temp_cache_dir / "nested" / "deep" / "test.json"

        mgr.write_file(test_file, "content")

        assert test_file.parent.exists()

    def test_write_file_syncs_to_s3_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that write_file syncs to S3 on Lambda."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")
        monkeypatch.setenv("ENV", "dev")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mgr = CacheManager(cache_root=temp_cache_dir)
            test_file = temp_cache_dir / "test.json"
            mgr.write_file(test_file, "content")

            # S3 upload should be called
            mock_s3.upload_file.assert_called_once()

    def test_write_file_no_s3_sync_locally(self, temp_cache_dir, monkeypatch):
        """Test that write_file does not sync to S3 locally."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mgr = CacheManager(cache_root=temp_cache_dir)
            test_file = temp_cache_dir / "test.json"
            mgr.write_file(test_file, "content")

            mock_boto.assert_not_called()


class TestCacheManagerS3Sync:
    """Tests for S3 sync operations."""

    def test_sync_from_s3_only_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that sync_from_s3 only runs on Lambda."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mgr = CacheManager(cache_root=temp_cache_dir)
            mgr.sync_from_s3()

            # Should not call boto3 if not Lambda
            mock_boto.assert_not_called()

    def test_sync_from_s3_requires_cache_bucket(self, temp_cache_dir, monkeypatch, caplog):
        """Test that sync_from_s3 skips if CACHE_BUCKET not set."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.delenv("CACHE_BUCKET", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mgr = CacheManager(cache_root=temp_cache_dir)
            mgr.sync_from_s3()

            # S3 operations should not be called
            mock_s3.get_paginator.assert_not_called()
            assert "CACHE_BUCKET not set" in caplog.text

    def test_sync_from_s3_downloads_files(self, temp_cache_dir, monkeypatch):
        """Test that sync_from_s3 downloads files from S3."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")
        monkeypatch.setenv("ENV", "dev")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            # Mock paginator response
            mock_paginator = MagicMock()
            mock_s3.get_paginator.return_value = mock_paginator
            # The paginator returns a list of pages, each page has a "Contents" key
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "dev/report_metadata/app/table_report.json"},
                        {"Key": "dev/report_data/app/table_data.json"},
                    ]
                }
            ]

            mgr = CacheManager(cache_root=temp_cache_dir)
            mgr.sync_from_s3()

            # Should download each file
            assert mock_s3.download_file.call_count == 2

    def test_sync_from_s3_creates_directories(self, temp_cache_dir, monkeypatch, caplog):
        """Test that sync_from_s3 creates necessary directories."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")
        monkeypatch.setenv("ENV", "dev")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mock_paginator = MagicMock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "dev/report_metadata/app/table_report.json"},
                    ]
                }
            ]

            mgr = CacheManager(cache_root=temp_cache_dir)
            mgr.sync_from_s3()

            assert "Synced 1 files from S3" in caplog.text


class TestEnsureCacheFreshness:
    """Tests for ensure_cache_freshness orchestration function."""

    def test_cache_fresh_no_refresh_needed(self, temp_cache_dir, monkeypatch, caplog):
        """Test that no refresh occurs when cache is fresh."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("QUICKBASE_CACHE_ROOT", str(temp_cache_dir))

        # Reset singleton to use new cache root
        _reset_cache_manager()

        # Create metadata cache files
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        # Create data cache files
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        refresh_callback = MagicMock()

        caplog.set_level(logging.DEBUG)
        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        # Refresh should not be called
        refresh_callback.assert_not_called()
        assert "Cache is fresh" in caplog.text

    def test_metadata_empty_triggers_refresh(self, temp_cache_dir, monkeypatch):
        """Test that refresh is called when metadata cache is empty."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        # Create only data cache (metadata empty)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        # Refresh should be called
        refresh_callback.assert_called_once()

    def test_metadata_stale_triggers_refresh(self, temp_cache_dir, monkeypatch, caplog):
        """Test that refresh is called when metadata is stale."""

        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

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

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,  # 7 days
            data_stale_hours=24,
        )

        # Refresh should be called
        refresh_callback.assert_called_once()

    def test_data_empty_triggers_refresh(self, temp_cache_dir, monkeypatch):
        """Test that refresh is called when data cache is empty."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        # Create only metadata cache (data empty)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        # Refresh should be called
        refresh_callback.assert_called_once()

    def test_data_stale_triggers_refresh(self, temp_cache_dir, monkeypatch, caplog):
        """Test that refresh is called when data is stale."""

        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

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

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,  # 1 day
        )

        # Refresh should be called
        refresh_callback.assert_called_once()

    def test_force_refresh_skips_checks(self, temp_cache_dir, monkeypatch):
        """Test that force=True always refreshes regardless of cache state."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        # Create fresh cache (both metadata and data)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
            force=True,  # Force refresh
        )

        # Refresh should be called even though cache is fresh
        refresh_callback.assert_called_once()

    def test_force_cache_refresh_env_var(self, temp_cache_dir, monkeypatch):
        """Test that FORCE_CACHE_REFRESH env var forces refresh."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("FORCE_CACHE_REFRESH", "true")

        # Create fresh cache
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_report_data.json").write_text("{}")

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        # Refresh should be called due to env var
        refresh_callback.assert_called_once()

    def test_env_var_thresholds_override_defaults(self, temp_cache_dir, monkeypatch, caplog):
        """Test that METADATA_STALE_HOURS and DATA_STALE_HOURS env vars are used."""

        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("METADATA_STALE_HOURS", "1")  # 1 hour
        monkeypatch.setenv("DATA_STALE_HOURS", "1")  # 1 hour

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

        refresh_callback = MagicMock()

        # Don't provide thresholds - should read from env vars
        ensure_cache_freshness(refresh_callback=refresh_callback)

        # Should refresh because both caches are older than 1 hour
        refresh_callback.assert_called_once()

    def test_refresh_failure_logged_not_raised(self, temp_cache_dir, monkeypatch, caplog):
        """Test that refresh failure is logged but not re-raised."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        # Create empty cache (will trigger refresh)
        refresh_callback = MagicMock(side_effect=Exception("Refresh failed!"))

        # Should not raise
        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        # But should log the error
        assert "Cache refresh failed: Refresh failed!" in caplog.text

    def test_non_callable_refresh_callback_raises_error(self, temp_cache_dir, monkeypatch):
        """Test that non-callable refresh_callback raises ValueError."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        with pytest.raises(ValueError, match="refresh_callback must be callable"):
            ensure_cache_freshness(
                refresh_callback="not_a_function",
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

    def test_multiple_stale_caches_reported(self, temp_cache_dir, monkeypatch, caplog):
        """Test that all stale cache reasons are included in log."""

        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
        monkeypatch.setenv("ENV", "dev")

        # Create old metadata (10 days old)
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")

        old_time = time.time() - (10 * 24 * 3600)

        os.utime(metadata_file, (old_time, old_time))

        # Create old data (2 days old)
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "table_report_data.json"
        data_file.write_text("{}")

        old_time_data = time.time() - (2 * 24 * 3600)
        os.utime(data_file, (old_time_data, old_time_data))

        refresh_callback = MagicMock()

        ensure_cache_freshness(
            refresh_callback=refresh_callback,
            metadata_stale_hours=168,  # 7 days
            data_stale_hours=24,  # 1 day
        )


class TestGetCacheManagerSingleton:
    """Tests for get_cache_manager singleton."""

    def test_singleton_returns_same_instance(self, temp_cache_dir):
        """Test that get_cache_manager returns the same instance."""
        mgr1 = get_cache_manager(cache_root=temp_cache_dir)
        mgr2 = get_cache_manager()

        assert mgr1 is mgr2

    def test_singleton_cache_root_ignored_after_first_call(self, temp_cache_dir):
        """Test that cache_root is ignored on subsequent calls."""
        mgr1 = get_cache_manager(cache_root=temp_cache_dir)

        # Second call with different cache_root should return same instance
        other_dir = temp_cache_dir / "other"
        other_dir.mkdir()
        mgr2 = get_cache_manager(cache_root=other_dir)

        # Should be same instance with original cache_root
        assert mgr1 is mgr2
        assert mgr2.cache_root == temp_cache_dir
