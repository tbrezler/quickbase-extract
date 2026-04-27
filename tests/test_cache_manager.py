"""Unit tests for cache_manager module."""

import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from quickbase_extract.cache_manager import CacheManager


class TestCacheManagerInit:
    """Tests for CacheManager initialization."""

    def test_init_requires_cache_root(self):
        """Test that cache_root is required."""
        with pytest.raises(TypeError):
            CacheManager()

    def test_init_with_cache_root(self, temp_cache_dir):
        """Test initialization with cache_root."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.cache_root == temp_cache_dir
        assert mgr.cache_root.exists()

    def test_init_local_environment(self, temp_cache_dir, monkeypatch):
        """Test initialization in local environment."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.is_lambda is False
        assert mgr.s3_client is None

    def test_init_lambda_environment(self, temp_cache_dir, monkeypatch):
        """Test initialization in Lambda environment."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )

            assert mgr.is_lambda is True
            mock_boto.assert_called_once_with("s3")

    def test_init_creates_cache_root(self, temp_cache_dir):
        """Test that cache root is created if it doesn't exist."""
        nested_dir = temp_cache_dir / "nested" / "cache"
        assert not nested_dir.exists()

        CacheManager(cache_root=nested_dir)

        assert nested_dir.exists()

    def test_init_s3_client_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that S3 client is created only on Lambda."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            mock_boto.assert_called_once_with("s3")

    def test_init_no_s3_client_locally(self, temp_cache_dir, monkeypatch):
        """Test that S3 client is not created locally."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            CacheManager(cache_root=temp_cache_dir)
            mock_boto.assert_not_called()

    def test_init_validates_s3_prefix_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that s3_prefix is required when using S3 on Lambda."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")

        with patch("quickbase_extract.cache_manager.boto3.client"):
            with pytest.raises(ValueError, match="s3_prefix is required"):
                CacheManager(
                    cache_root=temp_cache_dir,
                    s3_bucket="my-bucket",
                    s3_prefix=None,
                )


class TestCacheManagerPaths:
    """Tests for path methods."""

    def test_get_metadata_path(self, temp_cache_dir):
        """Test metadata path generation."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        path = mgr.get_metadata_path("My App", "My Table", "Python")

        assert path.name == "my_table_python.json"
        assert "report_metadata" in str(path)
        assert "my_app" in str(path)

    def test_get_data_path(self, temp_cache_dir):
        """Test data path generation."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        path = mgr.get_data_path("My App", "My Table", "Python")

        assert path.name == "my_table_python_data.json"
        assert "report_data" in str(path)
        assert "my_app" in str(path)

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

        assert "data_lake" in str(path)
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

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            test_file = temp_cache_dir / "test.json"
            mgr.write_file(test_file, "content")

            mock_s3.upload_file.assert_called_once()

    def test_write_file_no_s3_sync_locally(self, temp_cache_dir, monkeypatch):
        """Test that write_file does not sync to S3 locally."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mgr = CacheManager(cache_root=temp_cache_dir)
            test_file = temp_cache_dir / "test.json"
            mgr.write_file(test_file, "content")

            mock_boto.assert_not_called()

    def test_write_file_raises_on_s3_failure(self, temp_cache_dir, monkeypatch):
        """Test that write_file propagates S3 upload errors."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3
            mock_s3.upload_file.side_effect = Exception("S3 upload failed")

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            test_file = temp_cache_dir / "test.json"

            with pytest.raises(Exception, match="S3 upload failed"):
                mgr.write_file(test_file, "content")

            # File should still be written locally
            assert test_file.exists()


class TestCacheManagerS3Sync:
    """Tests for S3 sync operations."""

    def test_sync_from_s3_only_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that sync_from_s3 only runs on Lambda."""
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mgr = CacheManager(cache_root=temp_cache_dir)
            mgr.sync_from_s3()

            mock_boto.assert_not_called()

    def test_sync_from_s3_requires_cache_bucket(self, temp_cache_dir, monkeypatch, caplog):
        """Test that sync_from_s3 skips if CACHE_BUCKET not set."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.delenv("CACHE_BUCKET", raising=False)

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mgr = CacheManager(cache_root=temp_cache_dir)

            with caplog.at_level(logging.DEBUG):
                mgr.sync_from_s3()

            mock_s3.get_paginator.assert_not_called()
            assert "CACHE_BUCKET not set" in caplog.text

    def test_sync_from_s3_downloads_files(self, temp_cache_dir, monkeypatch):
        """Test that sync_from_s3 downloads files from S3."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mock_last_modified = datetime(2025, 1, 1, tzinfo=UTC)

            mock_paginator = MagicMock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {
                            "Key": "project/dev/cache/report_metadata/app/table_report.json",
                            "LastModified": mock_last_modified,
                        },
                        {
                            "Key": "project/dev/cache/report_data/app/table_data.json",
                            "LastModified": mock_last_modified,
                        },
                    ]
                }
            ]

            def create_file(bucket, key, local_path):
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                Path(local_path).write_text("{}")

            mock_s3.download_file.side_effect = create_file

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            mgr.sync_from_s3()

            assert mock_s3.download_file.call_count == 2

    def test_sync_from_s3_creates_directories(self, temp_cache_dir, monkeypatch, caplog):
        """Test that sync_from_s3 creates necessary directories."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mock_last_modified = datetime(2025, 1, 1, tzinfo=UTC)

            mock_paginator = MagicMock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {
                            "Key": "project/dev/cache/report_metadata/app/table_report.json",
                            "LastModified": mock_last_modified,
                        },
                    ]
                }
            ]

            def create_file(bucket, key, local_path):
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                Path(local_path).write_text("{}")

            mock_s3.download_file.side_effect = create_file

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )

            with caplog.at_level(logging.INFO):
                mgr.sync_from_s3()

            assert "Synced 1 files from S3" in caplog.text

    def test_get_cache_age_hours_with_old_files(self, temp_cache_dir):
        """Verify cache age is calculated correctly from file mtime."""

        # Create cache directory structure
        cache_dir = temp_cache_dir / "report_metadata" / "app"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Create a test file
        test_file = cache_dir / "table_report.json"
        test_file.write_text("{}")

        # Set its mtime to 48 hours ago
        past_time = time.time() - (48 * 3600)
        os.utime(test_file, (past_time, past_time))

        cache_manager = CacheManager(cache_root=temp_cache_dir)

        # Get cache age
        age = cache_manager.get_cache_age_hours("metadata")

        # Should be approximately 48 hours (within 1 hour tolerance for test execution time)
        assert 47 < age < 49


class TestCacheManagerCacheChecks:
    """Tests for cache state checking methods."""

    def test_is_cache_empty_metadata(self, temp_cache_dir):
        """Test is_cache_empty for metadata."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.is_cache_empty("metadata") is True

        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        assert mgr.is_cache_empty("metadata") is False

    def test_is_cache_empty_data(self, temp_cache_dir):
        """Test is_cache_empty for data."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.is_cache_empty("data") is True

        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_data.json").write_text("[]")

        assert mgr.is_cache_empty("data") is False

    def test_is_cache_empty_invalid_type(self, temp_cache_dir):
        """Test is_cache_empty with invalid type."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(ValueError, match="cache_type must be"):
            mgr.is_cache_empty("invalid")

    def test_get_cache_age_hours(self, temp_cache_dir):
        """Test get_cache_age_hours returns correct age."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")

        old_time = time.time() - (2 * 3600)
        os.utime(metadata_file, (old_time, old_time))

        age = mgr.get_cache_age_hours("metadata")
        assert 1.9 < age < 2.1

    def test_get_cache_age_hours_empty_cache(self, temp_cache_dir):
        """Test get_cache_age_hours returns 0 for empty cache."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        age = mgr.get_cache_age_hours("metadata")
        assert age == 0

    def test_get_cache_age_hours_invalid_type(self, temp_cache_dir):
        """Test get_cache_age_hours with invalid type."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(ValueError, match="cache_type must be"):
            mgr.get_cache_age_hours("invalid")


class TestCacheManagerReportChecks:
    """Tests for report-level existence checks."""

    def test_has_report_metadata_true(self, temp_cache_dir):
        """Test has_report_metadata returns True when file exists."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        path = mgr.get_metadata_path("My App", "My Table", "Python")
        path.write_text("{}")

        assert mgr.has_report_metadata("My App", "My Table", "Python") is True

    def test_has_report_metadata_false(self, temp_cache_dir):
        """Test has_report_metadata returns False when file missing."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.has_report_metadata("My App", "My Table", "Python") is False

    def test_has_report_data_true(self, temp_cache_dir):
        """Test has_report_data returns True when file exists."""
        mgr = CacheManager(cache_root=temp_cache_dir)
        path = mgr.get_data_path("My App", "My Table", "Python")
        path.write_text("[]")

        assert mgr.has_report_data("My App", "My Table", "Python") is True

    def test_has_report_data_false(self, temp_cache_dir):
        """Test has_report_data returns False when file missing."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        assert mgr.has_report_data("My App", "My Table", "Python") is False
