"""Unit tests for cache_manager module."""

import os
import time
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

        # Create CacheManager - this should create the directory
        CacheManager(cache_root=nested_dir)

        assert nested_dir.exists()

    def test_init_s3_client_on_lambda(self, temp_cache_dir, monkeypatch):
        """Test that S3 client is created only on Lambda."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            # Create CacheManager - this should call boto3.client
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
                        {"Key": "project/dev/cache/report_metadata/app/table_report.json"},
                        {"Key": "project/dev/cache/report_data/app/table_data.json"},
                    ]
                }
            ]

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            mgr.sync_from_s3()

            # Should download each file
            assert mock_s3.download_file.call_count == 2

    def test_sync_from_s3_creates_directories(self, temp_cache_dir, monkeypatch, caplog):
        """Test that sync_from_s3 creates necessary directories."""
        monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "test-function")
        monkeypatch.setenv("CACHE_BUCKET", "my-bucket")

        with patch("quickbase_extract.cache_manager.boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            mock_paginator = MagicMock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "project/dev/cache/report_metadata/app/table_report.json"},
                    ]
                }
            ]

            mgr = CacheManager(
                cache_root=temp_cache_dir,
                s3_bucket="my-bucket",
                s3_prefix="project/dev/cache",
            )
            mgr.sync_from_s3()

            assert "Synced 1 files from S3" in caplog.text


class TestCacheManagerCacheChecks:
    """Tests for cache state checking methods."""

    def test_is_cache_empty_metadata(self, temp_cache_dir):
        """Test is_cache_empty for metadata."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        # Empty initially
        assert mgr.is_cache_empty("metadata") is True

        # Create a metadata file
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        (metadata_dir / "table_report.json").write_text("{}")

        # Not empty now
        assert mgr.is_cache_empty("metadata") is False

    def test_is_cache_empty_data(self, temp_cache_dir):
        """Test is_cache_empty for data."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        # Empty initially
        assert mgr.is_cache_empty("data") is True

        # Create a data file
        data_dir = temp_cache_dir / "report_data" / "app"
        data_dir.mkdir(parents=True)
        (data_dir / "table_data.json").write_text("[]")

        # Not empty now
        assert mgr.is_cache_empty("data") is False

    def test_is_cache_empty_invalid_type(self, temp_cache_dir):
        """Test is_cache_empty with invalid type."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(ValueError, match="cache_type must be"):
            mgr.is_cache_empty("invalid")

    def test_get_cache_age_hours(self, temp_cache_dir):
        """Test get_cache_age_hours returns correct age."""
        mgr = CacheManager(cache_root=temp_cache_dir)

        # Create a metadata file
        metadata_dir = temp_cache_dir / "report_metadata" / "app"
        metadata_dir.mkdir(parents=True)
        metadata_file = metadata_dir / "table_report.json"
        metadata_file.write_text("{}")

        # Set modification time to 2 hours ago
        old_time = time.time() - (2 * 3600)
        os.utime(metadata_file, (old_time, old_time))

        age = mgr.get_cache_age_hours("metadata")
        assert 1.9 < age < 2.1  # Allow small margin

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
            mgr.get_cache_age_hours("invalid")
