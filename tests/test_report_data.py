"""Integration tests for report_data module."""

import json

import pytest
from quickbase_extract.cache_manager import get_cache_manager
from quickbase_extract.report_data import get_data, get_data_parallel, load_data
from quickbase_extract.report_metadata import get_report_metadata


class TestGetData:
    """Tests for get_data function."""

    def test_get_data_without_cache(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test getting data without caching."""
        # First cache metadata
        get_report_metadata(
            mock_qb_api,
            "Test App",
            "appXYZ",
            "Test Table",
            "Python",
            cache_root=temp_cache_dir,
        )

        # Get data without caching
        data = get_data(
            mock_qb_api,
            "Test Report",
            sample_report_configs,
            cache=False,
            cache_root=temp_cache_dir,
        )

        assert len(data) == 2
        assert data[0]["Name"] == "Alice"
        assert data[1]["Name"] == "Bob"

    def test_get_data_with_cache(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test getting data and caching it."""
        # First cache metadata
        get_report_metadata(
            mock_qb_api,
            "Test App",
            "appXYZ",
            "Test Table",
            "Python",
            cache_root=temp_cache_dir,
        )

        # Get data with caching
        data = get_data(
            mock_qb_api,
            "Test Report",
            sample_report_configs,
            cache=True,
            cache_root=temp_cache_dir,
        )

        # Verify data was cached
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        data_path = cache_mgr.get_data_path("Test App", "Test Table", "Python")
        assert data_path.exists()

        # Verify cached content matches
        cached_data = json.loads(data_path.read_text())
        assert cached_data == data

    def test_data_transformation(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that data is transformed correctly."""
        # First cache metadata
        get_report_metadata(
            mock_qb_api,
            "Test App",
            "appXYZ",
            "Test Table",
            "Python",
            cache_root=temp_cache_dir,
        )

        # Get data
        data = get_data(
            mock_qb_api,
            "Test Report",
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        # Data should have field labels as keys, not IDs
        assert "Record ID#" in data[0]
        assert "Name" in data[0]
        assert "Email" in data[0]
        assert "Status" in data[0]

        # Should not have field IDs as keys
        assert "3" not in data[0]
        assert "6" not in data[0]

    def test_get_data_missing_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when metadata not cached."""
        with pytest.raises(FileNotFoundError):
            get_data(
                mock_qb_api,
                "Test Report",
                sample_report_configs,
                cache_root=temp_cache_dir,
            )

    def test_get_data_unknown_report(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when report description not in config."""
        with pytest.raises(ValueError, match="No report found"):
            get_data(
                mock_qb_api,
                "Unknown Report",
                sample_report_configs,
                cache_root=temp_cache_dir,
            )

    def test_get_data_logs_result(self, temp_cache_dir, mock_qb_api, sample_report_configs, caplog):
        """Test that get_data logs result."""
        get_report_metadata(
            mock_qb_api,
            "Test App",
            "appXYZ",
            "Test Table",
            "Python",
            cache_root=temp_cache_dir,
        )

        get_data(
            mock_qb_api,
            "Test Report",
            sample_report_configs,
            cache=False,
            cache_root=temp_cache_dir,
        )

        assert "fetched but not cached" in caplog.text or "2 records" in caplog.text


class TestGetDataParallel:
    """Tests for get_data_parallel function."""

    def test_get_multiple_reports_parallel(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching multiple reports in parallel."""
        # First cache metadata for all reports
        for report in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                report["App"],
                report["App ID"],
                report["Table"],
                report["Report"],
                cache_root=temp_cache_dir,
            )

        # Get data in parallel
        results = get_data_parallel(
            mock_qb_api,
            ["Test Report", "Another Report"],
            sample_report_configs,
            cache=False,
            cache_root=temp_cache_dir,
        )

        assert len(results) == 2
        assert "Test Report" in results
        assert "Another Report" in results
        assert len(results["Test Report"]) == 2
        assert len(results["Another Report"]) == 2

    def test_parallel_fail_fast_on_error(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that parallel execution fails fast on first error."""
        # Cache only first report's metadata
        get_report_metadata(
            mock_qb_api,
            sample_report_configs[0]["App"],
            sample_report_configs[0]["App ID"],
            sample_report_configs[0]["Table"],
            sample_report_configs[0]["Report"],
            cache_root=temp_cache_dir,
        )

        # Try to get data for both (second should fail)
        with pytest.raises(FileNotFoundError):
            get_data_parallel(
                mock_qb_api,
                ["Test Report", "Another Report"],
                sample_report_configs,
                cache_root=temp_cache_dir,
            )


class TestLoadData:
    """Tests for load_data function."""

    def test_load_cached_data(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached data."""
        # First get and cache data
        get_report_metadata(
            mock_qb_api,
            "Test App",
            "appXYZ",
            "Test Table",
            "Python",
            cache_root=temp_cache_dir,
        )

        get_data(
            mock_qb_api,
            "Test Report",
            sample_report_configs,
            cache=True,
            cache_root=temp_cache_dir,
        )

        # Now load cached data
        loaded = load_data(
            "Test Report",
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        assert len(loaded) == 2
        assert loaded[0]["Name"] == "Alice"

    def test_load_nonexistent_data(self, temp_cache_dir, sample_report_configs):
        """Test error when data not cached."""
        with pytest.raises(FileNotFoundError):
            load_data(
                "Test Report",
                sample_report_configs,
                cache_root=temp_cache_dir,
            )

    def test_load_unknown_report(self, temp_cache_dir, sample_report_configs):
        """Test error when report not in config."""
        with pytest.raises(ValueError, match="No report found"):
            load_data(
                "Unknown Report",
                sample_report_configs,
                cache_root=temp_cache_dir,
            )
