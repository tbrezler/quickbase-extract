"""Integration tests for report_data module."""

import json

import pytest
from quickbase_extract.cache_manager import get_cache_manager
from quickbase_extract.report_data import get_data, get_data_parallel, load_data, load_data_batch
from quickbase_extract.report_metadata import get_report_metadata, load_report_metadata_batch


class TestGetData:
    """Tests for get_data function."""

    def test_get_data_without_cache(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test getting data without caching."""
        # First cache metadata for only the first report
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        # Get data without caching
        data = get_data(
            mock_qb_api,
            metadata,
            config["Description"],
            cache=False,
            cache_root=temp_cache_dir,
        )

        assert len(data) == 2
        assert data[0]["Name"] == "Alice"
        assert data[1]["Name"] == "Bob"

    def test_get_data_with_cache(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test getting data and caching it."""
        # First cache metadata for only the first report
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        # Get data with caching
        data = get_data(
            mock_qb_api,
            metadata,
            config["Description"],
            cache=True,
            cache_root=temp_cache_dir,
        )

        # Verify data was cached
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        data_path = cache_mgr.get_data_path(config["App"], config["Table"], config["Report"])
        assert data_path.exists()

        # Verify cached content matches
        cached_data = json.loads(data_path.read_text())
        assert cached_data == data

    def test_data_transformation(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that data is transformed correctly."""
        # First cache metadata for only the first report
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        # Get data
        data = get_data(
            mock_qb_api,
            metadata,
            config["Description"],
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

    def test_get_data_unknown_report(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when report description not in metadata."""
        # First cache metadata for only the first report
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        with pytest.raises(KeyError):
            get_data(
                mock_qb_api,
                metadata,
                "Unknown Report",
                cache_root=temp_cache_dir,
            )

    def test_get_data_logs_result(self, temp_cache_dir, mock_qb_api, sample_report_configs, caplog):
        """Test that get_data logs result."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        get_data(
            mock_qb_api,
            metadata,
            config["Description"],
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
                report,
                cache_root=temp_cache_dir,
            )

        # Load metadata
        metadata = load_report_metadata_batch(
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        # Get data in parallel
        descriptions = [r["Description"] for r in sample_report_configs]
        results = get_data_parallel(
            mock_qb_api,
            metadata,
            descriptions,
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
            sample_report_configs[0],
            cache_root=temp_cache_dir,
        )

        metadata = load_report_metadata_batch(
            [sample_report_configs[0]],
            cache_root=temp_cache_dir,
        )

        # Try to get data for both (second should fail with KeyError)
        descriptions = [r["Description"] for r in sample_report_configs]
        with pytest.raises(KeyError):
            get_data_parallel(
                mock_qb_api,
                metadata,
                descriptions,
                cache_root=temp_cache_dir,
            )

    def test_parallel_with_custom_max_workers(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test parallel execution with custom max_workers."""
        # Cache metadata
        for report in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                report,
                cache_root=temp_cache_dir,
            )

        metadata = load_report_metadata_batch(
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        descriptions = [r["Description"] for r in sample_report_configs]
        results = get_data_parallel(
            mock_qb_api,
            metadata,
            descriptions,
            cache=False,
            cache_root=temp_cache_dir,
            max_workers=2,
        )

        assert len(results) == 2

    def test_parallel_with_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test parallel execution with empty description list."""
        results = get_data_parallel(
            mock_qb_api,
            {},
            [],
            cache_root=temp_cache_dir,
        )

        assert results == {}
        assert "No report descriptions provided" in caplog.text


class TestLoadData:
    """Tests for load_data function."""

    def test_load_cached_data(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached data."""
        # First get and cache data for only the first report
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        get_data(
            mock_qb_api,
            metadata,
            config["Description"],
            cache=True,
            cache_root=temp_cache_dir,
        )

        # Now load cached data
        loaded = load_data(
            metadata,
            config["Description"],
            cache_root=temp_cache_dir,
        )

        assert len(loaded) == 2
        assert loaded[0]["Name"] == "Alice"

    def test_load_nonexistent_data(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when data not cached."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        with pytest.raises(FileNotFoundError):
            load_data(
                metadata,
                config["Description"],
                cache_root=temp_cache_dir,
            )

    def test_load_unknown_report(self, temp_cache_dir):
        """Test error when report not in metadata."""
        with pytest.raises(KeyError):
            load_data(
                {},
                "Unknown Report",
                cache_root=temp_cache_dir,
            )


class TestLoadDataBatch:
    """Tests for load_data_batch function."""

    def test_load_multiple_data(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading multiple data files."""
        # First cache metadata and data for all reports
        for report in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                report,
                cache_root=temp_cache_dir,
            )

        metadata = load_report_metadata_batch(
            sample_report_configs,  # Now we've cached all of them
            cache_root=temp_cache_dir,
        )

        # Cache all data
        descriptions = [r["Description"] for r in sample_report_configs]
        for desc in descriptions:
            get_data(
                mock_qb_api,
                metadata,
                desc,
                cache=True,
                cache_root=temp_cache_dir,
            )

        # Now load all
        all_data = load_data_batch(
            metadata,
            descriptions,
            cache_root=temp_cache_dir,
        )

        assert len(all_data) == 2
        assert "Test Report" in all_data
        assert "Another Report" in all_data
        assert len(all_data["Test Report"]) == 2

    def test_load_batch_with_empty_list(self, temp_cache_dir):
        """Test loading with empty description list."""
        result = load_data_batch({}, [], cache_root=temp_cache_dir)
        assert result == {}

    def test_load_batch_missing_file_raises_error(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that missing file raises error."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Load metadata - only pass the config we cached
        metadata = load_report_metadata_batch(
            [config],  # Only the one we cached
            cache_root=temp_cache_dir,
        )

        with pytest.raises(FileNotFoundError):
            load_data_batch(
                metadata,
                [config["Description"]],
                cache_root=temp_cache_dir,
            )
