"""Integration tests for report_metadata module."""

import json

import pytest
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.report_metadata import (
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    load_report_metadata_batch,
)


class TestGetReportMetadata:
    """Tests for get_report_metadata function."""

    def test_fetch_and_cache_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching and caching report metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_mgr,
        )

        # Check metadata was cached
        md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
        assert md_path.exists()

        # Verify cached content
        metadata = json.loads(md_path.read_text())
        assert metadata["table_id"] == "tblXYZ123"
        assert metadata["report_name"] == "python"
        assert "field_label" in metadata

    def test_metadata_structure(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that cached metadata has expected structure."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_mgr,
        )

        md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
        metadata = json.loads(md_path.read_text())

        assert metadata["app_name"] == "test_app"
        assert metadata["table_name"] == "test_table"
        assert metadata["report_id"] == "rptABC123"
        assert "fields" in metadata
        assert "filter" in metadata

    def test_report_not_found(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when report not found."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        mock_qb_api.get_reports.return_value = [
            {"id": "rptABC", "name": "Default"},
        ]

        config = sample_report_configs[0].copy()
        config["Report"] = "Nonexistent"

        with pytest.raises(ValueError, match="Report .* not found"):
            get_report_metadata(
                mock_qb_api,
                config,
                cache_mgr,
            )

    def test_api_calls_in_order(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that API calls are made in correct order."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_mgr,
        )

        # Verify call order
        assert mock_qb_api.get_table_id.called
        assert mock_qb_api.get_field_label_id_map.called
        assert mock_qb_api.get_reports.called
        assert mock_qb_api.get_report.called


class TestGetReportMetadataParallel:
    """Tests for get_report_metadata_parallel function."""

    def test_fetch_multiple_reports_parallel(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching multiple reports in parallel."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_mgr,
        )

        # Both reports should be cached
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
            assert md_path.exists()

    def test_parallel_fail_fast_on_error(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that parallel execution fails fast on first error."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Make second call fail
        side_effects = [
            "tblXYZ123",  # First call succeeds
            Exception("Table not found"),  # Second call fails
        ]
        mock_qb_api.get_table_id.side_effect = side_effects

        with pytest.raises(Exception, match="Table not found"):
            get_report_metadata_parallel(
                mock_qb_api,
                sample_report_configs,
                cache_mgr,
            )

    def test_parallel_with_custom_max_workers(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test parallel execution with custom max_workers."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_mgr,
            max_workers=2,
        )

        # Should complete successfully
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
            assert md_path.exists()

    def test_parallel_with_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test parallel execution with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            [],
            cache_mgr,
        )

        assert "No report configs provided" in caplog.text


class TestLoadReportMetadata:
    """Tests for load_report_metadata function."""

    def test_load_existing_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # First fetch and cache
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_mgr,
        )

        # Now load it
        metadata = load_report_metadata(
            config["Description"],
            sample_report_configs,
            cache_mgr,
        )

        assert metadata["table_id"] == "tblXYZ123"
        assert metadata["table_name"] == "test_table"

    def test_load_nonexistent_metadata(self, temp_cache_dir, sample_report_configs):
        """Test error when loading non-cached metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(FileNotFoundError):
            load_report_metadata(
                sample_report_configs[0]["Description"],
                sample_report_configs,
                cache_mgr,
            )

    def test_load_report_not_in_config(self, temp_cache_dir, sample_report_configs):
        """Test error when report description not in config."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(ValueError, match="No report found"):
            load_report_metadata(
                "Nonexistent Report",
                sample_report_configs,
                cache_mgr,
            )


class TestLoadReportMetadataBatch:
    """Tests for load_report_metadata_batch function."""

    def test_load_multiple_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading multiple metadata files."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # First cache both
        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_mgr,
        )

        # Now load both
        all_metadata = load_report_metadata_batch(
            sample_report_configs,
            cache_mgr,
        )

        assert len(all_metadata) == 2
        assert "Test Report" in all_metadata
        assert "Another Report" in all_metadata

    def test_load_batch_with_empty_list(self, temp_cache_dir):
        """Test loading with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        result = load_report_metadata_batch([], cache_mgr)
        assert result == {}

    def test_load_batch_missing_file_raises_error(self, temp_cache_dir, sample_report_configs):
        """Test that missing file raises error."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(FileNotFoundError):
            load_report_metadata_batch(
                sample_report_configs,
                cache_mgr,
            )
