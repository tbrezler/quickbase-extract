"""Integration tests for report_metadata module."""

import json

import pytest
from quickbase_extract.cache_manager import get_cache_manager
from quickbase_extract.report_metadata import (
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    load_report_metadata_batch,
    refresh_all,
)


class TestGetReportMetadata:
    """Tests for get_report_metadata function."""

    def test_fetch_and_cache_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching and caching report metadata."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Check metadata was cached
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
        assert md_path.exists()

        # Verify cached content
        metadata = json.loads(md_path.read_text())
        assert metadata["table_id"] == "tblXYZ123"
        assert metadata["report_name"] == "python"
        assert "field_label" in metadata

    def test_metadata_structure(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that cached metadata has expected structure."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
        metadata = json.loads(md_path.read_text())

        assert metadata["app_name"] == "test_app"
        assert metadata["table_name"] == "test_table"
        assert metadata["report_id"] == "rptABC123"
        assert "fields" in metadata
        assert "filter" in metadata

    def test_report_not_found(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when report not found."""
        mock_qb_api.get_reports.return_value = [
            {"id": "rptABC", "name": "Default"},
        ]

        config = sample_report_configs[0].copy()
        config["Report"] = "Nonexistent"

        with pytest.raises(ValueError, match="Report .* not found"):
            get_report_metadata(
                mock_qb_api,
                config,
                cache_root=temp_cache_dir,
            )

    def test_api_calls_in_order(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that API calls are made in correct order."""
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
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
        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        # Both reports should be cached
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
            assert md_path.exists()

    def test_parallel_fail_fast_on_error(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that parallel execution fails fast on first error."""
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
                cache_root=temp_cache_dir,
            )

    def test_parallel_with_custom_max_workers(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test parallel execution with custom max_workers."""
        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_root=temp_cache_dir,
            max_workers=2,
        )

        # Should complete successfully
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config["App"], config["Table"], config["Report"])
            assert md_path.exists()

    def test_parallel_with_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test parallel execution with empty config list."""
        get_report_metadata_parallel(
            mock_qb_api,
            [],
            cache_root=temp_cache_dir,
        )

        assert "No report configs provided" in caplog.text


class TestLoadReportMetadata:
    """Tests for load_report_metadata function."""

    def test_load_existing_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached metadata."""
        # First fetch and cache
        config = sample_report_configs[0]
        get_report_metadata(
            mock_qb_api,
            config,
            cache_root=temp_cache_dir,
        )

        # Now load it
        metadata = load_report_metadata(
            config["Description"],
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        assert metadata["table_id"] == "tblXYZ123"
        assert metadata["table_name"] == "test_table"

    def test_load_nonexistent_metadata(self, temp_cache_dir, sample_report_configs):
        """Test error when loading non-cached metadata."""
        with pytest.raises(FileNotFoundError):
            load_report_metadata(
                sample_report_configs[0]["Description"],
                sample_report_configs,
                cache_root=temp_cache_dir,
            )

    def test_load_report_not_in_config(self, temp_cache_dir, sample_report_configs):
        """Test error when report description not in config."""
        with pytest.raises(ValueError, match="No report found"):
            load_report_metadata(
                "Nonexistent Report",
                sample_report_configs,
                cache_root=temp_cache_dir,
            )


class TestLoadReportMetadataBatch:
    """Tests for load_report_metadata_batch function."""

    def test_load_multiple_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading multiple metadata files."""
        # First cache both
        get_report_metadata_parallel(
            mock_qb_api,
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        # Now load both
        all_metadata = load_report_metadata_batch(
            sample_report_configs,
            cache_root=temp_cache_dir,
        )

        assert len(all_metadata) == 2
        assert "Test Report" in all_metadata
        assert "Another Report" in all_metadata

    def test_load_batch_with_empty_list(self, temp_cache_dir):
        """Test loading with empty config list."""
        result = load_report_metadata_batch([], cache_root=temp_cache_dir)
        assert result == {}

    def test_load_batch_missing_file_raises_error(self, temp_cache_dir, sample_report_configs):
        """Test that missing file raises error."""
        with pytest.raises(FileNotFoundError):
            load_report_metadata_batch(
                sample_report_configs,
                cache_root=temp_cache_dir,
            )


class TestRefreshAll:
    """Tests for refresh_all function."""

    def test_refresh_all_reports(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test refreshing all reports."""
        refresh_all(mock_qb_api, sample_report_configs, cache_root=temp_cache_dir)

        # Both reports should be cached
        cache_mgr = get_cache_manager(cache_root=temp_cache_dir)

        for report in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(
                report["App"],
                report["Table"],
                report["Report"],
            )
            assert md_path.exists(), f"Metadata not found for {report['Description']}"

    def test_refresh_all_logs_time(self, temp_cache_dir, mock_qb_api, sample_report_configs, caplog):
        """Test that refresh_all logs elapsed time."""
        refresh_all(mock_qb_api, sample_report_configs, cache_root=temp_cache_dir)

        assert "refresh completed" in caplog.text or "refresh time" in caplog.text

    def test_refresh_all_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test refresh_all with empty report list."""
        refresh_all(mock_qb_api, [], cache_root=temp_cache_dir)

        assert "No report configs provided" in caplog.text
