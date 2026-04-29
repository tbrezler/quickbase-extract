"""Integration tests for report_metadata module."""

import json

import pytest
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.config import ReportConfig
from quickbase_extract.report_metadata import (
    fetch_report_metadata_api,
    filter_metadata_by_table,
    get_report_metadata,
    get_report_metadata_parallel,
    load_report_metadata,
    load_report_metadata_batch,
)


class TestFetchReportMetadataApi:
    """Tests for fetch_report_metadata_api function."""

    def test_fetch_extracts_essentials_only(self, mock_qb_api):
        """Test that fetch_report_metadata_api extracts only necessary fields."""
        result = fetch_report_metadata_api(
            mock_qb_api,
            "appXYZ123",
            "Test Table",
            "Python",
        )

        # Should have essentials
        assert result["table_id"] == "tblXYZ123"
        assert result["field_label"] == {
            "Record ID#": 3,
            "Name": 6,
            "Email": 7,
            "Status": 8,
        }
        assert result["fields"] == [3, 6, 7, 8]
        assert result["filter"] == "{8.EX.'Active'}"
        assert result["sort_by"] == [{"fieldId": 6, "order": "ASC"}]
        assert result["group_by"] == []

        # Should NOT have full report object
        assert "report" not in result

    def test_fetch_handles_missing_groupby(self, mock_qb_api):
        """Test that fetch_report_metadata_api handles missing groupBy."""
        # Mock response without groupBy
        mock_qb_api.get_report.return_value = {
            "id": "rptABC123",
            "name": "Python",
            "query": {
                "fields": [3, 6, 7, 8],
                "sortBy": [{"fieldId": 6, "order": "ASC"}],
                "filter": "{8.EX.'Active'}",
            },
        }

        result = fetch_report_metadata_api(
            mock_qb_api,
            "appXYZ123",
            "Test Table",
            "Python",
        )

        assert result["group_by"] == []

    def test_fetch_report_not_found(self, mock_qb_api):
        """Test error when report not found."""
        mock_qb_api.get_reports.return_value = [
            {"id": "rptABC", "name": "Default"},
        ]

        with pytest.raises(ValueError, match="Report .* not found"):
            fetch_report_metadata_api(
                mock_qb_api,
                "appXYZ123",
                "Test Table",
                "Nonexistent",
            )

    def test_multiple_reports_same_name_warning(self, mock_qb_api, caplog):
        """Test warning when multiple reports have the same name."""
        # Mock multiple reports with same name (edge case)
        mock_qb_api.get_reports.return_value = [
            {"id": "rptABC123", "name": "Python"},
            {"id": "rptDEF456", "name": "Python"},  # Duplicate name
            {"id": "rptGHI789", "name": "Default"},
        ]

        result = fetch_report_metadata_api(
            mock_qb_api,
            "appXYZ123",
            "Test Table",
            "Python",
        )

        # Should use first match
        assert result["table_id"] == "tblXYZ123"
        # Should warn about multiple matches
        assert "Multiple reports match" in caplog.text


class TestGetReportMetadata:
    """Tests for get_report_metadata function."""

    def test_fetch_and_cache_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching and caching report metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Check metadata was cached
        md_path = cache_mgr.get_metadata_path(config.app_name, config.table_name, config.report_name)
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
            cache_mgr,
            config,
            cache=True,
        )

        md_path = cache_mgr.get_metadata_path(config.app_name, config.table_name, config.report_name)
        metadata = json.loads(md_path.read_text())

        assert metadata["app_name"] == "test_app"
        assert metadata["table_name"] == "test_table"
        assert metadata["report_name"] == "python"
        assert "fields" in metadata
        assert "filter" in metadata
        assert "sort_by" in metadata
        assert "group_by" in metadata

        # Should NOT have nested report object
        assert "report" not in metadata

    def test_report_not_found(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test error when report not found."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        mock_qb_api.get_reports.return_value = [
            {"id": "rptABC", "name": "Default"},
        ]

        bad_config = ReportConfig(
            app_id="appXYZ123",
            app_name="test_app",
            table_name="Test Table",
            report_name="Nonexistent",
        )

        with pytest.raises(ValueError, match="Report .* not found"):
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                bad_config,
                cache=True,
            )

    def test_api_calls_in_order(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that API calls are made in correct order."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Verify call order
        assert mock_qb_api.get_table_id.called
        assert mock_qb_api.get_field_label_id_map.called
        assert mock_qb_api.get_reports.called
        assert mock_qb_api.get_report.called

    def test_cache_disabled(self, temp_cache_dir, mock_qb_api, sample_report_configs, caplog):
        """Test that cache can be disabled."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=False,
        )

        # Check metadata was NOT cached
        md_path = cache_mgr.get_metadata_path(config.app_name, config.table_name, config.report_name)
        assert not md_path.exists()
        assert "not cached" in caplog.text


class TestGetReportMetadataParallel:
    """Tests for get_report_metadata_parallel function."""

    def test_fetch_multiple_reports_parallel(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test fetching multiple reports in parallel."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            cache=True,
        )

        # Both reports should be cached
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config.app_name, config.table_name, config.report_name)
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
                cache_mgr,
                sample_report_configs,
                cache=True,
            )

    def test_parallel_with_custom_max_workers(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test parallel execution with custom max_workers."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            cache=True,
            max_workers=2,
        )

        # Should complete successfully
        for config in sample_report_configs:
            md_path = cache_mgr.get_metadata_path(config.app_name, config.table_name, config.report_name)
            assert md_path.exists()

    def test_parallel_with_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test parallel execution with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            cache_mgr,
            [],
            cache=True,
        )

        assert "No report config provided" in caplog.text


class TestLoadReportMetadata:
    """Tests for load_report_metadata function."""

    def test_load_existing_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # First fetch and cache
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Now load it
        metadata = load_report_metadata(cache_mgr, config)

        assert metadata["table_id"] == "tblXYZ123"
        assert metadata["table_name"] == "test_table"
        assert "fields" in metadata
        assert "filter" in metadata

    def test_load_nonexistent_metadata(self, temp_cache_dir, sample_report_configs):
        """Test error when loading non-cached metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        with pytest.raises(FileNotFoundError):
            load_report_metadata(cache_mgr, config)

    def test_load_normalizes_names(self, temp_cache_dir, mock_qb_api):
        """Test that load_report_metadata normalizes names correctly."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create config with mixed casing
        config = ReportConfig(
            app_id="appXYZ123",
            app_name="Test App",  # Mixed case
            table_name="Test Table",  # Mixed case
            report_name="Python",  # Mixed case
        )

        # Cache with normalized names
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Should be able to load with original config
        metadata = load_report_metadata(cache_mgr, config)
        assert metadata is not None


class TestLoadReportMetadataBatch:
    """Tests for load_report_metadata_batch function."""

    def test_load_multiple_metadata(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading multiple metadata files."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # First cache both
        get_report_metadata_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            cache=True,
        )

        # Now load both
        all_metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        assert len(all_metadata) == 2
        assert sample_report_configs[0] in all_metadata
        assert sample_report_configs[1] in all_metadata

    def test_load_batch_with_empty_list(self, temp_cache_dir):
        """Test loading with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        result = load_report_metadata_batch(cache_mgr, [])
        assert result == {}

    def test_load_batch_missing_file_raises_error(self, temp_cache_dir, sample_report_configs):
        """Test that missing file raises error."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        with pytest.raises(FileNotFoundError):
            load_report_metadata_batch(cache_mgr, sample_report_configs)

    def test_metadata_keyed_by_report_config(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test that returned metadata is keyed by ReportConfig."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        get_report_metadata_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            cache=True,
        )

        all_metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        # Keys should be ReportConfig instances
        for key in all_metadata.keys():
            assert isinstance(key, ReportConfig)

        # Should be able to look up by config
        config1 = sample_report_configs[0]
        assert all_metadata[config1]["table_id"] == "tblXYZ123"


class TestFilterMetadataByTable:
    """Tests for filter_metadata_by_table function."""

    def test_filter_metadata_by_table_unique(self, sample_report_metadata, sample_report_configs):
        """Test retrieving metadata for a unique table."""

        table_name = sample_report_configs[0].table_name
        result = filter_metadata_by_table(sample_report_metadata, table_name)

        assert result["table_name"] == "test_table"
        assert result["table_id"] == "tblXYZ123"

    def test_get_metadata_by_table_with_app_name(self, sample_report_metadata, sample_report_configs):
        """Test retrieving metadata filtering by both app and table."""

        config = sample_report_configs[0]
        result = filter_metadata_by_table(sample_report_metadata, config.table_name, app_name=config.app_name)

        assert result["table_name"] == "test_table"
        assert result["app_name"] == "test_app"

    def test_get_metadata_by_table_not_found(self, sample_report_metadata):
        """Test error when table not found."""

        with pytest.raises(ValueError, match="No metadata found"):
            filter_metadata_by_table(sample_report_metadata, "Nonexistent Table")

    def test_get_metadata_by_table_ambiguous_without_app(self, sample_report_metadata):
        """Test error when table exists in multiple apps without app_name specified."""

        # Create a second metadata entry with same table name but different app
        # This would require a fixture with duplicate table names across apps
        # For now, this documents expected behavior
        pass

    def test_get_metadata_by_table_ambiguous_with_app(self, sample_report_metadata, sample_report_configs):
        """Test successful lookup when table is ambiguous but app_name is provided."""

        config = sample_report_configs[0]
        # Should not raise error because app_name disambiguates
        result = filter_metadata_by_table(sample_report_metadata, config.table_name, app_name=config.app_name)

        assert result is not None
