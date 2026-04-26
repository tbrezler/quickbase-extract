"""Integration tests for report_data module."""

import json

import pytest
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.config import ReportConfig
from quickbase_extract.report_data import (
    _extract_report_names,
    _replace_ask_placeholders,
    get_data,
    get_data_parallel,
    load_data,
    load_data_batch,
)
from quickbase_extract.report_metadata import (
    get_report_metadata,
    load_report_metadata_batch,
)


class TestReplaceAskPlaceholders:
    """Tests for _replace_ask_placeholders function."""

    def test_replace_single_placeholder(self, sample_report_configs):
        """Test replacing a single ask placeholder."""
        config = sample_report_configs[0]
        filter_str = "{'25'.EX.'_ask1_'}"
        ask_values = {"ask1": "abc123"}

        result = _replace_ask_placeholders(filter_str, ask_values, config)

        assert result == "{'25'.EX.'abc123'}"

    def test_replace_multiple_placeholders(self, sample_report_configs):
        """Test replacing multiple ask placeholders."""
        config = sample_report_configs[0]
        filter_str = "({'25'.EX.'_ask1_'}AND{'40'.EX.'_ask2_'})"
        ask_values = {"ask1": "value1", "ask2": "value2"}

        result = _replace_ask_placeholders(filter_str, ask_values, config)

        assert result == "({'25'.EX.'value1'}AND{'40'.EX.'value2'})"

    def test_no_placeholders_returns_unchanged(self, sample_report_configs):
        """Test that filter without placeholders is returned unchanged."""
        config = sample_report_configs[0]
        filter_str = "{'25'.EX.'Fixed Value'}"
        ask_values = {}

        result = _replace_ask_placeholders(filter_str, ask_values, config)

        assert result == filter_str

    def test_missing_placeholder_value_raises_error(self, sample_report_configs):
        """Test error when required placeholder value is missing."""
        config = sample_report_configs[0]
        filter_str = "{'25'.EX.'_ask1_'}AND{'40'.EX.'_ask2_'}"
        ask_values = {"ask1": "value1"}  # Missing ask2

        with pytest.raises(ValueError, match="requires values for.*_ask2_"):
            _replace_ask_placeholders(filter_str, ask_values, config)

    def test_unused_placeholder_value_raises_error(self, sample_report_configs):
        """Test error when provided value is not used in filter."""
        config = sample_report_configs[0]
        filter_str = "{'25'.EX.'_ask1_'}"
        ask_values = {"ask1": "value1", "ask2": "value2"}  # ask2 not in filter

        with pytest.raises(ValueError, match="received ask_values.*ask2.*not used"):
            _replace_ask_placeholders(filter_str, ask_values, config)

    def test_placeholder_with_special_characters(self, sample_report_configs):
        """Test replacing placeholders with special characters in values."""
        config = sample_report_configs[0]
        filter_str = "{'25'.EX.'_ask1_'}"
        ask_values = {"ask1": "value with spaces & symbols!"}

        result = _replace_ask_placeholders(filter_str, ask_values, config)

        assert result == "{'25'.EX.'value with spaces & symbols!'}"

    def test_complex_filter_with_mixed_content(self, sample_report_configs):
        """Test replacing placeholders in complex filter."""
        config = sample_report_configs[0]
        filter_str = "({'15'.EX.'Pending'}AND({'41'.EX.'_ask1_'}OR{'40'.EX.'_ask1_'}))"
        ask_values = {"ask1": "urgent"}

        result = _replace_ask_placeholders(filter_str, ask_values, config)

        assert result == (
            "({'15'.EX.'Pending'}AND({'41'.EX.'urgent'}OR{'40'.EX.'urgent'}))"
        )


class TestExtractReportNames:
    """Tests for _extract_report_names helper function."""

    def test_extract_names_from_metadata(
        self, sample_report_metadata, sample_report_configs
    ):
        """Test extracting report names from metadata."""
        config = sample_report_configs[0]
        info = sample_report_metadata[config]

        app_name, table_name, report_name = _extract_report_names(info)

        assert app_name == "test_app"
        assert table_name == "test_table"
        assert report_name == "python"

    def test_extract_names_returns_tuple(
        self, sample_report_metadata, sample_report_configs
    ):
        """Test that extract_report_names returns a tuple."""
        config = sample_report_configs[0]
        info = sample_report_metadata[config]

        result = _extract_report_names(info)

        assert isinstance(result, tuple)
        assert len(result) == 3


class TestGetData:
    """Tests for get_data function."""

    def test_get_data_without_cache(
        self,
        temp_cache_dir,
        mock_qb_api,
        sample_report_configs,
        sample_transformed_data,
    ):
        """Test getting data without caching."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # First cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Get data without caching
        data = get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
            cache=False,
        )

        assert len(data) == 2
        assert data[0]["Name"] == "Alice"
        assert data[1]["Name"] == "Bob"

    def test_get_data_with_cache(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test getting data and caching it."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # First cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Get data with caching
        data = get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
            cache=True,
        )

        # Verify data was cached
        data_path = cache_mgr.get_data_path(
            config.app_name, config.table_name, config.report_name
        )
        assert data_path.exists()

        # Verify cached content matches
        cached_data = json.loads(data_path.read_text())
        assert cached_data == data

    def test_data_transformation(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test that data is transformed correctly."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Get data
        data = get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
        )

        # Data should have field labels as keys, not IDs
        assert "Record ID#" in data[0]
        assert "Name" in data[0]
        assert "Email" in data[0]
        assert "Status" in data[0]

        # Should not have field IDs as keys
        assert "3" not in data[0]
        assert "6" not in data[0]

    def test_get_data_unknown_report(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test error when report config not in metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load only first config's metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Try to get data for second config (not in metadata)
        other_config = sample_report_configs[1]
        with pytest.raises(KeyError):
            get_data(
                mock_qb_api,
                cache_mgr,
                other_config,
                metadata,
            )

    def test_get_data_with_ask_values(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test get_data with ask placeholder replacement."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Update mock to have ask placeholders in filter
        mock_qb_api.get_report.return_value = {
            "id": "rptABC123",
            "name": "Python",
            "query": {
                "fields": [3, 6, 7, 8],
                "sortBy": [{"fieldId": 6, "order": "ASC"}],
                "groupBy": [],
                "filter": "{'25'.EX.'_ask1_'}",
            },
        }

        # Cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Get data with ask values
        data = get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
            ask_values={"ask1": "specific_value"},
        )

        assert len(data) == 2

    def test_get_data_logs_result(
        self, temp_cache_dir, mock_qb_api, sample_report_configs, caplog
    ):
        """Test that get_data logs result."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
            cache=False,
        )

        assert "fetched but not cached" in caplog.text or "records" in caplog.text


class TestGetDataParallel:
    """Tests for get_data_parallel function."""

    def test_get_multiple_reports_parallel(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test fetching multiple reports in parallel."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Cache metadata for all reports
        for config in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                config,
                cache=True,
            )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        # Get data in parallel
        results = get_data_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            metadata,
            cache=False,
        )

        assert len(results) == 2
        assert sample_report_configs[0] in results
        assert sample_report_configs[1] in results
        assert len(results[sample_report_configs[0]]) == 2

    def test_parallel_fail_fast_on_error(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test that parallel execution fails fast on first error."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Cache only first report's metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            sample_report_configs[0],
            cache=True,
        )

        metadata = load_report_metadata_batch(
            cache_mgr,
            [sample_report_configs[0]],
        )

        # Try to get data for both (second should fail with KeyError)
        with pytest.raises(KeyError):
            get_data_parallel(
                mock_qb_api,
                cache_mgr,
                sample_report_configs,
                metadata,
            )

    def test_parallel_with_custom_max_workers(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test parallel execution with custom max_workers."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Cache metadata
        for config in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                config,
                cache=True,
            )

        metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        results = get_data_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            metadata,
            cache=False,
            max_workers=2,
        )

        assert len(results) == 2

    def test_parallel_with_empty_list(self, temp_cache_dir, mock_qb_api, caplog):
        """Test parallel execution with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        results = get_data_parallel(
            mock_qb_api,
            cache_mgr,
            [],
            {},
        )

        assert results == {}
        assert "No report config provided" in caplog.text

    def test_parallel_with_per_report_ask_values(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test parallel execution with per-report ask values."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Update mock for ask placeholders
        mock_qb_api.get_report.return_value = {
            "id": "rptABC123",
            "name": "Python",
            "query": {
                "fields": [3, 6, 7, 8],
                "sortBy": [{"fieldId": 6, "order": "ASC"}],
                "groupBy": [],
                "filter": "{'25'.EX.'_ask1_'}",
            },
        }

        # Cache metadata
        for config in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                config,
                cache=True,
            )

        metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        # Get data with per-report ask values
        ask_values = {
            sample_report_configs[0]: {"ask1": "value1"},
            sample_report_configs[1]: {"ask1": "value2"},
        }

        results = get_data_parallel(
            mock_qb_api,
            cache_mgr,
            sample_report_configs,
            metadata,
            ask_values=ask_values,
        )

        assert len(results) == 2


class TestLoadData:
    """Tests for load_data function."""

    def test_load_cached_data(self, temp_cache_dir, mock_qb_api, sample_report_configs):
        """Test loading cached data."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        # Get and cache data
        get_data(
            mock_qb_api,
            cache_mgr,
            config,
            metadata,
            cache=True,
        )

        # Now load cached data
        loaded = load_data(cache_mgr, config, metadata)

        assert len(loaded) == 2
        assert loaded[0]["Name"] == "Alice"

    def test_load_nonexistent_data(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test error when data not cached."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata but not data
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        with pytest.raises(FileNotFoundError):
            load_data(cache_mgr, config, metadata)

    def test_load_unknown_report(self, temp_cache_dir):
        """Test error when report not in metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        unknown_config = ReportConfig(
            app_id="appXYZ123",
            app_name="test_app",
            table_name="Unknown Table",
            report_name="Python",
        )

        with pytest.raises(KeyError):
            load_data(cache_mgr, unknown_config, {})


class TestLoadDataBatch:
    """Tests for load_data_batch function."""

    def test_load_multiple_data(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test loading multiple data files."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Cache metadata and data for all reports
        for config in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                config,
                cache=True,
            )

        metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        # Cache all data
        for config in sample_report_configs:
            get_data(
                mock_qb_api,
                cache_mgr,
                config,
                metadata,
                cache=True,
            )

        # Now load all
        all_data = load_data_batch(cache_mgr, sample_report_configs, metadata)

        assert len(all_data) == 2
        assert sample_report_configs[0] in all_data
        assert sample_report_configs[1] in all_data
        assert len(all_data[sample_report_configs[0]]) == 2

    def test_load_batch_with_empty_list(self, temp_cache_dir):
        """Test loading with empty config list."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        result = load_data_batch(cache_mgr, [], {})
        assert result == {}

    def test_load_batch_missing_file_raises_error(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test that missing file raises error."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        config = sample_report_configs[0]

        # Cache metadata but not data
        get_report_metadata(
            mock_qb_api,
            cache_mgr,
            config,
            cache=True,
        )

        # Load metadata
        metadata = load_report_metadata_batch(cache_mgr, [config])

        with pytest.raises(FileNotFoundError):
            load_data_batch(cache_mgr, [config], metadata)

    def test_load_batch_keyed_by_report_config(
        self, temp_cache_dir, mock_qb_api, sample_report_configs
    ):
        """Test that returned data is keyed by ReportConfig."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Cache metadata and data
        for config in sample_report_configs:
            get_report_metadata(
                mock_qb_api,
                cache_mgr,
                config,
                cache=True,
            )

        metadata = load_report_metadata_batch(cache_mgr, sample_report_configs)

        for config in sample_report_configs:
            get_data(
                mock_qb_api,
                cache_mgr,
                config,
                metadata,
                cache=True,
            )

        all_data = load_data_batch(cache_mgr, sample_report_configs, metadata)

        # Keys should be ReportConfig instances
        for key in all_data.keys():
            assert isinstance(key, ReportConfig)

        # Should be able to look up by config
        assert all_data[sample_report_configs[0]][0]["Name"] == "Alice"
        assert all_data[sample_report_configs[0]][0]["Name"] == "Alice"
        assert all_data[sample_report_configs[0]][0]["Name"] == "Alice"
        assert all_data[sample_report_configs[0]][0]["Name"] == "Alice"
