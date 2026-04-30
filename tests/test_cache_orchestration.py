"""Unit tests for cache_orchestration module."""

import logging
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.cache_orchestration import (
    CacheRefreshError,
    _determine_refresh_needs,
    _get_missing_reports,
    _refresh_data_cache,
    _refresh_metadata_cache,
    ensure_cache_freshness,
)


class TestGetMissingReports:
    """Tests for _get_missing_reports helper function."""

    def test_no_missing_reports_metadata(self, temp_cache_dir, sample_report_configs):
        """Test returns empty list when all metadata exists."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create metadata for all reports (use normalized names)
        for config in sample_report_configs:
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            # Use normalized table name (lowercase, underscores)
            normalized_table = config.table_name.lower().replace(" ", "_")
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        missing = _get_missing_reports(cache_mgr, sample_report_configs, cache_type="metadata")
        assert missing == []

    def test_no_missing_reports_data(self, temp_cache_dir, sample_report_configs):
        """Test returns empty list when all data exists."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create data for all reports (use normalized names)
        for config in sample_report_configs:
            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            # Use normalized table name (lowercase, underscores)
            normalized_table = config.table_name.lower().replace(" ", "_")
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        missing = _get_missing_reports(cache_mgr, sample_report_configs, cache_type="data")
        assert missing == []

    def test_all_missing_reports(self, temp_cache_dir, sample_report_configs):
        """Test returns all reports when none exist in cache."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        missing = _get_missing_reports(cache_mgr, sample_report_configs, cache_type="metadata")
        assert missing == sample_report_configs

    def test_partially_missing_reports(self, temp_cache_dir, sample_report_configs):
        """Test returns only missing reports when some exist."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create metadata for first report only (use normalized names)
        first_config = sample_report_configs[0]
        metadata_dir = temp_cache_dir / "report_metadata" / first_config.app_name
        metadata_dir.mkdir(parents=True)
        normalized_table = first_config.table_name.lower().replace(" ", "_")
        (metadata_dir / f"{normalized_table}_{first_config.report_name}.json").write_text("{}")

        missing = _get_missing_reports(cache_mgr, sample_report_configs, cache_type="metadata")
        assert first_config not in missing
        assert len(missing) == len(sample_report_configs) - 1


class TestDetermineRefreshNeeds:
    """Tests for _determine_refresh_needs helper function."""

    def test_force_flag_returns_all_reports(self, sample_report_configs):
        """Test force=True always returns refresh needed with all reports."""
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=False,
            cache_age=1.0,
            stale_hours=24.0,
            missing_reports=[],
            all_reports=sample_report_configs,
            force=True,
            cache_type="metadata",
        )

        assert needs_refresh is True
        assert reports == sample_report_configs
        assert reasons == ["force=True"]

    def test_empty_cache_returns_all_reports(self, sample_report_configs):
        """Test empty cache triggers refresh of all reports."""
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=True,
            cache_age=0.0,
            stale_hours=24.0,
            missing_reports=[],
            all_reports=sample_report_configs,
            force=False,
            cache_type="metadata",
        )

        assert needs_refresh is True
        assert reports == sample_report_configs
        assert reasons == ["metadata empty"]

    def test_stale_cache_returns_all_reports(self, sample_report_configs):
        """Test stale cache triggers refresh of all reports."""
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=False,
            cache_age=48.0,
            stale_hours=24.0,
            missing_reports=[],
            all_reports=sample_report_configs,
            force=False,
            cache_type="data",
        )

        assert needs_refresh is True
        assert reports == sample_report_configs
        assert reasons == ["data stale (48.0h > 24.0h)"]

    def test_missing_reports_returns_only_missing(self, sample_report_configs):
        """Test missing reports triggers refresh of only those reports."""
        missing = [sample_report_configs[0]]
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=False,
            cache_age=1.0,
            stale_hours=24.0,
            missing_reports=missing,
            all_reports=sample_report_configs,
            force=False,
            cache_type="metadata",
        )

        assert needs_refresh is True
        assert reports == missing
        assert reasons == ["1 report(s) missing metadata"]

    def test_fresh_cache_no_refresh_needed(self, sample_report_configs):
        """Test fresh cache with no missing reports needs no refresh."""
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=False,
            cache_age=1.0,
            stale_hours=24.0,
            missing_reports=[],
            all_reports=sample_report_configs,
            force=False,
            cache_type="metadata",
        )

        assert needs_refresh is False
        assert reports == []
        assert reasons == []

    def test_force_overrides_fresh_cache(self, sample_report_configs):
        """Test force=True overrides fresh cache state."""
        needs_refresh, reports, reasons = _determine_refresh_needs(
            cache_empty=False,
            cache_age=0.5,
            stale_hours=24.0,
            missing_reports=[],
            all_reports=sample_report_configs,
            force=True,
            cache_type="data",
        )

        assert needs_refresh is True
        assert reports == sample_report_configs
        assert reasons == ["force=True"]


class TestRefreshMetadataCache:
    """Tests for _refresh_metadata_cache helper function."""

    def test_successful_refresh(self, temp_cache_dir, sample_report_configs, caplog):
        """Test successful metadata refresh logs correctly."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_refresh:
            caplog.set_level(logging.INFO)
            _refresh_metadata_cache(
                client=mock_client,
                cache_manager=cache_mgr,
                reports_to_refresh=sample_report_configs,
                reasons=["metadata empty"],
            )

            mock_refresh.assert_called_once_with(
                mock_client,
                cache_manager=cache_mgr,
                report_configs=sample_report_configs,
                cache=True,
            )
            assert "Metadata cache refresh needed: metadata empty" in caplog.text
            assert "Metadata cache refresh completed successfully" in caplog.text

    def test_refresh_failure_raises_cache_refresh_error(self, temp_cache_dir, sample_report_configs, caplog):
        """Test metadata refresh failure raises CacheRefreshError."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_refresh:
            mock_refresh.side_effect = Exception("API error")

            with pytest.raises(CacheRefreshError) as exc_info:
                _refresh_metadata_cache(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    reports_to_refresh=sample_report_configs,
                    reasons=["force=True"],
                )

            assert "Failed to refresh metadata cache" in str(exc_info.value)
            assert "API error" in str(exc_info.value)
            assert "Metadata cache refresh failed" in caplog.text

    def test_multiple_reasons_logged(self, temp_cache_dir, sample_report_configs, caplog):
        """Test multiple refresh reasons are logged correctly."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel"):
            caplog.set_level(logging.WARNING)
            _refresh_metadata_cache(
                client=mock_client,
                cache_manager=cache_mgr,
                reports_to_refresh=sample_report_configs,
                reasons=["metadata empty", "force=True"],
            )

            assert "metadata empty; force=True" in caplog.text


class TestRefreshDataCache:
    """Tests for _refresh_data_cache helper function."""

    def test_successful_refresh(self, temp_cache_dir, sample_report_configs, caplog):
        """Test successful data refresh logs correctly."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_refresh:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                caplog.set_level(logging.INFO)
                _refresh_data_cache(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    reports_to_refresh=sample_report_configs,
                    reasons=["data stale (48.0h > 24.0h)"],
                )

                mock_load.assert_called_once_with(cache_mgr, sample_report_configs)
                mock_refresh.assert_called_once_with(
                    mock_client,
                    cache_manager=cache_mgr,
                    report_configs=sample_report_configs,
                    report_metadata={config: {} for config in sample_report_configs},
                    cache=True,
                    ask_values=None,
                )
                assert "Data cache refresh needed: data stale" in caplog.text
                assert "Data cache refresh completed successfully" in caplog.text

    def test_refresh_failure_raises_cache_refresh_error(self, temp_cache_dir, sample_report_configs, caplog):
        """Test data refresh failure raises CacheRefreshError."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_refresh:
                mock_load.return_value = {config: {} for config in sample_report_configs}
                mock_refresh.side_effect = Exception("Network timeout")

                with pytest.raises(CacheRefreshError) as exc_info:
                    _refresh_data_cache(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        reports_to_refresh=sample_report_configs,
                        reasons=["data empty"],
                    )

                assert "Failed to refresh data cache" in str(exc_info.value)
                assert "Network timeout" in str(exc_info.value)
                assert "Data cache refresh failed" in caplog.text

    def test_metadata_load_failure_raises_cache_refresh_error(self, temp_cache_dir, sample_report_configs):
        """Test metadata load failure during data refresh raises CacheRefreshError."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            mock_load.side_effect = Exception("Metadata not found")

            with pytest.raises(CacheRefreshError) as exc_info:
                _refresh_data_cache(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    reports_to_refresh=sample_report_configs,
                    reasons=["data empty"],
                )

            assert "Failed to refresh data cache" in str(exc_info.value)
            assert "Metadata not found" in str(exc_info.value)


class TestEnsureCacheFreshnessIntegration:
    """Integration tests for ensure_cache_freshness orchestration function."""

    def test_fresh_cache_no_refresh(self, temp_cache_dir, caplog, sample_report_configs):
        """Test no refresh occurs when caches are fresh."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata and data (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        caplog.set_level(logging.DEBUG)
        ensure_cache_freshness(
            client=mock_client,
            cache_manager=cache_mgr,
            report_configs_all=sample_report_configs,
            report_configs_to_cache=sample_report_configs,
            metadata_stale_hours=168,
            data_stale_hours=24,
        )

        assert "Cache is fresh" in caplog.text
        assert "metadata age" in caplog.text
        assert "data age" in caplog.text

    def test_metadata_empty_triggers_refresh(self, temp_cache_dir, sample_report_configs):
        """Test empty metadata cache triggers refresh."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            ensure_cache_freshness(
                client=mock_client,
                cache_manager=cache_mgr,
                report_configs_all=sample_report_configs,
                report_configs_to_cache=None,
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

            mock_meta.assert_called_once_with(
                mock_client,
                cache_manager=cache_mgr,
                report_configs=sample_report_configs,
                cache=True,
            )

    def test_data_empty_triggers_refresh(self, temp_cache_dir, sample_report_configs):
        """Test empty data cache triggers refresh."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=sample_report_configs,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                )

                mock_data.assert_called_once()

    def test_stale_metadata_triggers_refresh_all_reports(self, temp_cache_dir, caplog, sample_report_configs):
        """Test stale metadata triggers refresh of all reports."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create old metadata (10 days) with normalized names
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            metadata_file = metadata_dir / f"{normalized_table}_{config.report_name}.json"
            metadata_file.write_text("{}")
            old_time = time.time() - (10 * 24 * 3600)
            os.utime(metadata_file, (old_time, old_time))

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            caplog.set_level(logging.WARNING)
            ensure_cache_freshness(
                client=mock_client,
                cache_manager=cache_mgr,
                report_configs_all=sample_report_configs,
                report_configs_to_cache=None,
                metadata_stale_hours=168,  # 7 days
                data_stale_hours=24,
            )

            mock_meta.assert_called_once()
            assert "metadata stale" in caplog.text.lower()

    def test_stale_data_triggers_refresh_all_data_reports(self, temp_cache_dir, caplog, sample_report_configs):
        """Test stale data triggers refresh of all data cache reports."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata and old data (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            data_file = data_dir / f"{normalized_table}_{config.report_name}_data.json"
            data_file.write_text("{}")
            old_time = time.time() - (2 * 24 * 3600)
            os.utime(data_file, (old_time, old_time))

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                caplog.set_level(logging.WARNING)
                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=sample_report_configs,
                    metadata_stale_hours=168,
                    data_stale_hours=24,  # 1 day
                )

                mock_data.assert_called_once()
                assert "data stale" in caplog.text.lower()

    def test_missing_metadata_reports_refreshes_only_missing(self, temp_cache_dir, caplog, sample_report_configs):
        """Test missing metadata reports triggers refresh of only those reports."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create metadata for all but the last report (use normalized names)
        for config in sample_report_configs[:-1]:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            caplog.set_level(logging.WARNING)
            ensure_cache_freshness(
                client=mock_client,
                cache_manager=cache_mgr,
                report_configs_all=sample_report_configs,
                report_configs_to_cache=None,
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

            # Should only refresh the missing report
            mock_meta.assert_called_once()
            call_args = mock_meta.call_args
            assert call_args[1]["report_configs"] == [sample_report_configs[-1]]
            assert "report(s) missing metadata" in caplog.text

    def test_missing_data_reports_refreshes_only_missing(self, temp_cache_dir, caplog, sample_report_configs):
        """Test missing data reports triggers refresh of only those reports."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata for all and data for all but the last report (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        for config in sample_report_configs[:-1]:
            normalized_table = config.table_name.lower().replace(" ", "_")
            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {sample_report_configs[-1]: {}}

                caplog.set_level(logging.WARNING)
                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=sample_report_configs,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                )

                # Should only refresh the missing report
                mock_data.assert_called_once()
                call_args = mock_data.call_args
                assert call_args[1]["report_configs"] == [sample_report_configs[-1]]
                assert "report(s) missing data" in caplog.text

    def test_force_all_refreshes_both_caches(self, temp_cache_dir, sample_report_configs):
        """Test force_all=True refreshes both caches even when fresh."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh caches (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {config: {} for config in sample_report_configs}

                    ensure_cache_freshness(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        report_configs_all=sample_report_configs,
                        report_configs_to_cache=sample_report_configs,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                        force_all=True,
                    )

                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_force_metadata_only(self, temp_cache_dir, sample_report_configs):
        """Test force_metadata=True refreshes only metadata."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh caches (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=sample_report_configs,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                    force_metadata=True,
                )

                mock_meta.assert_called_once()
                mock_data.assert_not_called()

    def test_force_data_only(self, temp_cache_dir, sample_report_configs):
        """Test force_data=True refreshes only data."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh caches (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=sample_report_configs,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                    force_data=True,
                )

                mock_data.assert_called_once()

    def test_force_all_overrides_individual_flags(self, temp_cache_dir, sample_report_configs):
        """Test force_all=True overrides individual force flags."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh caches (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / f"{normalized_table}_{config.report_name}_data.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {config: {} for config in sample_report_configs}

                    # force_all=True should refresh both, even with force_metadata=False
                    ensure_cache_freshness(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        report_configs_all=sample_report_configs,
                        report_configs_to_cache=sample_report_configs,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                        force_all=True,
                        force_metadata=False,
                        force_data=False,
                    )

                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_data_caching_disabled_when_no_configs(self, temp_cache_dir, caplog, sample_report_configs):
        """Test data caching is disabled when report_configs_to_cache is None."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
            caplog.set_level(logging.DEBUG)
            ensure_cache_freshness(
                client=mock_client,
                cache_manager=cache_mgr,
                report_configs_all=sample_report_configs,
                report_configs_to_cache=None,  # Disabled
                metadata_stale_hours=168,
                data_stale_hours=24,
            )

            assert "Data caching is disabled" in caplog.text
            mock_data.assert_not_called()

    def test_cache_all_data_flag(self, temp_cache_dir, sample_report_configs):
        """Test cache_all_data=True caches all reports."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=None,  # Not provided
                    cache_all_data=True,  # But this enables it
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                    force_data=True,  # Force to trigger refresh
                )

                # Should cache all reports
                mock_data.assert_called_once()
                call_args = mock_data.call_args
                assert call_args[1]["report_configs"] == sample_report_configs

    def test_cache_all_data_overrides_subset(self, temp_cache_dir, sample_report_configs):
        """Test cache_all_data=True overrides report_configs_to_cache subset."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}

                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=[sample_report_configs[0]],  # Subset
                    cache_all_data=True,  # Overrides subset
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                    force_data=True,
                )

                # Should use all configs, not just the subset
                mock_data.assert_called_once()
                call_args = mock_data.call_args
                assert call_args[1]["report_configs"] == sample_report_configs

    def test_metadata_refresh_failure_raises_error(self, temp_cache_dir, caplog, sample_report_configs):
        """Test metadata refresh failure raises CacheRefreshError."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            mock_meta.side_effect = Exception("API timeout")

            with pytest.raises(CacheRefreshError) as exc_info:
                ensure_cache_freshness(
                    client=mock_client,
                    cache_manager=cache_mgr,
                    report_configs_all=sample_report_configs,
                    report_configs_to_cache=None,
                    metadata_stale_hours=168,
                    data_stale_hours=24,
                )

            assert "Failed to refresh metadata cache" in str(exc_info.value)
            assert "API timeout" in str(exc_info.value)
            assert "Metadata cache refresh failed" in caplog.text

    def test_data_refresh_failure_raises_error(self, temp_cache_dir, caplog, sample_report_configs):
        """Test data refresh failure raises CacheRefreshError."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (use normalized names)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_load.return_value = {config: {} for config in sample_report_configs}
                mock_data.side_effect = Exception("Network error")

                with pytest.raises(CacheRefreshError) as exc_info:
                    ensure_cache_freshness(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        report_configs_all=sample_report_configs,
                        report_configs_to_cache=sample_report_configs,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                    )

                assert "Failed to refresh data cache" in str(exc_info.value)
                assert "Network error" in str(exc_info.value)
                assert "Data cache refresh failed" in caplog.text

    def test_metadata_failure_prevents_data_refresh(self, temp_cache_dir, sample_report_configs):
        """Test metadata refresh failure prevents data refresh from being attempted."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)
        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                mock_meta.side_effect = Exception("Metadata failed")

                with pytest.raises(CacheRefreshError):
                    ensure_cache_freshness(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        report_configs_all=sample_report_configs,
                        report_configs_to_cache=sample_report_configs,
                        metadata_stale_hours=168,
                        data_stale_hours=24,
                    )

                # Data refresh should NOT be attempted
                mock_data.assert_not_called()

    def test_both_caches_stale_refreshes_both(self, temp_cache_dir, sample_report_configs):
        """Test both stale caches are refreshed."""
        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create old metadata (10 days) and old data (2 days) with normalized names
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")

            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            metadata_file = metadata_dir / f"{normalized_table}_{config.report_name}.json"
            metadata_file.write_text("{}")
            old_time = time.time() - (10 * 24 * 3600)
            os.utime(metadata_file, (old_time, old_time))

            data_dir = temp_cache_dir / "report_data" / config.app_name
            data_dir.mkdir(parents=True, exist_ok=True)
            data_file = data_dir / f"{normalized_table}_{config.report_name}_data.json"
            data_file.write_text("{}")
            old_time = time.time() - (2 * 24 * 3600)
            os.utime(data_file, (old_time, old_time))

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            with patch("quickbase_extract.cache_orchestration.load_report_metadata_batch") as mock_load:
                with patch("quickbase_extract.cache_orchestration.get_data_parallel") as mock_data:
                    mock_load.return_value = {config: {} for config in sample_report_configs}

                    ensure_cache_freshness(
                        client=mock_client,
                        cache_manager=cache_mgr,
                        report_configs_all=sample_report_configs,
                        report_configs_to_cache=sample_report_configs,
                        metadata_stale_hours=168,  # 7 days
                        data_stale_hours=24,  # 1 day
                    )

                    mock_meta.assert_called_once()
                    mock_data.assert_called_once()

    def test_environment_variable_thresholds(self, temp_cache_dir, monkeypatch, sample_report_configs):
        """Test staleness thresholds can be set via environment variables."""
        monkeypatch.setenv("METADATA_STALE_HOURS", "48")
        monkeypatch.setenv("DATA_STALE_HOURS", "12")

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create metadata that's 3 days old (should be stale with 48h threshold)
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            metadata_file = metadata_dir / f"{normalized_table}_{config.report_name}.json"
            metadata_file.write_text("{}")
            old_time = time.time() - (3 * 24 * 3600)  # 3 days old
            os.utime(metadata_file, (old_time, old_time))

        mock_client = MagicMock()

        with patch("quickbase_extract.cache_orchestration.get_report_metadata_parallel") as mock_meta:
            ensure_cache_freshness(
                client=mock_client,
                cache_manager=cache_mgr,
                report_configs_all=sample_report_configs,
                report_configs_to_cache=None,
                # Don't pass thresholds - should read from env vars
            )

            # Should refresh because 3 days > 48 hours
            mock_meta.assert_called_once()

    def test_parameter_thresholds_override_environment(
        self, temp_cache_dir, monkeypatch, caplog, sample_report_configs
    ):
        """Test parameter thresholds override environment variables."""
        monkeypatch.setenv("METADATA_STALE_HOURS", "1")  # Very short threshold in env

        cache_mgr = CacheManager(cache_root=temp_cache_dir)

        # Create fresh metadata (less than 1 hour old) with normalized names
        for config in sample_report_configs:
            normalized_table = config.table_name.lower().replace(" ", "_")
            metadata_dir = temp_cache_dir / "report_metadata" / config.app_name
            metadata_dir.mkdir(parents=True, exist_ok=True)
            (metadata_dir / f"{normalized_table}_{config.report_name}.json").write_text("{}")

        mock_client = MagicMock()

        caplog.set_level(logging.DEBUG)
        ensure_cache_freshness(
            client=mock_client,
            cache_manager=cache_mgr,
            report_configs_all=sample_report_configs,
            report_configs_to_cache=None,
            metadata_stale_hours=168,  # Override with longer threshold
            data_stale_hours=24,
        )

        # Should be fresh because parameter overrides env var
        assert "Cache is fresh" in caplog.text
        assert "Cache is fresh" in caplog.text
