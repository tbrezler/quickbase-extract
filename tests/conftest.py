"""Pytest configuration and shared fixtures."""

from unittest.mock import MagicMock

import pytest
from quickbase_extract.config import ReportConfig


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create a temporary cache directory for testing."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def mock_qb_api():
    """Create a mock Quickbase API client."""
    client = MagicMock()

    # Mock get_table_id
    client.get_table_id.return_value = "tblXYZ123"

    # Mock get_field_label_id_map
    client.get_field_label_id_map.return_value = {
        "Record ID#": "3",
        "Name": "6",
        "Email": "7",
        "Status": "8",
    }

    # Mock get_reports
    client.get_reports.return_value = [
        {"id": "rptABC123", "name": "Python"},
        {"id": "rptDEF456", "name": "Default"},
    ]

    # Mock get_report
    client.get_report.return_value = {
        "id": "rptABC123",
        "name": "Python",
        "query": {
            "fields": [3, 6, 7, 8],
            "sortBy": [{"fieldId": 6, "order": "ASC"}],
            "groupBy": [],
            "filter": "{8.EX.'Active'}",
        },
    }

    # Mock query_for_data
    client.query_for_data.return_value = {
        "data": [
            {
                "3": {"value": "1"},
                "6": {"value": "Alice"},
                "7": {"value": "alice@example.com"},
                "8": {"value": "Active"},
            },
            {
                "3": {"value": "2"},
                "6": {"value": "Bob"},
                "7": {"value": "bob@example.com"},
                "8": {"value": "Active"},
            },
        ]
    }

    # Mock upsert_records
    client.upsert_records.return_value = {
        "metadata": {
            "createdRecordIds": ["1"],
            "updatedRecordIds": ["2"],
            "unchangedRecordIds": [],
        }
    }

    # Mock delete_records
    client.delete_records.return_value = 5

    return client


@pytest.fixture
def sample_report_configs():
    """Sample report configurations as ReportConfig instances."""
    return [
        ReportConfig(
            app_id="appXYZ123",
            app_name="test_app",
            table_name="Test Table",
            report_name="Python",
        ),
        ReportConfig(
            app_id="appXYZ123",
            app_name="test_app",
            table_name="Another Table",
            report_name="Python",
        ),
    ]


@pytest.fixture
def sample_report_metadata(sample_report_configs):
    """Sample report metadata structure keyed by ReportConfig."""
    config1, config2 = sample_report_configs
    return {
        config1: {
            "app_name": "test_app",
            "table_name": "test_table",
            "report_name": "python",
            "table_id": "tblXYZ123",
            "field_label": {
                "Record ID#": "3",
                "Name": "6",
                "Email": "7",
                "Status": "8",
            },
            "fields": [3, 6, 7, 8],
            "filter": "{8.EX.'Active'}",
            "sort_by": [{"fieldId": 6, "order": "ASC"}],
            "group_by": [],
        },
        config2: {
            "app_name": "test_app",
            "table_name": "another_table",
            "report_name": "python",
            "table_id": "tblXYZ123",
            "field_label": {
                "Record ID#": "3",
                "Name": "6",
                "Email": "7",
                "Status": "8",
            },
            "fields": [3, 6, 7, 8],
            "filter": "{8.EX.'Active'}",
            "sort_by": [{"fieldId": 6, "order": "ASC"}],
            "group_by": [],
        },
    }


@pytest.fixture
def sample_report_data():
    """Sample report data (raw from API)."""
    return [
        {
            "3": {"value": "1"},
            "6": {"value": "Alice"},
            "7": {"value": "alice@example.com"},
            "8": {"value": "Active"},
        },
        {
            "3": {"value": "2"},
            "6": {"value": "Bob"},
            "7": {"value": "bob@example.com"},
            "8": {"value": "Active"},
        },
    ]


@pytest.fixture
def sample_transformed_data():
    """Sample report data after transformation (field labels as keys)."""
    return [
        {
            "Record ID#": "1",
            "Name": "Alice",
            "Email": "alice@example.com",
            "Status": "Active",
        },
        {
            "Record ID#": "2",
            "Name": "Bob",
            "Email": "bob@example.com",
            "Status": "Active",
        },
    ]


@pytest.fixture(autouse=True)
def monkeypatch_env(monkeypatch):
    """Clear AWS/Lambda env vars by default."""
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.delenv("CACHE_BUCKET", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("QUICKBASE_CACHE_ROOT", raising=False)


@pytest.fixture(autouse=True)
def caplog_setup(caplog):
    """Ensure logging is captured for all tests."""
    caplog.set_level("DEBUG")


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset all singleton instances before each test."""
    from quickbase_extract.cache_sync import _reset_cache_sync

    yield

    _reset_cache_sync()


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client for testing."""
    from unittest.mock import MagicMock

    return MagicMock()
