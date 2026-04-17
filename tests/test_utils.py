"""Unit tests for utils module."""

import pytest
from quickbase_extract.utils import find_report, normalize_name


class TestNormalizeName:
    """Tests for normalize_name function."""

    def test_simple_name(self):
        """Test normalizing a simple name."""
        assert normalize_name("Test") == "test"

    def test_name_with_spaces(self):
        """Test normalizing a name with spaces."""
        assert normalize_name("Test Name") == "test_name"

    def test_name_with_multiple_spaces(self):
        """Test normalizing a name with multiple spaces."""
        assert normalize_name("Test  Name  Here") == "test__name__here"

    def test_mixed_case(self):
        """Test normalizing mixed case."""
        assert normalize_name("TeSt NaMe") == "test_name"

    def test_empty_string(self):
        """Test normalizing empty string."""
        assert normalize_name("") == ""

    def test_already_normalized(self):
        """Test normalizing already normalized string."""
        assert normalize_name("already_normalized") == "already_normalized"


class TestFindReport:
    """Tests for find_report function."""

    def test_find_existing_report(self, sample_report_configs):
        """Test finding an existing report."""
        result = find_report(sample_report_configs, "Test Report")
        assert result["Description"] == "Test Report"
        assert result["Table"] == "Test Table"

    def test_find_another_report(self, sample_report_configs):
        """Test finding another report."""
        result = find_report(sample_report_configs, "Another Report")
        assert result["Description"] == "Another Report"
        assert result["Table"] == "Another Table"

    def test_report_not_found(self, sample_report_configs):
        """Test error when report not found."""
        with pytest.raises(ValueError, match="No report found matching description"):
            find_report(sample_report_configs, "Nonexistent Report")

    def test_error_message_shows_available(self, sample_report_configs):
        """Test that error message lists available reports."""
        with pytest.raises(ValueError, match="Test Report"):
            find_report(sample_report_configs, "Nonexistent")

    def test_multiple_matches_warning(self, caplog, sample_report_configs):
        """Test that multiple matches log a warning but return first."""
        # Add duplicate
        configs = sample_report_configs + [sample_report_configs[0]]
        result = find_report(configs, "Test Report")
        assert result["Description"] == "Test Report"
        assert "Multiple reports match" in caplog.text

    def test_empty_reports_list(self):
        """Test error with empty reports list."""
        with pytest.raises(ValueError):
            find_report([], "Any Report")
