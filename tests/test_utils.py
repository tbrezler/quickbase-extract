"""Unit tests for utils module."""

from quickbase_extract.utils import normalize_name


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
        assert normalize_name("already_normalized") == "already_normalized"
