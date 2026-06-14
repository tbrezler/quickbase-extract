"""Unit tests for api_handlers module."""

import pytest

from quickbase_extract.api_handlers import (
    QuickbaseOperationError,
    handle_delete,
    handle_query,
    handle_upsert,
)


class TestQuickbaseOperationError:
    """Tests for QuickbaseOperationError exception."""

    def test_error_message(self):
        """Test error message format."""
        error = QuickbaseOperationError("upsert", "Rate limited")
        assert "Quickbase upsert failed" in str(error)
        assert "Rate limited" in str(error)

    def test_error_attributes(self):
        """Test error attributes."""
        error = QuickbaseOperationError("delete", "Record not found")
        assert error.operation == "delete"
        assert error.details == "Record not found"


class TestHandleUpsert:
    """Tests for handle_upsert function."""

    def test_upsert_success(self, mock_qb_api):
        """Test successful upsert."""
        data = [{"field1": "value1"}]

        result = handle_upsert(mock_qb_api, "tblXYZ", data, description="Test upsert")

        assert "metadata" in result
        mock_qb_api.upsert_records.assert_called_once_with("tblXYZ", data=data)

    def test_upsert_logs_result(self, mock_qb_api, caplog):
        """Test that upsert logs result counts."""
        data = [{"field1": "value1"}]

        handle_upsert(mock_qb_api, "tblXYZ", data, description="Test upsert")

        assert "1 created" in caplog.text
        assert "1 updated" in caplog.text

    def test_upsert_failure(self, mock_qb_api):
        """Test that upsert failure raises QuickbaseOperationError."""
        mock_qb_api.upsert_records.side_effect = Exception("Invalid field")

        with pytest.raises(QuickbaseOperationError, match="upsert"):
            handle_upsert(mock_qb_api, "tblXYZ", [], description="Test")


class TestHandleDelete:
    """Tests for handle_delete function."""

    def test_delete_success(self, mock_qb_api):
        """Test successful delete."""
        result = handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Inactive'}", description="Test delete")

        assert result == 5
        mock_qb_api.delete_records.assert_called_once_with("tblXYZ", where="{8.EX.'Inactive'}")

    def test_delete_logs_result(self, mock_qb_api, caplog):
        """Test that delete logs result."""
        handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Inactive'}")

        assert "5 records deleted" in caplog.text

    def test_delete_failure(self, mock_qb_api):
        """Test that delete failure raises QuickbaseOperationError."""
        mock_qb_api.delete_records.side_effect = Exception("Invalid where clause")

        with pytest.raises(QuickbaseOperationError, match="delete"):
            handle_delete(mock_qb_api, "tblXYZ", where="invalid")

    def test_delete_failure_single_attempt(self, mock_qb_api):
        """Test that delete fails immediately without retry."""
        mock_qb_api.delete_records.side_effect = Exception("Permission denied")

        with pytest.raises(QuickbaseOperationError):
            handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Test'}")

        assert mock_qb_api.delete_records.call_count == 1


class TestHandleQuery:
    """Tests for handle_query function."""

    def test_query_success(self, mock_qb_api):
        """Test successful query."""
        result = handle_query(
            mock_qb_api,
            "tblXYZ",
            select=[3, 6, 7],
            where="{8.EX.'Active'}",
        )

        assert "data" in result
        assert len(result["data"]) == 2

    def test_query_with_all_parameters(self, mock_qb_api):
        """Test query with all optional parameters including description."""
        result = handle_query(
            mock_qb_api,
            "tblXYZ",
            select=[3, 6],
            where="{8.EX.'Active'}",
            sort_by=[{"fieldId": 6, "order": "ASC"}],
            group_by=[{"fieldId": 8}],
            skip=0,
            top=100,
            description="active users",
        )

        assert result is not None
        mock_qb_api.query_for_data.assert_called_once()

    def test_query_failure(self, mock_qb_api):
        """Test that query failure raises QuickbaseOperationError."""
        mock_qb_api.query_for_data.side_effect = Exception("Invalid field ID")

        with pytest.raises(QuickbaseOperationError, match="query"):
            handle_query(mock_qb_api, "tblXYZ", select=[999])

    def test_query_description_in_logs(self, mock_qb_api, caplog):
        """Test that description appears in error log message on failure."""
        mock_qb_api.query_for_data.side_effect = Exception("Invalid field ID")

        with pytest.raises(QuickbaseOperationError):
            handle_query(mock_qb_api, "tblXYZ", description="customer records")

        assert "customer records" in caplog.text
        assert any(record.levelname == "ERROR" for record in caplog.records)
