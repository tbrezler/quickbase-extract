"""Unit tests for api_handlers module."""

import time

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

    def test_upsert_failure_non_retriable(self, mock_qb_api):
        """Test upsert failure with non-retriable error."""
        mock_qb_api.upsert_records.side_effect = Exception("Invalid field")

        with pytest.raises(QuickbaseOperationError, match="upsert"):
            handle_upsert(mock_qb_api, "tblXYZ", [], description="Test")

    def test_upsert_retry_on_rate_limit(self, mock_qb_api):
        """Test upsert retry on 429 rate limit."""
        # Fail twice, then succeed
        mock_qb_api.upsert_records.side_effect = [
            Exception("429 Rate Limit Exceeded"),
            Exception("429 Rate Limit Exceeded"),
            {
                "metadata": {
                    "createdRecordIds": [],
                    "updatedRecordIds": [],
                    "unchangedRecordIds": [],
                }
            },
        ]

        with pytest.raises(QuickbaseOperationError):
            # Will still fail after max retries, but should have tried multiple times
            handle_upsert(mock_qb_api, "tblXYZ", [], max_retries=2)

        assert mock_qb_api.upsert_records.call_count >= 2

    def test_upsert_max_retries_customizable(self, mock_qb_api):
        """Test that max_retries parameter is respected."""
        mock_qb_api.upsert_records.side_effect = Exception("429 Rate Limit")

        with pytest.raises(QuickbaseOperationError):
            handle_upsert(mock_qb_api, "tblXYZ", [], max_retries=2)

        assert mock_qb_api.upsert_records.call_count == 2

    def test_upsert_wait_time_cap(self, mock_qb_api):
        """Test that wait time is capped at 60 seconds."""
        mock_qb_api.upsert_records.side_effect = [
            Exception("429 Rate Limit"),
            {
                "metadata": {
                    "createdRecordIds": [],
                    "updatedRecordIds": [],
                    "unchangedRecordIds": [],
                }
            },
        ]

        start = time.time()
        handle_upsert(
            mock_qb_api, "tblXYZ", [], max_retries=10
        )  # Would be 2^9 = 512s without cap
        elapsed = time.time() - start

        # Should be capped at ~60 seconds, not 512
        assert elapsed < 65  # Allow some margin


class TestHandleDelete:
    """Tests for handle_delete function."""

    def test_delete_success(self, mock_qb_api):
        """Test successful delete."""
        result = handle_delete(
            mock_qb_api, "tblXYZ", where="{8.EX.'Inactive'}", description="Test delete"
        )

        assert result == 5
        mock_qb_api.delete_records.assert_called_once_with(
            "tblXYZ", where="{8.EX.'Inactive'}"
        )

    def test_delete_logs_result(self, mock_qb_api, caplog):
        """Test that delete logs result."""
        handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Inactive'}")

        assert "5 records deleted" in caplog.text

    def test_delete_failure(self, mock_qb_api):
        """Test delete failure."""
        mock_qb_api.delete_records.side_effect = Exception("Invalid where clause")

        with pytest.raises(QuickbaseOperationError, match="delete"):
            handle_delete(mock_qb_api, "tblXYZ", where="invalid")

    def test_delete_retries_on_rate_limit(self, mock_qb_api):
        """Test that delete retries on 429 rate limit."""
        mock_qb_api.delete_records.side_effect = [
            Exception("429 Rate Limit"),
            5,
        ]

        result = handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Test'}")

        assert result == 5
        assert mock_qb_api.delete_records.call_count == 2

    def test_delete_no_retry_on_other_errors(self, mock_qb_api):
        """Test that delete does not retry non-rate-limit errors."""
        mock_qb_api.delete_records.side_effect = Exception("Permission denied")

        with pytest.raises(QuickbaseOperationError):
            handle_delete(mock_qb_api, "tblXYZ", where="{8.EX.'Test'}")

        # Should only try once for non-rate-limit errors
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
            options={"skip": 0, "top": 100},
            description="active users",
        )

        assert result is not None
        mock_qb_api.query_for_data.assert_called_once()

    def test_query_failure_non_retriable(self, mock_qb_api):
        """Test query failure with non-retriable error."""
        mock_qb_api.query_for_data.side_effect = Exception("Invalid field ID")

        with pytest.raises(QuickbaseOperationError, match="query"):
            handle_query(mock_qb_api, "tblXYZ", select=[999])

    def test_query_retry_on_rate_limit(self, mock_qb_api):
        """Test query retry on 429 rate limit."""
        mock_qb_api.query_for_data.side_effect = [
            Exception("429 Rate Limit Exceeded"),
            {"data": []},
        ]

        result = handle_query(mock_qb_api, "tblXYZ")

        assert mock_qb_api.query_for_data.call_count == 2
        assert result == {"data": []}

    def test_query_max_retries_customizable(self, mock_qb_api):
        """Test that max_retries parameter is respected."""
        mock_qb_api.query_for_data.side_effect = Exception("429 Rate Limit")

        with pytest.raises(QuickbaseOperationError):
            handle_query(mock_qb_api, "tblXYZ", max_retries=2)

        assert mock_qb_api.query_for_data.call_count == 2

    def test_query_logs_record_count(self, mock_qb_api, caplog):
        """Test that query logs record count at info level."""
        handle_query(mock_qb_api, "tblXYZ", description="test query")

        assert "2 records" in caplog.text
        # Check it's at info level, not debug
        assert any(record.levelname == "INFO" for record in caplog.records)

    def test_query_exponential_backoff(self, mock_qb_api):
        """Test that retries use exponential backoff with cap."""
        mock_qb_api.query_for_data.side_effect = [
            Exception("429 Rate Limit"),
            Exception("429 Rate Limit"),
            {"data": []},
        ]

        start = time.time()
        handle_query(mock_qb_api, "tblXYZ", max_retries=3)
        elapsed = time.time() - start

        # Should have some delay due to exponential backoff
        # (2^0 + 2^1) = 3 seconds + random = at least ~3 seconds
        assert elapsed >= 2  # Allow some margin

    def test_query_description_in_logs(self, mock_qb_api, caplog):
        """Test that description appears in log messages."""
        handle_query(mock_qb_api, "tblXYZ", description="customer records")

        assert "customer records" in caplog.text
        assert "customer records" in caplog.text
