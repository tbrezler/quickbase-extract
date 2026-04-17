"""Integration tests for cache_freshness module."""

import time

from quickbase_extract.cache_freshness import check_cache_freshness, get_cache_files, get_cache_summary


class TestGetCacheFiles:
    """Tests for get_cache_files function."""

    def test_get_empty_cache(self, temp_cache_dir):
        """Test getting files from empty cache."""
        files = get_cache_files(cache_root=temp_cache_dir)

        assert files == []

    def test_get_single_file(self, temp_cache_dir):
        """Test getting a single cached file."""
        # Create a test file
        test_file = temp_cache_dir / "report_data" / "app" / "test_data.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"test": "data"}')

        files = get_cache_files(cache_root=temp_cache_dir)

        assert len(files) == 1
        assert files[0]["file"] == "report_data/app/test_data.json"
        # Size will be 16 (includes newline from write_text or JSON encoding)
        assert files[0]["size_bytes"] > 0

    def test_get_multiple_files(self, temp_cache_dir):
        """Test getting multiple cached files."""
        # Create multiple test files
        file1 = temp_cache_dir / "report_data" / "app1" / "data1.json"
        file1.parent.mkdir(parents=True)
        file1.write_text('{"data": 1}')

        file2 = temp_cache_dir / "report_metadata" / "app2" / "meta.json"
        file2.parent.mkdir(parents=True)
        file2.write_text('{"meta": 2}')

        files = get_cache_files(cache_root=temp_cache_dir)

        assert len(files) == 2
        file_names = [f["file"] for f in files]
        assert "report_data/app1/data1.json" in file_names
        assert "report_metadata/app2/meta.json" in file_names

    def test_files_sorted_by_age(self, temp_cache_dir):
        """Test that files are sorted by age (oldest first)."""
        import os

        # Create first file
        file1 = temp_cache_dir / "report_data" / "app" / "old.json"
        file1.parent.mkdir(parents=True)
        file1.write_text('{"old": 1}')

        # Create second file
        file2 = temp_cache_dir / "report_data" / "app" / "new.json"
        file2.write_text('{"new": 2}')

        # Manually set modification times to ensure difference
        # Set old.json to 1 hour ago
        old_mtime = time.time() - 3600
        os.utime(file1, (old_mtime, old_mtime))

        files = get_cache_files(cache_root=temp_cache_dir)

        # First file should have higher age_hours (older)
        assert files[0]["file"] == "report_data/app/old.json"
        assert files[1]["file"] == "report_data/app/new.json"
        assert files[0]["age_hours"] > files[1]["age_hours"]

    def test_file_metadata(self, temp_cache_dir):
        """Test that file metadata is complete."""
        test_file = temp_cache_dir / "report_data" / "app" / "test.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"test": "data"}')

        files = get_cache_files(cache_root=temp_cache_dir)

        file_info = files[0]
        assert "file" in file_info
        assert "path" in file_info
        assert "size_bytes" in file_info
        assert "size_mb" in file_info
        assert "modified" in file_info
        assert "age_hours" in file_info


class TestCheckCacheFreshness:
    """Tests for check_cache_freshness function."""

    def test_all_fresh_cache(self, temp_cache_dir):
        """Test when all cache files are fresh."""
        # Create recent file
        test_file = temp_cache_dir / "report_data" / "app" / "fresh.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"data": 1}')

        stale = check_cache_freshness(threshold_hours=24, cache_root=temp_cache_dir)

        assert stale == []

    def test_identify_stale_cache(self, temp_cache_dir):
        """Test identifying stale cache files."""
        # Create old file (manually set old mtime)
        test_file = temp_cache_dir / "report_data" / "app" / "old.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"data": 1}')

        # Set modification time to 48 hours ago
        old_mtime = time.time() - (48 * 3600)
        import os

        os.utime(test_file, (old_mtime, old_mtime))

        stale = check_cache_freshness(threshold_hours=24, cache_root=temp_cache_dir)

        assert len(stale) == 1
        assert stale[0]["file"] == "report_data/app/old.json"

    def test_custom_threshold(self, temp_cache_dir):
        """Test using custom staleness threshold."""
        # Create file
        test_file = temp_cache_dir / "report_data" / "app" / "test.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"data": 1}')

        # Set to 10 hours old
        old_mtime = time.time() - (10 * 3600)
        import os

        os.utime(test_file, (old_mtime, old_mtime))

        # Should be fresh with 24-hour threshold
        stale_24 = check_cache_freshness(threshold_hours=24, cache_root=temp_cache_dir)
        assert len(stale_24) == 0

        # Should be stale with 5-hour threshold
        stale_5 = check_cache_freshness(threshold_hours=5, cache_root=temp_cache_dir)
        assert len(stale_5) == 1

    def test_empty_cache(self, temp_cache_dir):
        """Test freshness check on empty cache."""
        stale = check_cache_freshness(cache_root=temp_cache_dir)

        assert stale == []


class TestGetCacheSummary:
    """Tests for get_cache_summary function."""

    def test_empty_cache_summary(self, temp_cache_dir):
        """Test summary of empty cache."""
        summary = get_cache_summary(cache_root=temp_cache_dir)

        assert summary["total_files"] == 0
        assert summary["total_size_mb"] == 0
        assert summary["oldest_file"] is None
        assert summary["newest_file"] is None

    def test_single_file_summary(self, temp_cache_dir):
        """Test summary with single file."""
        test_file = temp_cache_dir / "report_data" / "app" / "test.json"
        test_file.parent.mkdir(parents=True)
        test_file.write_text('{"test": "data"}')

        summary = get_cache_summary(cache_root=temp_cache_dir)

        assert summary["total_files"] == 1
        assert summary["oldest_file"] == "report_data/app/test.json"
        assert summary["newest_file"] == "report_data/app/test.json"

    def test_multiple_files_summary(self, temp_cache_dir):
        """Test summary with multiple files."""
        # Create multiple files
        for i in range(3):
            test_file = temp_cache_dir / "report_data" / "app" / f"file{i}.json"
            test_file.parent.mkdir(parents=True, exist_ok=True)
            test_file.write_text(f'{{"file": {i}}}')

        summary = get_cache_summary(cache_root=temp_cache_dir)

        assert summary["total_files"] == 3
        assert summary["total_size_mb"] >= 0  # Can be 0 for very small files

    def test_summary_contains_cache_dir(self, temp_cache_dir):
        """Test that summary includes cache directory path."""
        summary = get_cache_summary(cache_root=temp_cache_dir)

        assert "cache_dir" in summary
        assert str(temp_cache_dir) in summary["cache_dir"]
