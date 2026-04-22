"""Unified cache management for local dev and Lambda environments."""

import logging
import os
import time
from pathlib import Path

import boto3

from quickbase_extract.utils import normalize_name

logger = logging.getLogger(__name__)

# Cache freshness thresholds (in hours)
# Metadata rarely changes, so longer threshold is acceptable
DEFAULT_METADATA_STALE_HOURS = 168  # 7 days
# Data should be refreshed more frequently
DEFAULT_DATA_STALE_HOURS = 24  # 1 day


class CacheManager:
    """Manages cache reads/writes for both local and Lambda environments.

    Supports local file-based caching and S3-backed caching on Lambda.
    Cache root path must be explicitly provided - no defaults to ensure
    clear intent and avoid configuration issues.

    Args:
        cache_root: Path to cache root directory (required). Should follow pattern:
            - Local: my-project/src/my_project/quickbase/dev/cache/
            - Lambda: /tmp/my_project/dev/cache/
        s3_bucket: S3 bucket name for Lambda persistence. If not provided,
            reads from CACHE_BUCKET environment variable.
        s3_prefix: Path prefix within S3 bucket. Should match cache_root structure.
            Example: "my_project/dev/cache"

    Example:
        >>> # Local development
        >>> cache_mgr = CacheManager(
        ...     cache_root=Path("my_project/quickbase/dev/cache"),
        ... )
        >>>
        >>> # Lambda with S3
        >>> cache_mgr = CacheManager(
        ...     cache_root=Path("/tmp/my_project/dev/cache"),
        ...     s3_bucket="mit-bio-quickbase",
        ...     s3_prefix="my_project/dev/cache",
        ... )
    """

    def __init__(
        self,
        cache_root: Path,
        s3_bucket: str | None = None,
        s3_prefix: str | None = None,
    ):
        """Initialize the cache manager.

        Args:
            cache_root: Path to cache root directory (required).
            s3_bucket: S3 bucket name. If not provided, reads from CACHE_BUCKET env var.
            s3_prefix: Path prefix within S3 bucket (required if using S3).

        Raises:
            ValueError: If cache_root is not provided or if s3_prefix is missing on Lambda.
        """
        if not cache_root:
            raise ValueError("cache_root is required")

        self.cache_root = Path(cache_root)
        self.is_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
        self.s3_bucket = s3_bucket or os.environ.get("CACHE_BUCKET")
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client("s3") if self.is_lambda else None

        # Validate S3 configuration on Lambda
        if self.is_lambda and self.s3_bucket and not self.s3_prefix:
            raise ValueError("s3_prefix is required when using S3 on Lambda")

        self.cache_root.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache root: {self.cache_root}")

    def get_metadata_path(self, app_name: str, table_name: str, report_name: str) -> Path:
        """Get path for report metadata file.

        Args:
            app_name: Application name.
            table_name: Table name.
            report_name: Report name.

        Returns:
            Path object for the metadata file.

        Example:
            >>> cache_mgr.get_metadata_path("Sales Tracker", "Opportunities", "Open Deals")
            PosixPath('my_project/dev/cache/report_metadata/sales_tracker/opportunities_open_deals.json')
        """
        app_fmt = normalize_name(app_name)
        table_fmt = normalize_name(table_name)
        report_fmt = normalize_name(report_name)

        path = self.cache_root / "report_metadata" / app_fmt / f"{table_fmt}_{report_fmt}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_data_path(self, app_name: str, table_name: str, report_name: str) -> Path:
        """Get path for report data file.

        Args:
            app_name: Application name.
            table_name: Table name.
            report_name: Report name.

        Returns:
            Path object for the data file.

        Example:
            >>> cache_mgr.get_data_path("Sales Tracker", "Opportunities", "Open Deals")
            PosixPath('my_project/dev/cache/report_data/sales_tracker/opportunities_open_deals_data.json')
        """
        app_fmt = normalize_name(app_name)
        table_fmt = normalize_name(table_name)
        report_fmt = normalize_name(report_name)

        path = self.cache_root / "report_data" / app_fmt / f"{table_fmt}_{report_fmt}_data.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def write_file(self, file_path: Path, content: str) -> None:
        """Write cache file and sync to S3 if on Lambda.

        Args:
            file_path: Path where file should be written.
            content: String content to write.

        Raises:
            Exception: If S3 sync fails on Lambda (required for operation success).

        Example:
            >>> cache_mgr.write_file(Path("metadata.json"), '{"key": "value"}')
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)

        if self.is_lambda and self.s3_client and self.s3_bucket:
            self._sync_to_s3(file_path)

    def read_file(self, file_path: Path) -> str:
        """Read cache file.

        Args:
            file_path: Path to file to read.

        Returns:
            File contents as string.

        Raises:
            FileNotFoundError: If file does not exist.

        Example:
            >>> content = cache_mgr.read_file(Path("metadata.json"))
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Cache file not found: {file_path}")
        return file_path.read_text()

    def _sync_to_s3(self, file_path: Path) -> None:
        """Upload file to S3 for persistence across Lambda invocations.

        Args:
            file_path: Path to file to upload.

        Raises:
            Exception: If upload fails. This is critical - Lambda /tmp is ephemeral.
        """
        try:
            relative_path = file_path.relative_to(self.cache_root)
            s3_key = f"{self.s3_prefix}/{relative_path}" if self.s3_prefix else str(relative_path)
            self.s3_client.upload_file(str(file_path), self.s3_bucket, s3_key)
            logger.info(f"Synced {s3_key} to S3")
        except Exception as e:
            logger.error(f"Failed to sync {file_path} to S3: {e}")
            raise

    def sync_from_s3(self) -> None:
        """Download all cache files from S3 to local cache_root (Lambda only).

        Restores cache from S3 at Lambda initialization. Only runs on Lambda.
        Logs and continues if bucket not configured.

        Raises:
            Exception: If S3 operations fail.

        Note:
            Lambda /tmp has storage limits (default 512 MB, max 10 GB).
            Current cache size (~32 MB) is well within limits.
        """
        if not self.is_lambda or not self.s3_client:
            logger.debug("Not in Lambda or S3 client unavailable, skipping S3 sync")
            return

        if not self.s3_bucket:
            logger.debug("CACHE_BUCKET not set, skipping S3 sync")
            return

        prefix = f"{self.s3_prefix}/" if self.s3_prefix else ""
        logger.info(f"Syncing cache from S3 bucket: {self.s3_bucket}, prefix: {prefix}")

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)

            file_count = 0
            for page in pages:
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    if not s3_key or s3_key.endswith("/"):
                        continue

                    # Extract relative path (remove prefix)
                    if self.s3_prefix:
                        relative_key = s3_key.replace(f"{self.s3_prefix}/", "", 1)
                    else:
                        relative_key = s3_key

                    local_path = self.cache_root / relative_key

                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    self.s3_client.download_file(self.s3_bucket, s3_key, str(local_path))
                    file_count += 1

            logger.info(f"Synced {file_count} files from S3")
        except Exception as e:
            logger.error(f"Failed to sync from S3: {e}")
            raise

    def is_cache_empty(self, cache_type: str = "metadata") -> bool:
        """Check if cache directory is empty or missing.

        Args:
            cache_type: Type of cache to check. Options: "metadata", "data".
                Defaults to "metadata".

        Returns:
            True if no cache files of the specified type exist, False otherwise.

        Raises:
            ValueError: If cache_type is not "metadata" or "data".

        Example:
            >>> if cache_mgr.is_cache_empty("metadata"):
            ...     print("Metadata cache is empty")
        """
        if cache_type not in ("metadata", "data"):
            raise ValueError(f"cache_type must be 'metadata' or 'data', got: {cache_type}")

        cache_dir = self.cache_root / f"report_{cache_type}"

        if not cache_dir.exists():
            logger.warning(f"Cache directory does not exist: {cache_dir}")
            return True

        # Check if directory has any .json files
        json_files = list(cache_dir.rglob("*.json"))
        if not json_files:
            logger.warning(f"Cache directory is empty: {cache_dir}")
            return True

        return False

    def get_cache_age_hours(self, cache_type: str = "metadata") -> float:
        """Get age of oldest file in cache directory.

        Returns the age of the oldest file in the specified cache directory.
        This helps determine if cache needs refreshing.

        Args:
            cache_type: Type of cache to check. Options: "metadata", "data".
                Defaults to "metadata".

        Returns:
            Age in hours of the oldest .json file. Returns 0 if no files found.

        Raises:
            ValueError: If cache_type is not "metadata" or "data".

        Example:
            >>> age = cache_mgr.get_cache_age_hours("metadata")
            >>> if age > 168:  # 7 days
            ...     print(f"Cache is {age} hours old, needs refresh")
        """
        if cache_type not in ("metadata", "data"):
            raise ValueError(f"cache_type must be 'metadata' or 'data', got: {cache_type}")

        cache_dir = self.cache_root / f"report_{cache_type}"

        if not cache_dir.exists():
            return 0

        json_files = list(cache_dir.rglob("*.json"))
        if not json_files:
            return 0

        oldest_mtime = min(f.stat().st_mtime for f in json_files)
        # 60 sec × 60 min = 3600
        age_hours = (time.time() - oldest_mtime) / 3600

        return round(age_hours, 1)
