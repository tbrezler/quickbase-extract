"""Unified cache management for local dev and Lambda environments."""

import logging
import os
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages cache reads/writes for both local and Lambda environments.

    Supports local file-based caching and S3-backed caching on Lambda.
    Cache root path is configurable via QUICKBASE_CACHE_ROOT environment variable.
    """

    def __init__(self, cache_root: Path = None):
        """Initialize the cache manager.

        Args:
            cache_root: Path to cache root directory. If not provided, uses
                QUICKBASE_CACHE_ROOT env var, or defaults based on environment.
        """
        self.is_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
        self.environment = os.environ.get("ENV", "dev")
        self.s3_bucket = os.environ.get("CACHE_BUCKET")
        self.s3_client = boto3.client("s3") if self.is_lambda else None

        # Determine cache root path
        if cache_root:
            # Explicitly provided
            self.cache_root = Path(cache_root)
        elif os.environ.get("QUICKBASE_CACHE_ROOT"):
            # From environment variable
            self.cache_root = Path(os.environ.get("QUICKBASE_CACHE_ROOT"))
        else:
            # Default based on environment
            if self.is_lambda:
                self.cache_root = Path("/tmp/quickbase-extract/data")
            else:
                # Local: use current working directory or home
                self.cache_root = Path.cwd() / ".quickbase-cache" / self.environment

        self.cache_root.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache root: {self.cache_root}")

    def get_metadata_path(self, app_name: str, table_name: str, report_name: str) -> Path:
        """Get path for report metadata file.

        Args:
            app_name: Application name (not used in path, kept for API compatibility).
            table_name: Table name.
            report_name: Report name.

        Returns:
            Path object for the metadata file.
        """
        table_fmt = table_name.lower().replace(" ", "_")
        report_fmt = report_name.lower().replace(" ", "_")

        path = self.cache_root / "report_metadata" / f"{table_fmt}_{report_fmt}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_data_path(self, app_name: str, table_name: str, report_name: str) -> Path:
        """Get path for report data file.

        Args:
            app_name: Application name (not used in path, kept for API compatibility).
            table_name: Table name.
            report_name: Report name.

        Returns:
            Path object for the data file.
        """
        table_fmt = table_name.lower().replace(" ", "_")
        report_fmt = report_name.lower().replace(" ", "_")

        path = self.cache_root / "report_data" / f"{table_fmt}_{report_fmt}_data.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def write_file(self, file_path: Path, content: str) -> None:
        """Write cache file and sync to S3 if on Lambda.

        Args:
            file_path: Path where file should be written.
            content: String content to write.

        Raises:
            Exception: If S3 sync fails on Lambda.
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)

        if self.is_lambda and self.s3_client:
            self._sync_to_s3(file_path)

    def read_file(self, file_path: Path) -> str:
        """Read cache file.

        Args:
            file_path: Path to file to read.

        Returns:
            File contents as string.

        Raises:
            FileNotFoundError: If file does not exist.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Cache file not found: {file_path}")
        return file_path.read_text()

    def _sync_to_s3(self, file_path: Path) -> None:
        """Upload file to S3.

        Args:
            file_path: Path to file to upload.

        Raises:
            Exception: If upload fails.
        """
        try:
            relative_path = file_path.relative_to(self.cache_root)
            s3_key = f"{self.environment}/{relative_path}"
            self.s3_client.upload_file(str(file_path), self.s3_bucket, s3_key)
            logger.info(f"Synced {s3_key} to S3")
        except Exception as e:
            logger.error(f"Failed to sync {file_path} to S3: {e}")
            raise

    def sync_from_s3(self) -> None:
        """Download all cache files from S3 to /tmp (Lambda only).

        Only runs on Lambda. Logs and continues if bucket not configured.

        Raises:
            Exception: If S3 operations fail.
        """
        if not self.is_lambda or not self.s3_client:
            logger.debug("Not in Lambda or S3 client unavailable, skipping S3 sync")
            return

        if not self.s3_bucket:
            logger.debug("CACHE_BUCKET not set, skipping S3 sync")
            return

        logger.info(f"Syncing cache from S3 for environment: {self.environment}")
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=f"{self.environment}/")

            file_count = 0
            for page in pages:
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    if not s3_key or s3_key.endswith("/"):
                        continue

                    # Extract relative path (remove environment prefix)
                    relative_key = s3_key.replace(f"{self.environment}/", "", 1)
                    local_path = self.cache_root / relative_key

                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    self.s3_client.download_file(self.s3_bucket, s3_key, str(local_path))
                    file_count += 1

            logger.info(f"Synced {file_count} files from S3")
        except Exception as e:
            logger.error(f"Failed to sync from S3: {e}")
            raise


# Singleton instance
_cache_manager = None


def get_cache_manager(cache_root: Path = None) -> CacheManager:
    """Get or create cache manager instance.

    Args:
        cache_root: Optional path to cache root. Only used on first call.
            Subsequent calls with different cache_root will return the existing
            instance (cache_root parameter is ignored on subsequent calls).

    Returns:
        Singleton CacheManager instance.
    """
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager(cache_root=cache_root)
    return _cache_manager


def _reset_cache_manager() -> None:
    """Reset the singleton cache manager. For testing only."""
    global _cache_manager
    _cache_manager = None
