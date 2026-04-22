# Quickbase Extract

A Python package for efficiently extracting, transforming, and caching data from Quickbase reports with built-in error handling, retry logic, and S3 support for AWS Lambda environments.

## Features

- **🚀 Parallel Processing** - Fetch multiple reports concurrently for improved performance
- **💾 Smart Caching** - Local and S3-backed caching to minimize API calls
- **🔄 Automatic Retries** - Built-in retry logic with exponential backoff for rate limits
- **☁️ Lambda Ready** - First-class support for AWS Lambda with S3 cache sync
- **🎯 Type Safe** - Full type hints with TypedDict for better IDE support
- **📊 Cache Monitoring** - Tools to check cache freshness and manage stale data
- **🛡️ Robust Error Handling** - Comprehensive error handling with detailed logging
- **🔍 Data Transformation** - Automatically converts field IDs to human-readable labels

## Installation

```bash
pip install quickbase-extract
```

### Requirements

- Python 3.9+
- `quickbase-api` - Quickbase API client
- `boto3` - AWS SDK (for Lambda/S3 support)

## Quick Start

### Basic Usage

```python
import quickbase_api
from pathlib import Path
from quickbase_extract import (
    CacheManager,
    load_report_metadata_batch,
    get_data_parallel
)
from quickbase_extract.cache_orchestration import ensure_cache_freshness

# Initialize Quickbase client
client = quickbase_api.client(
    realm="your-realm.quickbase.com",
    user_token="YOUR_USER_TOKEN"
)

# Initialize cache manager
cache_mgr = CacheManager(
    cache_root=Path("my_project/dev/cache"),
    s3_bucket="my-bucket",  # Optional: for Lambda
    s3_prefix="my_project/dev/cache"  # Optional: for Lambda
)

# Define report configurations
report_configs = [
    {
        "Description": "active_customers",
        "App": "Sales Tracker",
        "App ID": "bq8xyx9z",
        "Table": "Customers",
        "Report": "Active Customers"
    },
    {
        "Description": "open_deals",
        "App": "Sales Tracker",
        "App ID": "bq8xyx9z",
        "Table": "Opportunities",
        "Report": "Open Deals"
    }
]

# Ensure cache is fresh (checks staleness and refreshes if needed)
ensure_cache_freshness(
    client=client,
    report_configs=report_configs,
    cache_mgr=cache_mgr
)

# Load metadata from cache
metadata = load_report_metadata_batch(report_configs, cache_mgr)

# Fetch data for multiple reports in parallel
descriptions = ["active_customers", "open_deals"]
data = get_data_parallel(
    client,
    metadata,
    descriptions,
    cache_mgr,
    cache=True  # Cache the data for later use
)

# Access the data
customers = data["active_customers"]
deals = data["open_deals"]

print(f"Found {len(customers)} active customers")
print(f"Found {len(deals)} open deals")
```

### Single Report Fetch

```python
from quickbase_extract import get_data, load_data

# Fetch a single report
customer_data = get_data(
    client,
    metadata,
    "active_customers",
    cache_mgr,
    cache=True
)

# Later, load from cache without API call
cached_data = load_data(
    metadata,
    "active_customers",
    cache_mgr
)
```

## Report Configuration

### Configuration Structure

Each report configuration is a dictionary with the following keys:

```python
{
    "Description": "unique_identifier",  # Unique key to reference this report
    "App": "App Display Name",           # Quickbase app name (for organization)
    "App ID": "bq8xyx9z",               # Quickbase app ID (required for API calls)
    "Table": "Table Name",               # Table name in Quickbase
    "Report": "Report Name"              # Report name within the table
}
```

### Basic Example

```python
# config/reports.py
"""Quickbase report configurations."""

import os

# Load app IDs from environment variables
SALES_APP_ID = os.environ.get("QB_SALES_APP_ID", "bq8xyx9z")

REPORTS = [
    {
        "Description": "active_customers",
        "App": "Sales Tracker",
        "App ID": SALES_APP_ID,
        "Table": "Customers",
        "Report": "Active Customers"
    },
    {
        "Description": "open_deals",
        "App": "Sales Tracker",
        "App ID": SALES_APP_ID,
        "Table": "Opportunities",
        "Report": "Open Deals"
    },
    {
        "Description": "recent_orders",
        "App": "Sales Tracker",
        "App ID": SALES_APP_ID,
        "Table": "Orders",
        "Report": "Last 30 Days"
    }
]
```

### Pattern 1: Single App, Multiple Tables (DRY Approach)

When all reports come from the same app and use the same report name:

```python
# report-config.py
"""Customer Portal Quickbase report configurations."""

import os

# Load app ID from environment
CUSTOMER_PORTAL_APP_ID = os.environ.get("QB_CUSTOMER_PORTAL_APP_ID", "bq8xyz123")

# Default values for all reports
_DEFAULTS = {
    "App": "Customer Portal",
    "App ID": CUSTOMER_PORTAL_APP_ID,
    "Report": "Python"  # All reports use "Python" report
}

# List of tables to fetch
_TABLES = [
    "Customers",
    "Orders",
    "Products",
    "Invoices",
    "Payments",
    "Shipping Addresses",
    "Support Tickets",
    "Reviews",
    "Promotions",
]

# Generate report configs (Description and Table match)
REPORTS = [
    {"Description": table, "Table": table, **_DEFAULTS}
    for table in _TABLES
]


def get_reports():
    """Return the list of Quickbase report configurations.

    Returns:
        List of dicts containing report description, app, table, and report info.

    Example:
        >>> reports = get_reports()
        >>> print(reports[0])
        {
            'Description': 'Customers',
            'Table': 'Customers',
            'App': 'Customer Portal',
            'App ID': 'bq8xyz123',
            'Report': 'Python'
        }
    """
    return REPORTS
```

### Pattern 2: Multiple Apps - Grouped by App

For better organization when dealing with many apps:

```python
# report-config.py
"""Organized report configurations by application."""

import os

# Load app IDs
SALES_APP_ID = os.environ.get("QB_SALES_APP_ID", "bq8abc123")
HR_APP_ID = os.environ.get("QB_HR_APP_ID", "bq9def456")
INVENTORY_APP_ID = os.environ.get("QB_INVENTORY_APP_ID", "bq7ghi789")

def _create_report(description, app, app_id, table, report="Python"):
    """Helper to create report config dict."""
    return {
        "Description": description,
        "App": app,
        "App ID": app_id,
        "Table": table,
        "Report": report
    }

# Sales reports
SALES_REPORTS = [
    _create_report("customers", "Sales", SALES_APP_ID, "Customers", "Active"),
    _create_report("orders", "Sales", SALES_APP_ID, "Orders", "All Orders"),
    _create_report("invoices", "Sales", SALES_APP_ID, "Invoices", "Unpaid"),
]

# HR reports
HR_REPORTS = [
    _create_report("employees", "HR", HR_APP_ID, "Employees", "Active"),
    _create_report("timesheets", "HR", HR_APP_ID, "Timesheets", "Current Period"),
    _create_report("benefits", "HR", HR_APP_ID, "Benefits", "All"),
]

# Inventory reports
INVENTORY_REPORTS = [
    _create_report("products", "Inventory", INVENTORY_APP_ID, "Products"),
    _create_report("warehouses", "Inventory", INVENTORY_APP_ID, "Warehouses"),
    _create_report("stock_levels", "Inventory", INVENTORY_APP_ID, "Stock Levels"),
]

# Combine all reports
REPORTS = SALES_REPORTS + HR_REPORTS + INVENTORY_REPORTS


def get_reports(app=None):
    """Get report configurations, optionally filtered by app.

    Args:
        app: Optional app name to filter by (e.g., "Sales", "HR")

    Returns:
        List of report configuration dicts.
    """
    if app is None:
        return REPORTS

    return [r for r in REPORTS if r["App"] == app]


def get_report_by_description(description):
    """Get a single report config by description.

    Args:
        description: Report description to find

    Returns:
        Report config dict or None if not found.
    """
    return next((r for r in REPORTS if r["Description"] == description), None)
```

### Using Configurations

```python
# main.py
"""Main application using report configurations."""

from quickbase_extract import (
    get_qb_client,
    refresh_all,
    load_report_metadata_batch,
    get_data_parallel
)
from config.reports import get_reports
import os

# Initialize client
client = get_qb_client(
    realm=os.environ["QB_REALM"],
    user_token=os.environ["QB_USER_TOKEN"]
)

# Get all report configurations
report_configs = get_reports()

# One-time: Refresh metadata (run when reports change)
if os.environ.get("REFRESH_METADATA") == "true":
    refresh_all(client, report_configs)

# Load metadata
metadata = load_report_metadata_batch(report_configs)

# Fetch data for specific reports
descriptions = ["customers", "orders", "invoices"]
data = get_data_parallel(client, metadata, descriptions, cache=True)

# Process data
for desc, records in data.items():
    print(f"{desc}: {len(records)} records")
```

### Environment Configuration

Create a `.env` file for local development:

```bash
# .env
QB_REALM=example.quickbase.com
QB_USER_TOKEN=b5xy8x_abc123_token_here

# App IDs
QB_SALES_APP_ID=bq8abc123
QB_HR_APP_ID=bq9def456
QB_INVENTORY_APP_ID=bq7ghi789

# Optional
REFRESH_METADATA=false
QUICKBASE_CACHE_ROOT=./.quickbase-cache/dev
ENV=dev
```

Load with python-dotenv:

```python
# main.py
from dotenv import load_dotenv
load_dotenv()  # Load .env file

import os
from config.reports import get_reports

# Now environment variables are available
reports = get_reports()
```

### Cache Configuration

The `CacheManager` requires explicit configuration:

```python
from pathlib import Path
from quickbase_extract import CacheManager

# Local development
cache_mgr = CacheManager(
    cache_root=Path("my_project/dev/cache")
)

# Lambda with S3
cache_mgr = CacheManager(
    cache_root=Path("/tmp/my_project/dev/cache"),
    s3_bucket="mit-bio-quickbase",  # Or set CACHE_BUCKET env var
    s3_prefix="my_project/dev/cache"
)
```

**Cache Structure:**

Local and Lambda follow the same structure:
```
my_project/dev/cache/
├── report_metadata/
│   └── app_name/
│       └── table_report.json
└── report_data/
    └── app_name/
        └── table_report_data.json
```

S3 matches the local structure:
```
s3://mit-bio-quickbase/my_project/dev/cache/report_metadata/...
```

### Best Practices

1. **Use descriptive Description keys**
   - Use lowercase with underscores: `"active_customers"` not `"Active Customers"`
   - Make them unique and memorable
   - Consider using them as variable names: `data["active_customers"]`

2. **Store App IDs in environment variables**
   - Never hardcode credentials or IDs in source code
   - Use `.env` for local development
   - Use Lambda environment variables or Secrets Manager for production

3. **Keep configurations in a separate module**
   - Easy to maintain and update
   - Can be imported by multiple scripts
   - Version control friendly

4. **Group related reports**
   - By app for multi-app projects
   - By function (e.g., all billing reports)
   - Makes it easier to run subsets

5. **Document your reports**
   ```python
   REPORTS = [
       {
           "Description": "active_customers",
           "App": "Sales",
           "App ID": SALES_APP_ID,
           "Table": "Customers",
           "Report": "Active",
           # Optional: Add custom metadata
           "notes": "Customers with activity in last 90 days",
           "refresh_frequency": "daily"
       }
   ]
   ```

6. **Validate configurations on startup**
   ```python
   def validate_reports(reports):
       """Validate report configurations."""
       required_keys = ["Description", "App", "App ID", "Table", "Report"]
       descriptions = set()

       for report in reports:
           # Check required keys
           missing = [k for k in required_keys if k not in report]
           if missing:
               raise ValueError(f"Report missing keys {missing}: {report}")

           # Check for duplicate descriptions
           desc = report["Description"]
           if desc in descriptions:
               raise ValueError(f"Duplicate description: {desc}")
           descriptions.add(desc)

       return True

   # Use it
   from config.reports import get_reports
   reports = get_reports()
   validate_reports(reports)
   ```

### Dynamic Configuration (Advanced)

For very large or dynamic report lists:

```python
# config/reports.py
"""Dynamic report configuration from database or API."""

import os
import json
from pathlib import Path

def load_reports_from_file(filepath):
    """Load report configs from JSON file.

    Args:
        filepath: Path to JSON file with report configs

    Returns:
        List of report configuration dicts.
    """
    with open(filepath) as f:
        return json.load(f)


def load_reports_from_database(connection_string):
    """Load report configs from database.

    Args:
        connection_string: Database connection string

    Returns:
        List of report configuration dicts.
    """
    import psycopg2

    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT description, app_name, app_id, table_name, report_name
        FROM quickbase_report_configs
        WHERE active = true
    """)

    reports = []
    for row in cursor.fetchall():
        reports.append({
            "Description": row[0],
            "App": row[1],
            "App ID": row[2],
            "Table": row[3],
            "Report": row[4]
        })

    conn.close()
    return reports


def get_reports():
    """Get reports from configured source.

    Checks for reports in this order:
    1. JSON file (if REPORTS_FILE env var set)
    2. Database (if DATABASE_URL env var set)
    3. Hardcoded defaults
    """
    # Try JSON file
    reports_file = os.environ.get("REPORTS_FILE")
    if reports_file and Path(reports_file).exists():
        return load_reports_from_file(reports_file)

    # Try database
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        return load_reports_from_database(db_url)

    # Fallback to defaults
    return [
        # ... hardcoded reports ...
    ]
```

## Architecture

### How It Works

```
┌─────────────────┐
│  Report Config  │
└────────┬────────┘
         │
         ▼
┌─────────────────────┐      ┌──────────────┐
│ Fetch Metadata      │─────▶│ Cache (JSON) │
│ - Table ID          │      │ Local or S3  │
│ - Field mappings    │      └──────────────┘
│ - Report filters    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐      ┌──────────────┐
│ Fetch Data          │─────▶│ Cache (JSON) │
│ - Query Quickbase   │      │ Local or S3  │
│ - Transform records │      └──────────────┘
│ - Apply labels      │
└─────────────────────┘
```

### Cache Structure

```
.quickbase-cache/
├── report_metadata/
│   └── sales_tracker/
│       ├── customers_active_customers.json
│       └── opportunities_open_deals.json
└── report_data/
    └── sales_tracker/
        ├── customers_active_customers_data.json
        └── opportunities_open_deals_data.json
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `QUICKBASE_CACHE_ROOT` | Local cache directory | `.quickbase-cache/dev` |
| `ENV` | Environment name (dev/prod) | `dev` |
| `AWS_LAMBDA_FUNCTION_NAME` | Set by Lambda (auto-detected) | - |
| `CACHE_BUCKET` | S3 bucket for Lambda cache | - |
| `METADATA_STALE_HOURS` | Threshold (hours) for metadata cache staleness | `168` (7 days) |
| `DATA_STALE_HOURS` | Threshold (hours) for data cache staleness | `24` (1 day) |
| `FORCE_CACHE_REFRESH` | If set to "true", forces cache refresh on next sync | - |

### Custom Cache Location

```python
from quickbase_extract import CacheManager

# Use custom cache directory
cache_mgr = CacheManager(cache_root="/path/to/cache")

# Or set via environment
import os
os.environ["QUICKBASE_CACHE_ROOT"] = "/path/to/cache"
```

## AWS Lambda Deployment

### Setup

1. **Set environment variables in Lambda:**
   ```
   CACHE_BUCKET=my-quickbase-cache-bucket
   ENV=prod
   ```
2. **Lambda handler example:**

```python
import quickbase_api
from pathlib import Path
from quickbase_extract import (
    CacheManager,
    sync_from_s3_once,
    load_report_metadata_batch,
    get_data_parallel
)
from quickbase_extract.cache_orchestration import ensure_cache_freshness
import os

# Initialize client (reuse across warm starts)
client = quickbase_api.client(
    realm=os.environ["QB_REALM"],
    user_token=os.environ["QB_USER_TOKEN"]
)

# Initialize cache manager
cache_mgr = CacheManager(
    cache_root=Path("/tmp/my_project/dev/cache"),
    s3_bucket=os.environ["CACHE_BUCKET"],
    s3_prefix="my_project/dev/cache"
)

# Load configs
report_configs = [...]  # Your configs

def lambda_handler(event, context):
    # Sync cache from S3 on cold start
    sync_from_s3_once(cache_mgr)

    # Ensure cache is fresh (auto-refresh if stale)
    ensure_cache_freshness(
        client=client,
        report_configs=report_configs,
        cache_mgr=cache_mgr
    )

    # Load metadata from cache
    metadata = load_report_metadata_batch(report_configs, cache_mgr)

    # Fetch fresh data
    descriptions = event.get("reports", ["active_customers"])
    data = get_data_parallel(
        client,
        metadata,
        descriptions,
        cache_mgr,
        cache=True  # Will sync to S3
    )

    return {
        "statusCode": 200,
        "body": f"Fetched {len(data)} reports"
    }
```

### S3 Bucket Structure

```
my-quickbase-cache-bucket/
├── dev/
│   ├── report_metadata/...
│   └── report_data/...
└── prod/
    ├── report_metadata/...
    └── report_data/...
```

## API Reference

### Client Management

#### Creating a Quickbase Client

Create your Quickbase client using the `quickbase-api` package directly:

```python
import quickbase_api

client = quickbase_api.client(
    realm="example.quickbase.com",
    user_token="YOUR_TOKEN"
)
```

### Metadata Operations

#### `load_report_metadata_batch(report_configs, cache_mgr)`

Load metadata for all reports from cache.

**Returns:** Dict mapping report descriptions to metadata

```python
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
metadata = load_report_metadata_batch(report_configs, cache_mgr)
# Returns: {"active_customers": {...}, "open_deals": {...}}
```

#### `ensure_cache_freshness(client, report_configs, cache_mgr, metadata_stale_hours=None, data_stale_hours=None, force=False)`

Ensure cache is fresh; refresh metadata and/or data if empty or stale.

Checks metadata and data caches independently. Refreshes only the caches that are stale, avoiding unnecessary API calls.

**Parameters:**
- `client`: Quickbase API client
- `report_configs` (list[dict]): List of report configurations
- `cache_mgr` (CacheManager): Cache manager instance
- `metadata_stale_hours` (float, optional): Threshold for metadata staleness (default: 168 hours / 7 days)
- `data_stale_hours` (float, optional): Threshold for data staleness (default: 24 hours)
- `force` (bool): Force refresh regardless of cache state (default: False)

**Environment Variables:**
- `METADATA_STALE_HOURS`: Override default metadata staleness threshold
- `DATA_STALE_HOURS`: Override default data staleness threshold
- `FORCE_CACHE_REFRESH`: Set to "true" to force refresh

```python
from quickbase_extract.cache_orchestration import ensure_cache_freshness

ensure_cache_freshness(
    client=client,
    report_configs=report_configs,
    cache_mgr=cache_mgr,
    metadata_stale_hours=720,  # 30 days
    data_stale_hours=24  # 1 day
)
```

### Data Operations

#### `get_data(client, report_metadata, report_desc, cache_mgr, cache=False)`

Fetch data for a single report.

**Parameters:**
- `client`: Quickbase API client
- `report_metadata` (dict): Metadata from `load_report_metadata_batch()`
- `report_desc` (str): Report description key
- `cache_mgr` (CacheManager): Cache manager instance
- `cache` (bool): Whether to cache the data (default: False)

**Returns:** List of record dicts with field labels as keys

```python
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
customers = get_data(client, metadata, "active_customers", cache_mgr, cache=True)
```

#### `get_data_parallel(client, report_metadata, report_descriptions, cache_mgr, cache=False, max_workers=8)`

Fetch data for multiple reports in parallel.

**Parameters:**
- `client`: Quickbase API client
- `report_metadata` (dict): Metadata from `load_report_metadata_batch()`
- `report_descriptions` (list[str]): List of report description keys
- `cache_mgr` (CacheManager): Cache manager instance
- `cache` (bool): Whether to cache the data (default: False)
- `max_workers` (int): Maximum concurrent threads (default: 8)

**Returns:** Dict mapping report descriptions to data lists

```python
data = get_data_parallel(
    client,
    metadata,
    ["active_customers", "open_deals"],
    cache_mgr,
    cache=True,
    max_workers=4
)
```

#### `load_data(report_metadata, report_desc, cache_mgr)`

Load cached data for a single report.

```python
customers = load_data(metadata, "active_customers", cache_mgr)
```

#### `load_data_batch(report_metadata, report_descriptions, cache_mgr)`

Load cached data for multiple reports.

```python
data = load_data_batch(metadata, ["active_customers", "open_deals"], cache_mgr)
```

### Cache Management

#### `CacheManager(cache_root, s3_bucket=None, s3_prefix=None)`

Manages cache reads/writes for both local and Lambda environments.

**Parameters:**
- `cache_root` (Path): Path to cache root directory (required)
- `s3_bucket` (str, optional): S3 bucket name for Lambda. Reads from `CACHE_BUCKET` env var if not provided
- `s3_prefix` (str, optional): Path prefix within S3 bucket (required if using S3 on Lambda)

```python
# Local
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))

# Lambda
cache_mgr = CacheManager(
    cache_root=Path("/tmp/my_project/dev/cache"),
    s3_bucket="mit-bio-quickbase",
    s3_prefix="my_project/dev/cache"
)
```

#### `sync_from_s3_once(cache_mgr, force=False)`

Download cache from S3 to /tmp on Lambda cold start.

**Parameters:**
- `cache_mgr` (CacheManager): Cache manager instance
- `force` (bool): Force sync even if already synced (default: False)

```python
sync_from_s3_once(cache_mgr)  # On cold start
sync_from_s3_once(cache_mgr, force=True)  # Force re-sync
```

#### `is_cache_synced()`

Check if cache has been synced in this invocation.

```python
if not is_cache_synced():
    print("Cache needs syncing")
```

### Error Handling

#### `handle_query(client, table_id, *, select=None, where=None, sort_by=None, group_by=None, options=None, description="", max_retries=3)`

Execute a Quickbase query with retry logic.

```python
from quickbase_extract import handle_query

result = handle_query(
    client,
    "tblABC123",
    select=[3, 6, 7],
    where="{8.EX.'Active'}",
    description="active customers",
    max_retries=3
)
```

#### `handle_upsert(client, table_id, data, description="", max_retries=3)`

Execute a Quickbase upsert with retry logic.

```python
from quickbase_extract import handle_upsert

records = [{"6": {"value": "Alice"}, "7": {"value": "alice@example.com"}}]
result = handle_upsert(
    client,
    "tblABC123",
    records,
    description="customer records"
)
```

#### `handle_delete(client, table_id, where, description="", max_retries=3)`

Execute a Quickbase delete with retry logic.

```python
from quickbase_extract import handle_delete

deleted = handle_delete(
    client,
    "tblABC123",
    where="{8.EX.'Inactive'}",
    description="inactive customers"
)
print(f"Deleted {deleted} records")
```

## Development

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/quickbase-extract.git
cd quickbase-extract

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with test dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=quickbase_extract --cov-report=html

# Run specific test file
pytest tests/test_report_data.py

# Run specific test
pytest tests/test_report_data.py::TestGetData::test_get_data_with_cache
```

### Code Quality

```bash
# Format code
black quickbase_extract tests

# Lint
flake8 quickbase_extract tests

# Type check
mypy quickbase_extract
```

## Cache Freshness Thresholds

Different cache types have different recommended freshness thresholds:

| Cache Type | Default | Recommended | Reason |
|------------|---------|-------------|--------|
| Metadata | 168 hours (7 days) | Manual refresh | Table structure rarely changes |
| Data | 24 hours (1 day) | Varies by use case | Data changes frequently |

```python
from quickbase_extract.cache_freshness import (
    DEFAULT_METADATA_STALE_HOURS,
    DEFAULT_DATA_STALE_HOURS
)

# Check metadata freshness (rarely changes)
stale_metadata = check_cache_freshness(
    threshold_hours=DEFAULT_METADATA_STALE_HOURS
)

# Check data freshness (changes often)
stale_data = check_cache_freshness(
    threshold_hours=DEFAULT_DATA_STALE_HOURS
)
```

## Best Practices

### 1. Metadata Refresh Strategy

Metadata (table structure, field mappings) changes infrequently. Only refresh when:
- Adding new reports
- Report configurations change
- Field definitions change

```python
# Manual metadata refresh (not in production loop)
if metadata_changed:
    refresh_all(client, report_configs)
```

### 2. Data Caching Strategy

For Lambda, cache data during the function execution to avoid repeated API calls:

```python
# Good: Fetch once, cache, reuse
metadata = load_report_metadata_batch(report_configs)
data = get_data_parallel(client, metadata, descriptions, cache=True)

# Later in same invocation
cached_data = load_data_batch(metadata, descriptions)
```

### 3. Rate Limit Management

Adjust `max_workers` based on your Quickbase API rate limits:

```python
# Conservative (better for rate limits)
data = get_data_parallel(client, metadata, descriptions, max_workers=4)

# Aggressive (faster but may hit rate limits)
data = get_data_parallel(client, metadata, descriptions, max_workers=16)
```

### 4. Error Handling

All operations include retry logic for rate limits (429 errors) but fail fast on other errors:

```python
from quickbase_extract import QuickbaseOperationError

try:
    data = get_data(client, metadata, "report_name")
except QuickbaseOperationError as e:
    print(f"Operation {e.operation} failed: {e.details}")
except KeyError:
    print("Report not found in metadata")
```

## Troubleshooting

### Issue: "Report metadata not found"

**Solution:** Run `refresh_all()` to cache metadata first:

```python
refresh_all(client, report_configs)
metadata = load_report_metadata_batch(report_configs)
```

### Issue: "Rate limit exceeded" (429 errors)

**Solution:** Reduce `max_workers` or increase retry delays:

```python
# Reduce concurrency
data = get_data_parallel(client, metadata, descriptions, max_workers=2)

# Increase max retries
from quickbase_extract import handle_query
result = handle_query(client, table_id, max_retries=5)
```

### Issue: Lambda "Cache not synced from S3"

**Solution:** Ensure `CACHE_BUCKET` is set and bucket exists:

```python
import os
print(os.environ.get("CACHE_BUCKET"))  # Should not be None

# Call sync explicitly
from quickbase_extract import sync_from_s3_once
sync_from_s3_once()
```

### Issue: "Cache directory does not exist"

**Solution:** The cache directory is created automatically, but ensure parent directory is writable:

```python
from quickbase_extract import CacheManager
import os

cache_path = os.path.expanduser("~/.quickbase-cache")
cache_mgr = CacheManager(cache_root=cache_path)
```

### Issue: "cache_root is required"

**Solution:** `CacheManager` now requires explicit `cache_root` parameter:

```python
# Old (no longer works)
from quickbase_extract import get_cache_manager
cache_mgr = get_cache_manager()  # ❌ Removed

# New (required)
from quickbase_extract import CacheManager
from pathlib import Path
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))  # ✅
```

## Cache Freshness Management

### Automatic Cache Refresh

Use `ensure_cache_freshness()` to automatically check and refresh cache if stale:

```python
from quickbase_extract.cache_orchestration import ensure_cache_freshness

def lambda_handler(event, context):
    # Sync from S3 on cold start
    sync_from_s3_once(cache_mgr)

    # Ensure cache is fresh (auto-refresh if needed)
    ensure_cache_freshness(
        client=client,
        report_configs=report_configs,
        cache_mgr=cache_mgr,
        metadata_stale_hours=720,  # 30 days
        data_stale_hours=24         # 1 day
    )

    # Load metadata and proceed
    metadata = load_report_metadata_batch(report_configs, cache_mgr)
    data = get_data_parallel(client, metadata, descriptions, cache_mgr, cache=True)

    return {"statusCode": 200, "body": "Success"}
```

### Independent Refresh

Metadata and data are refreshed independently based on their staleness:

- **Metadata only stale**: Refreshes only metadata (table structure changes rarely)
- **Data only stale**: Refreshes only data (data changes frequently)
- **Both stale**: Refreshes both

This minimizes unnecessary API calls.

### Force Refresh

Force a cache refresh either programmatically or via environment variable:

```python
# Programmatic force
ensure_cache_freshness(
    client=client,
    report_configs=report_configs,
    cache_mgr=cache_mgr,
    force=True  # Always refresh, skip age checks
)

# Via environment variable (set in Lambda before invocation)
# FORCE_CACHE_REFRESH=true
# Then call normally:
ensure_cache_freshness(
    client=client,
    report_configs=report_configs,
    cache_mgr=cache_mgr
)
```

## Advanced Usage

### Custom Report Configurations

#### Using Field IDs Instead of Report Names

```python
from quickbase_extract import handle_query

# Query specific fields directly without a report
result = handle_query(
    client,
    table_id="tblABC123",
    select=[3, 6, 7, 8],  # Field IDs
    where="{8.EX.'Active'}AND{12.GT.'2024-01-01'}",
    sort_by=[{"fieldId": 6, "order": "ASC"}],
    description="custom query"
)

data = result["data"]
```

#### Dynamic Report Filtering

```python
def get_filtered_customers(client, metadata, status, min_date):
    """Fetch customers with dynamic filters."""
    info = metadata["active_customers"]

    # Build custom filter
    custom_filter = f"{{8.EX.'{status}'}}AND{{12.GT.'{min_date}'}}"

    result = handle_query(
        client,
        info["table_id"],
        select=info["fields"],
        where=custom_filter,
        sort_by=info["report"]["query"]["sortBy"],
        description=f"customers_{status}_{min_date}"
    )

    return result["data"]

# Usage
active = get_filtered_customers(client, metadata, "Active", "2024-01-01")
```

### Batch Processing with Progress Tracking

```python
from quickbase_extract import get_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_reports_with_progress(client, metadata, descriptions):
    """Process multiple reports with progress tracking."""
    results = {}
    total = len(descriptions)

    for i, desc in enumerate(descriptions, 1):
        logger.info(f"Processing {i}/{total}: {desc}")

        try:
            data = get_data(client, metadata, desc, cache=True)
            results[desc] = {
                "status": "success",
                "records": len(data),
                "data": data
            }
            logger.info(f"✓ {desc}: {len(data)} records")
        except Exception as e:
            results[desc] = {
                "status": "error",
                "error": str(e)
            }
            logger.error(f"✗ {desc}: {e}")

    return results

# Usage
results = process_reports_with_progress(
    client,
    metadata,
    ["customers", "orders", "products"]
)
```

### Incremental Data Updates

```python
from datetime import datetime, timedelta
from quickbase_extract import handle_query, handle_upsert

def sync_recent_changes(client, source_table_id, target_table_id):
    """Sync only records modified in last 24 hours."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Fetch recent changes
    result = handle_query(
        client,
        source_table_id,
        where=f"{{1.AFT.'{yesterday}'}}",  # Date Modified after yesterday
        description="recent changes"
    )

    if result["data"]:
        # Transform and upsert
        records = [transform_record(r) for r in result["data"]]
        handle_upsert(
            client,
            target_table_id,
            records,
            description="sync recent changes"
        )
        print(f"Synced {len(records)} recent changes")
    else:
        print("No recent changes found")
```

### Multi-Environment Configuration

```python
import os
from quickbase_extract import get_qb_client, CacheManager

class QuickbaseConfig:
    """Environment-aware Quickbase configuration."""

    def __init__(self, env=None):
        self.env = env or os.environ.get("ENV", "dev")
        self.config = self._load_config()

    def _load_config(self):
        configs = {
            "dev": {
                "realm": "dev-realm.quickbase.com",
                "token": os.environ.get("QB_TOKEN_DEV"),
                "cache_root": "./.quickbase-cache/dev"
            },
            "staging": {
                "realm": "staging-realm.quickbase.com",
                "token": os.environ.get("QB_TOKEN_STAGING"),
                "cache_root": "./.quickbase-cache/staging"
            },
            "prod": {
                "realm": "prod-realm.quickbase.com",
                "token": os.environ.get("QB_TOKEN_PROD"),
                "cache_root": "/tmp/quickbase-cache"
            }
        }
        return configs[self.env]

    def get_client(self):
        return get_qb_client(
            realm=self.config["realm"],
            user_token=self.config["token"]
        )

    def get_cache_manager(self):
        return CacheManager(cache_root=self.config["cache_root"])

# Usage
config = QuickbaseConfig(env="prod")
client = config.get_client()
cache_mgr = config.get_cache_manager()
```

### Data Transformation Pipeline

```python
from quickbase_extract import get_data
from typing import List, Dict, Callable

class DataPipeline:
    """Pipeline for transforming Quickbase data."""

    def __init__(self, client, metadata):
        self.client = client
        self.metadata = metadata
        self.transformers: List[Callable] = []

    def add_transformer(self, func: Callable):
        """Add a transformation function to the pipeline."""
        self.transformers.append(func)
        return self

    def execute(self, report_desc: str) -> List[Dict]:
        """Execute pipeline for a report."""
        # Fetch data
        data = get_data(self.client, self.metadata, report_desc)

        # Apply transformations
        for transformer in self.transformers:
            data = transformer(data)

        return data

# Example transformers
def filter_active(data):
    """Keep only active records."""
    return [r for r in data if r.get("Status") == "Active"]

def add_full_name(data):
    """Add computed full name field."""
    for record in data:
        first = record.get("First Name", "")
        last = record.get("Last Name", "")
        record["Full Name"] = f"{first} {last}".strip()
    return data

def convert_dates(data):
    """Convert date strings to datetime objects."""
    from dateutil import parser
    for record in data:
        if "Date Created" in record:
            record["Date Created"] = parser.parse(record["Date Created"])
    return data

# Usage
pipeline = (
    DataPipeline(client, metadata)
    .add_transformer(filter_active)
    .add_transformer(add_full_name)
    .add_transformer(convert_dates)
)

customers = pipeline.execute("active_customers")
```

## Performance Optimization

### Benchmarking Your Setup

```python
import time
from quickbase_extract import get_data_parallel

def benchmark_parallel_fetch(client, metadata, descriptions, workers_list):
    """Test different worker counts to find optimal setting."""
    results = {}

    for workers in workers_list:
        start = time.time()
        data = get_data_parallel(
            client,
            metadata,
            descriptions,
            max_workers=workers
        )
        elapsed = time.time() - start

        total_records = sum(len(d) for d in data.values())
        results[workers] = {
            "time": elapsed,
            "records": total_records,
            "records_per_second": total_records / elapsed
        }

        print(f"Workers={workers}: {elapsed:.2f}s, {total_records} records")

    return results

# Usage
descriptions = ["customers", "orders", "products", "invoices"]
results = benchmark_parallel_fetch(
    client,
    metadata,
    descriptions,
    workers_list=[2, 4, 8, 16]
)

# Find optimal worker count
optimal = max(results.items(), key=lambda x: x[1]["records_per_second"])
print(f"Optimal workers: {optimal[0]} ({optimal[1]['records_per_second']:.0f} rec/sec)")
```

### Memory-Efficient Processing for Large Datasets

```python
from quickbase_extract import handle_query
import json

def fetch_large_dataset_in_chunks(client, table_id, chunk_size=1000):
    """Fetch large datasets in chunks to avoid memory issues."""
    skip = 0
    all_data = []

    while True:
        result = handle_query(
            client,
            table_id,
            options={"skip": skip, "top": chunk_size},
            description=f"chunk at {skip}"
        )

        data = result["data"]
        if not data:
            break

        all_data.extend(data)
        print(f"Fetched {len(all_data)} records so far...")

        if len(data) < chunk_size:
            break  # Last chunk

        skip += chunk_size

    return all_data

# Stream to file instead of loading in memory
def stream_to_file(client, table_id, output_file):
    """Stream large dataset directly to file."""
    skip = 0
    chunk_size = 1000

    with open(output_file, 'w') as f:
        f.write('[')
        first_chunk = True

        while True:
            result = handle_query(
                client,
                table_id,
                options={"skip": skip, "top": chunk_size}
            )

            data = result["data"]
            if not data:
                break

            # Write chunk
            if not first_chunk:
                f.write(',')
            f.write(json.dumps(data)[1:-1])  # Remove outer brackets
            first_chunk = False

            if len(data) < chunk_size:
                break

            skip += chunk_size

        f.write(']')
```

### Caching Strategies

```python
from quickbase_extract import load_data, get_data
from datetime import datetime, timedelta
import os

def get_data_with_ttl(client, metadata, report_desc, ttl_hours=24):
    """Get data from cache if fresh, otherwise fetch new."""
    from quickbase_extract.cache_manager import get_cache_manager

    cache_mgr = get_cache_manager()
    info = metadata[report_desc]
    data_path = cache_mgr.get_data_path(
        info["app_name"],
        info["table_name"],
        info["report_name"]
    )

    # Check if cache exists and is fresh
    if data_path.exists():
        mtime = datetime.fromtimestamp(data_path.stat().st_mtime)
        age = datetime.now() - mtime

        if age < timedelta(hours=ttl_hours):
            print(f"Using cached data ({age.seconds / 3600:.1f}h old)")
            return load_data(metadata, report_desc)

    # Cache miss or stale, fetch new
    print("Fetching fresh data from Quickbase")
    return get_data(client, metadata, report_desc, cache=True)

# Usage
customers = get_data_with_ttl(client, metadata, "customers", ttl_hours=6)
```

## Real-World Use Cases

### Use Case 1: Daily Sales Report

```python
from quickbase_extract import *
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def generate_daily_sales_report(client, metadata):
    """Generate and email daily sales report."""
    # Fetch today's data
    today = datetime.now().strftime("%Y-%m-%d")

    orders = get_data(client, metadata, "todays_orders")
    revenue = sum(float(o.get("Order Total", 0)) for o in orders)

    # Generate report
    report = f"""
    Daily Sales Report - {today}
    ================================
    Total Orders: {len(orders)}
    Total Revenue: ${revenue:,.2f}
    Average Order: ${revenue / len(orders) if orders else 0:,.2f}

    Top 5 Orders:
    """

    top_orders = sorted(orders, key=lambda x: float(x.get("Order Total", 0)), reverse=True)[:5]
    for i, order in enumerate(top_orders, 1):
        report += f"\n{i}. Order #{order['Order ID']}: ${order['Order Total']}"

    # Email report
    send_email_report(report, "sales-team@company.com")

    return {"orders": len(orders), "revenue": revenue}

def send_email_report(body, to_email):
    """Send email report."""
    msg = MIMEText(body)
    msg['Subject'] = f"Daily Sales Report - {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = "quickbase-bot@company.com"
    msg['To'] = to_email

    # Send email (configure SMTP settings)
    # ...
```

### Use Case 2: Data Warehouse ETL

```python
from quickbase_extract import *
import psycopg2
from datetime import datetime

class QuickbaseToPostgresETL:
    """ETL pipeline from Quickbase to PostgreSQL."""

    def __init__(self, qb_client, pg_conn_string):
        self.qb_client = qb_client
        self.pg_conn = psycopg2.connect(pg_conn_string)

    def extract(self, metadata, report_descs):
        """Extract data from Quickbase."""
        return get_data_parallel(
            self.qb_client,
            metadata,
            report_descs,
            cache=False  # Always fresh for ETL
        )

    def transform(self, data):
        """Transform data for warehouse schema."""
        transformed = {}

        for report_desc, records in data.items():
            # Clean and transform each record
            cleaned = []
            for record in records:
                cleaned_record = {
                    # Normalize field names
                    k.lower().replace(" ", "_"): v
                    for k, v in record.items()
                }
                # Add metadata
                cleaned_record["_extracted_at"] = datetime.now().isoformat()
                cleaned_record["_source"] = report_desc
                cleaned.append(cleaned_record)

            transformed[report_desc] = cleaned

        return transformed

    def load(self, data, schema="quickbase"):
        """Load data into PostgreSQL."""
        cursor = self.pg_conn.cursor()

        for table_name, records in data.items():
            if not records:
                continue

            # Create/truncate table
            columns = list(records[0].keys())
            col_defs = ", ".join([f'"{col}" TEXT' for col in columns])

            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
            cursor.execute(f'DROP TABLE IF EXISTS {schema}.{table_name}')
            cursor.execute(f'CREATE TABLE {schema}.{table_name} ({col_defs})')

            # Bulk insert
            for record in records:
                placeholders = ", ".join(["%s" for _ in columns])
                values = [record.get(col) for col in columns]
                cursor.execute(
                    f'INSERT INTO {schema}.{table_name} VALUES ({placeholders})',
                    values
                )

            self.pg_conn.commit()
            print(f"Loaded {len(records)} records into {schema}.{table_name}")

        cursor.close()

    def run(self, metadata, report_descs):
        """Run full ETL pipeline."""
        print("Starting ETL pipeline...")

        # Extract
        print("Extracting...")
        data = self.extract(metadata, report_descs)

        # Transform
        print("Transforming...")
        transformed = self.transform(data)

        # Load
        print("Loading...")
        self.load(transformed)

        print("ETL complete!")

        return {
            "tables_loaded": len(transformed),
            "total_records": sum(len(records) for records in transformed.values())
        }

# Usage
etl = QuickbaseToPostgresETL(
    qb_client=client,
    pg_conn_string="postgresql://user:pass@localhost/warehouse"
)

result = etl.run(
    metadata,
    ["customers", "orders", "products"]
)
```

### Use Case 3: Automated Data Quality Checks

```python
from quickbase_extract import get_data
from typing import List, Dict, Any

class DataQualityChecker:
    """Run data quality checks on Quickbase data."""

    def __init__(self, client, metadata):
        self.client = client
        self.metadata = metadata
        self.issues = []

    def check_required_fields(self, report_desc, required_fields):
        """Check that required fields are not empty."""
        data = get_data(self.client, self.metadata, report_desc)

        for i, record in enumerate(data):
            for field in required_fields:
                if not record.get(field):
                    self.issues.append({
                        "report": report_desc,
                        "record_index": i,
                        "record_id": record.get("Record ID#"),
                        "issue": f"Missing required field: {field}"
                    })

    def check_duplicates(self, report_desc, unique_field):
        """Check for duplicate values in unique fields."""
        data = get_data(self.client, self.metadata, report_desc)

        seen = {}
        for i, record in enumerate(data):
            value = record.get(unique_field)
            if value in seen:
                self.issues.append({
                    "report": report_desc,
                    "record_index": i,
                    "record_id": record.get("Record ID#"),
                    "issue": f"Duplicate {unique_field}: {value} (also at index {seen[value]})"
                })
            else:
                seen[value] = i

    def check_value_range(self, report_desc, field, min_val=None, max_val=None):
        """Check that numeric values are within expected range."""
        data = get_data(self.client, self.metadata, report_desc)

        for i, record in enumerate(data):
            value = record.get(field)
            if value is not None:
                try:
                    num_value = float(value)
                    if min_val is not None and num_value < min_val:
                        self.issues.append({
                            "report": report_desc,
                            "record_index": i,
                            "record_id": record.get("Record ID#"),
                            "issue": f"{field} below minimum: {value} < {min_val}"
                        })
                    if max_val is not None and num_value > max_val:
                        self.issues.append({
                            "report": report_desc,
                            "record_index": i,
                            "record_id": record.get("Record ID#"),
                            "issue": f"{field} above maximum: {value} > {max_val}"
                        })
                except ValueError:
                    self.issues.append({
                        "report": report_desc,
                        "record_index": i,
                        "record_id": record.get("Record ID#"),
                        "issue": f"{field} is not numeric: {value}"
                    })

    def generate_report(self):
        """Generate data quality report."""
        if not self.issues:
            return "✓ All data quality checks passed!"

        report = f"Found {len(self.issues)} data quality issues:\n\n"
        for issue in self.issues:
            report += f"- Record {issue['record_id']} in {issue['report']}: {issue['issue']}\n"

        return report

# Usage
checker = DataQualityChecker(client, metadata)

# Run checks
checker.check_required_fields("customers", ["Name", "Email"])
checker.check_duplicates("customers", "Email")
checker.check_value_range("orders", "Order Total", min_val=0, max_val=1000000)

# Get report
print(checker.generate_report())
```

## FAQ

### Q: How do I handle Quickbase API rate limits?

**A:** The package automatically retries on 429 (rate limit) errors with exponential backoff. You can also:
- Reduce `max_workers` in parallel operations
- Increase `max_retries` for more attempts
- Cache data aggressively to reduce API calls

```python
# Conservative approach
data = get_data_parallel(client, metadata, descriptions, max_workers=2)

# More retries
result = handle_query(client, table_id, max_retries=5)
```

### Q: What's the difference between cache=True and loading from cache?

**A:**
- `get_data(..., cache=True)` - Fetches from API and saves to cache
- `load_data(...)` - Loads from cache only (no API call)

```python
# Scenario 1: Fresh data needed
data = get_data(client, metadata, "customers", cache=True)  # API call + cache
# Later in same execution
data = load_data(metadata, "customers")  # From cache, no API call

# Scenario 2: Cache-first approach
try:
    data = load_data(metadata, "customers")  # Try cache first
except FileNotFoundError:
    data = get_data(client, metadata, "customers", cache=True)  # Fallback to API
```
