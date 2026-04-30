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

- Python 3.12+
- `quickbase-api` - Quickbase API client
- `boto3` - AWS SDK (for Lambda/S3 support)

## Quick Start

### Basic Usage

```python
import quickbase_api
from pathlib import Path
from quickbase_extract.config import ReportConfig
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

# Define report configurations as ReportConfig instances
report_config_all = [
    ReportConfig(
        app_id="bq8xyx9z",
        app_name="sales_tracker",
        table_name="Customers",
        report_name="Active Customers"
    ),
    ReportConfig(
        app_id="bq8xyx9z",
        app_name="sales_tracker",
        table_name="Opportunities",
        report_name="Open Deals"
    )
]

# Optional: Define a subset for data caching
report_configs_to_cache = [report_configs_all[0], report_configs_all[1]]

# Ensure cache is fresh (checks staleness and refreshes if needed)
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=report_configs_all,
    report_configs_to_cache=report_configs_to_cache
)

# Load metadata from cache
metadata = load_report_metadata_batch(cache_mgr, report_configs_all)

# Fetch data for multiple reports in parallel
data = get_data_parallel(
    client,
    cache_mgr,
    report_configs_all,
    metadata,
    cache=True  # Cache the data for later use
)

# Access the data
customers = data[report_configs_all[0]]
deals = data[report_configs_all[1]]

print(f"Found {len(customers)} active customers")
print(f"Found {len(deals)} open deals")
```

### Single Report Fetch

```python
from quickbase_extract import get_data, load_data

# Fetch a single report
customer_data = get_data(
    client,
    cache_mgr,
    report_configs_all[0],
    metadata,
    cache=True
)

# Later, load from cache without API call
cached_data = load_data(cache_mgr, report_configs_all[0], metadata)
```

## Report Configuration

### ReportConfig Structure

`ReportConfig` is a NamedTuple that identifies a Quickbase report. It contains four required fields:

```python
from quickbase_extract.config import ReportConfig

config = ReportConfig(
    app_id="bq8xyx9z",           # Quickbase app ID (required for API calls)
    app_name="sales_tracker",     # Normalized app name (for cache paths and logging)
    table_name="Customers",       # Table name in Quickbase
    report_name="Active Customers" # Report name within the table
)
```

#### Why Each Field Matters

| Field | Purpose | Required? | Example |
|-------|---------|-----------|---------|
| `app_id` | **Technical identifier** for API calls. Uniquely identifies the Quickbase app. Must match exactly what Quickbase expects. | ✅ Yes | `"bq8xyx9z"` |
| `app_name` | **Human-readable identifier** for cache paths and logging. Use lowercase with underscores. Makes it easy to understand which app a cache file belongs to. | ✅ Yes | `"sales_tracker"` |
| `table_name` | **Table name in Quickbase**. Used to fetch table ID and field mappings. Used in cache paths. | ✅ Yes | `"Customers"` |
| `report_name` | **Report name within the table**. Used to find the specific report and its configuration (filters, fields, sorting). Used in cache paths. | ✅ Yes | `"Active Customers"` |

#### Why Normalize app_name?

The `app_name` field is normalized (lowercase, spaces to underscores) for consistent cache paths:

```
cache/
├── report_metadata/
│   └── sales_tracker/           # app_name (normalized)
│       ├── customers_python.json
│       └── opportunities_python.json
└── report_data/
    └── sales_tracker/           # Same normalized form
        ├── customers_python_data.json
        └── opportunities_python_data.json
```

If you used mixed-case like `"Sales Tracker"`, cache paths would be inconsistent across systems.

### Configuration Storage Strategy: Option 2 (Recommended for Scale)

For projects with many reports (especially 100+), use a **dict with readable keys** to organize configs. This keeps your code DRY and maintainable.

#### Basic Example

```python
# config/reports.py
"""Quickbase report configurations organized by app."""

import os
from quickbase_extract.config import ReportConfig

# Load app IDs from environment
SALES_APP_ID = os.environ.get("QB_SALES_APP_ID", "bq8xyx9z")
HR_APP_ID = os.environ.get("QB_HR_APP_ID", "bq9yza0a")

# Define all reports in a dict with readable keys
_REPORTS = {
    # Sales app reports
    "customers_active": ReportConfig(
        app_id=SALES_APP_ID,
        app_name="sales_tracker",
        table_name="Customers",
        report_name="Active Customers"
    ),
    "opportunities_open": ReportConfig(
        app_id=SALES_APP_ID,
        app_name="sales_tracker",
        table_name="Opportunities",
        report_name="Open Deals"
    ),
    "invoices_unpaid": ReportConfig(
        app_id=SALES_APP_ID,
        app_name="sales_tracker",
        table_name="Invoices",
        report_name="Unpaid"
    ),
    # HR app reports
    "employees_active": ReportConfig(
        app_id=HR_APP_ID,
        app_name="hr_system",
        table_name="Employees",
        report_name="Active"
    ),
    "timesheets_current": ReportConfig(
        app_id=HR_APP_ID,
        app_name="hr_system",
        table_name="Timesheets",
        report_name="Current Period"
    ),
}

# Return all reports for metadata refresh
def get_all_reports() -> list[ReportConfig]:
    """Return all report configurations for metadata refresh.

    Use this to refresh metadata for all reports.
    """
    return list(_REPORTS.values())

# Return specific subset for data caching
def get_reports_to_cache() -> list[ReportConfig]:
    """Return reports to cache data for.

    Caching data is expensive (many API calls), so only cache
    the reports you actually need. Returns a subset of all reports.
    """
    return [
        _REPORTS["customers_active"],
        _REPORTS["opportunities_open"],
        _REPORTS["employees_active"],
    ]

# Access individual reports by name
def get_report(name: str) -> ReportConfig:
    """Get a specific report by name.

    Args:
        name: Report key (e.g., "customers_active")

    Returns:
        ReportConfig instance

    Raises:
        KeyError: If report name not found
    """
    return _REPORTS[name]
```

**Why this structure works:**
- ✅ **Readable keys** — `_REPORTS["customers_active"]` is clear what it refers to
- ✅ **DRY** — Configs defined once, reused multiple ways
- ✅ **Easy to subset** — Create multiple cache strategies (dev only caches critical reports, prod caches all)
- ✅ **Scales to 100+ reports** — Dict keys are much cleaner than `_REPORTS[0]`, `_REPORTS[1]`, etc.

#### Usage

```python
from config.reports import get_all_reports, get_reports_to_cache, get_report

# Use in your application
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),        # All reports for metadata
    report_configs_to_cache=get_reports_to_cache() # Subset for data
)

# Access individual report when needed
customer_config = get_report("customers_active")
data = get_data(client, cache_mgr, customer_config, metadata)
```

### Organization Strategies

#### Strategy 1: Single App, Multiple Tables (DRY Approach)

When all reports come from one app and use the same report name, use a loop to reduce duplication:

```python
# config/reports.py
"""Customer Portal app reports."""

import os
from quickbase_extract.config import ReportConfig

CUSTOMER_PORTAL_APP_ID = os.environ.get("QB_CUSTOMER_PORTAL_APP_ID", "bq8xyz123")

# Tables in the app (one report per table)
_TABLES = [
    "Customers",
    "Orders",
    "Products",
    "Invoices",
    "Payments",
    "Shipping Addresses",
    "Support Tickets",
]

# Generate configs automatically (normalize table names for keys)
_REPORTS = {
    table.lower().replace(" ", "_"): ReportConfig(
        app_id=CUSTOMER_PORTAL_APP_ID,
        app_name="customer_portal",
        table_name=table,
        report_name="Python"  # All use same report name
    )
    for table in _TABLES
}

def get_all_reports():
    """Get all Customer Portal reports."""
    return list(_REPORTS.values())

# Usage
all_configs = get_all_reports()
# [ReportConfig(..., table_name="Customers", ...),
#  ReportConfig(..., table_name="Orders", ...), ...]
```

#### Strategy 2: Multiple Apps, Grouped by Purpose

Organize by function or business area for easier management:

```python
# config/reports.py
"""Reports organized by business function."""

import os
from quickbase_extract.config import ReportConfig

SALES_APP_ID = os.environ.get("QB_SALES_APP_ID", "bq8abc123")
HR_APP_ID = os.environ.get("QB_HR_APP_ID", "bq9def456")
ACCOUNTING_APP_ID = os.environ.get("QB_ACCOUNTING_APP_ID", "bq7ghi789")

# Sales function reports
_SALES_REPORTS = {
    "customers_active": ReportConfig(
        app_id=SALES_APP_ID, app_name="sales",
        table_name="Customers", report_name="Active"
    ),
    "opportunities_open": ReportConfig(
        app_id=SALES_APP_ID, app_name="sales",
        table_name="Opportunities", report_name="Open"
    ),
    "orders_recent": ReportConfig(
        app_id=SALES_APP_ID, app_name="sales",
        table_name="Orders", report_name="Last 30 Days"
    ),
}

# HR function reports
_HR_REPORTS = {
    "employees_active": ReportConfig(
        app_id=HR_APP_ID, app_name="hr",
        table_name="Employees", report_name="Active"
    ),
    "timesheets_current": ReportConfig(
        app_id=HR_APP_ID, app_name="hr",
        table_name="Timesheets", report_name="Current Period"
    ),
}

# Accounting function reports
_ACCOUNTING_REPORTS = {
    "invoices_unpaid": ReportConfig(
        app_id=ACCOUNTING_APP_ID, app_name="accounting",
        table_name="Invoices", report_name="Unpaid"
    ),
    "expense_reports": ReportConfig(
        app_id=ACCOUNTING_APP_ID, app_name="accounting",
        table_name="Expenses", report_name="All"
    ),
}

# Combine all
_REPORTS = {**_SALES_REPORTS, **_HR_REPORTS, **_ACCOUNTING_REPORTS}

def get_all_reports():
    """Get all reports across all functions."""
    return list(_REPORTS.values())

def get_reports_by_function(function):
    """Get reports for specific business function.

    Args:
        function: "sales", "hr", or "accounting"
    """
    function_map = {
        "sales": _SALES_REPORTS,
        "hr": _HR_REPORTS,
        "accounting": _ACCOUNTING_REPORTS,
    }
    return list(function_map[function].values())

# Usage
sales_reports = get_reports_by_function("sales")
all_reports = get_all_reports()
```

#### Strategy 3: Environment-Specific Subsets

Cache different reports in dev, staging, and prod:

```python
# config/reports.py
"""Environment-aware report caching strategy."""

import os
from quickbase_extract.config import ReportConfig

APP_ID = os.environ.get("QB_APP_ID", "bq8xyx9z")

_REPORTS = {
    "customers": ReportConfig(APP_ID, "sales", "Customers", "Python"),
    "orders": ReportConfig(APP_ID, "sales", "Orders", "Python"),
    "products": ReportConfig(APP_ID, "sales", "Products", "Python"),
    "invoices": ReportConfig(APP_ID, "sales", "Invoices", "Python"),
    "payments": ReportConfig(APP_ID, "sales", "Payments", "Python"),
}

def get_all_reports():
    """All reports (for metadata refresh)."""
    return list(_REPORTS.values())

def get_reports_to_cache(env=None):
    """Get reports to cache based on environment.

    Args:
        env: Environment name ("dev", "staging", "prod").
             If None, reads from ENV environment variable.
    """
    env = env or os.environ.get("ENV", "dev")

    # Dev: cache only critical reports (faster, less storage)
    if env == "dev":
        return [_REPORTS["customers"], _REPORTS["orders"]]

    # Staging: cache most reports for testing
    elif env == "staging":
        return [
            _REPORTS["customers"],
            _REPORTS["orders"],
            _REPORTS["products"],
            _REPORTS["invoices"],
        ]

    # Prod: cache everything (comprehensive)
    else:  # prod
        return list(_REPORTS.values())

# Usage
cache_mgr = CacheManager(cache_root=Path("/tmp/cache"))
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),  # Env-specific
)
```

#### Strategy 4: Tag-Based Filtering (Advanced)

For fine-grained control, add metadata tags to configs:

```python
# config/reports.py
"""Reports with metadata tags for filtering."""

import os
from quickbase_extract.config import ReportConfig
from typing import NamedTuple

class TaggedReportConfig(NamedTuple):
    """Extended config with tags for organization."""
    config: ReportConfig
    tags: list[str]  # e.g., ["daily", "critical", "finance"]

APP_ID = os.environ.get("QB_APP_ID", "bq8xyx9z")

_TAGGED_REPORTS = {
    "customers": TaggedReportConfig(
        ReportConfig(APP_ID, "sales", "Customers", "Python"),
        tags=["daily", "critical", "sales"]
    ),
    "orders": TaggedReportConfig(
        ReportConfig(APP_ID, "sales", "Orders", "Python"),
        tags=["hourly", "critical", "sales"]
    ),
    "products": TaggedReportConfig(
        ReportConfig(APP_ID, "sales", "Products", "Python"),
        tags=["daily", "sales"]
    ),
    "invoices": TaggedReportConfig(
        ReportConfig(APP_ID, "sales", "Invoices", "Python"),
        tags=["daily", "finance", "critical"]
    ),
}

def get_all_reports():
    """All reports (for metadata refresh)."""
    return [cfg.config for cfg in _TAGGED_REPORTS.values()]

def get_reports_by_tags(*tags):
    """Get reports matching any of the given tags.

    Args:
        *tags: Tag names to filter by (e.g., "critical", "daily")

    Returns:
        List of ReportConfig instances matching the tags
    """
    matching = []
    for tagged_cfg in _TAGGED_REPORTS.values():
        if any(tag in tagged_cfg.tags for tag in tags):
            matching.append(tagged_cfg.config)
    return matching

# Usage
critical_reports = get_reports_by_tags("critical")  # Customers, Orders, Invoices
daily_reports = get_reports_by_tags("daily")         # Customers, Products, Invoices

ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=critical_reports,  # Only cache critical reports
)
```

### Using Configurations

```python
# main.py
"""Main application using report configurations."""

from quickbase_extract import (
    get_qb_client,
    get_data,
    load_data,
    load_report_metadata_batch,
    get_data_parallel
)
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from config.reports import get_all_reports, get_reports_to_cache, get_report
import os

# Initialize client
client = get_qb_client(
    realm=os.environ["QB_REALM"],
    user_token=os.environ["QB_USER_TOKEN"]
)

# Initialize cache
from pathlib import Path
from quickbase_extract import CacheManager
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))

# Ensure cache is fresh
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache()
)

# Load metadata for all reports
metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

# Fetch multiple reports at once
data = get_data_parallel(
    client,
    cache_mgr,
    get_reports_to_cache(),
    metadata,
    cache=True
)

# Process data
for config, records in data.items():
    print(f"{config.table_name}: {len(records)} records")

# Access individual report
customer_config = get_report("customers_active")
customers = data[customer_config]

# Or fetch a single report if not in parallel batch
orders_config = get_report("orders")
orders = get_data(client, cache_mgr, orders_config, metadata)

# Later, load from cache (no API call)
cached_customers = load_data(cache_mgr, customer_config, metadata)
```

### Dynamic Filters with ask_values

Quickbase reports can have "ask the user" placeholders in filters. Use the `ask_values` parameter to replace them at runtime.

#### How It Works

A Quickbase filter might be:
```
{'15'.EX.'_ask1_'}AND({'41'.EX.'_ask2_'}OR{'40'.EX.'_ask2_'})
```

The `_ask1_` and `_ask2_` are placeholders. At runtime, you provide actual values:

```python
from quickbase_extract import get_data

# Replace placeholders with actual values
ask_values = {
    "ask1": "Pending",
    "ask2": "urgent"
}

data = get_data(
    client,
    cache_mgr,
    report_config,
    metadata,
    ask_values=ask_values
)
# Filter becomes: {'15'.EX.'Pending'}AND({'41'.EX.'urgent'}OR{'40'.EX.'urgent'})
```

#### For Parallel Fetches

Each report can have different ask values:

```python
from quickbase_extract import get_data_parallel

# Map each report config to its ask_values
ask_values_map = {
    report_configs[0]: {"ask1": "Active"},
    report_configs[1]: {"ask1": "Inactive"},
}

data = get_data_parallel(
    client,
    cache_mgr,
    report_config,
    metadata,
    ask_values=ask_values_map
)
```

#### Error Handling

`_replace_ask_placeholders()` validates that:
1. All placeholders in the filter have corresponding values provided
2. All provided values are actually used in the filter

```python
# Error: Missing value for _ask2_
ask_values = {"ask1": "Active"}  # Missing ask2
data = get_data(client, cache_mgr, config, metadata, ask_values=ask_values)
# Raises: ValueError: requires values for ['_ask2_'], but they were not provided

# Error: Unused value
ask_values = {"ask1": "Active", "ask2": "unused"}  # ask2 not in filter
data = get_data(client, cache_mgr, config, metadata, ask_values=ask_values)
# Raises: ValueError: received ask_values ['ask2'] that are not used in the filter
```

#### Complete Example

```python
def fetch_sales_by_status(client, cache_mgr, metadata, status):
    """Fetch sales orders matching a specific status.

    The report has an ask placeholder for status filtering.
    """
    from quickbase_extract import get_data

    orders_config = get_report("orders")

    # Replace the _ask1_ placeholder with the provided status
    return get_data(
        client,
        cache_mgr,
        orders_config,
        metadata,
        ask_values={"ask1": status}
    )

# Usage
active_orders = fetch_sales_by_status(client, cache_mgr, metadata, "Active")
pending_orders = fetch_sales_by_status(client, cache_mgr, metadata, "Pending")
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

# Optional
QUICKBASE_CACHE_ROOT=./.quickbase-cache/dev
ENV=dev
```

Load with python-dotenv:

```python
# main.py
from dotenv import load_dotenv
load_dotenv()  # Load .env file

import os
from config.reports import get_all_reports

# Now environment variables are available
reports = get_all_reports()
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

1. **Use ReportConfig for all configs**
   - Provides type safety and IDE autocomplete
   - Makes it clear what parameters are required

2. **Store app IDs in environment variables**
   - Never hardcode credentials or IDs in source code
   - Use `.env` for local development
   - Use Lambda environment variables or Secrets Manager for production

3. **Keep configurations in a separate module**
   - Easy to maintain and update
   - Can be imported by multiple scripts
   - Version control friendly

4. **Choose an organization strategy based on scale**
   - 1-10 reports: Basic dict approach
   - 10-50 reports: Organize by app or function
   - 50-100+ reports: Use tags or environment-specific subsets

5. **Separate metadata refresh from data caching**
   - Refresh metadata for ALL reports (rarely changes)
   - Cache data only for reports you need (expensive)
   - Use `get_all_reports()` and `get_reports_to_cache()` functions

6. **Document your reports with metadata**
   ```python
   # In strategy 4 example, tags serve as documentation
   _TAGGED_REPORTS = {
       "invoices": TaggedReportConfig(
           ReportConfig(...),
           tags=["daily", "finance", "critical"]  # Explains importance
       ),
   }
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
   QB_REALM=your-realm.quickbase.com
   QB_USER_TOKEN=your_token_here
   QB_SALES_APP_ID=bq8abc123
   QB_HR_APP_ID=bq9def456
   CACHE_BUCKET=my-quickbase-cache-bucket
   ENV=prod
   ```

2. **Lambda handler example:**

```python
import os
from pathlib import Path
import quickbase_api

from quickbase_extract import CacheManager, sync_from_s3_once
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from quickbase_extract.report_metadata import load_report_metadata_batch
from quickbase_extract.report_data import get_data_parallel
from config.reports import get_all_reports, get_reports_to_cache

# Initialize client (reuse across warm starts for efficiency)
_client = None
_cache_mgr = None

def get_client():
    """Get or create Quickbase client (cached across warm starts)."""
    global _client
    if _client is None:
        _client = quickbase_api.client(
            realm=os.environ["QB_REALM"],
            user_token=os.environ["QB_USER_TOKEN"]
        )
    return _client

def get_cache_manager():
    """Get or create cache manager (cached across warm starts)."""
    global _cache_mgr
    if _cache_mgr is None:
        _cache_mgr = CacheManager(
            cache_root=Path("/tmp/my_project/cache"),
            s3_bucket=os.environ.get("CACHE_BUCKET"),
            s3_prefix="my_project/cache"
        )
    return _cache_mgr

def lambda_handler(event, context):
    """Lambda handler for Quickbase data fetching.

    Handles:
    - Cold start: Syncs cache from S3
    - Cache freshness: Auto-refreshes if stale
    - Data fetching: Queries Quickbase and caches results
    - Development: Supports forcing complete cache refresh for testing
    """
    client = get_client()
    cache_mgr = get_cache_manager()

    # OPTIONAL: Set to True to force cache refresh (for development/debugging only)
    # Only one should be True at a time. force_all overrides the others.
    FORCE_COMPLETE_CACHE_REFRESH_ALL = False
    FORCE_COMPLETE_CACHE_REFRESH_METADATA = False
    FORCE_COMPLETE_CACHE_REFRESH_DATA = False

    # Force complete cache refresh if needed (dev/debugging only)
    if (FORCE_COMPLETE_CACHE_REFRESH_ALL or FORCE_COMPLETE_CACHE_REFRESH_METADATA
            or FORCE_COMPLETE_CACHE_REFRESH_DATA):
        from quickbase_extract import complete_cache_refresh
        complete_cache_refresh(
            cache_manager=cache_mgr,
            client=client,
            report_configs=get_all_reports(),
            force_all=FORCE_COMPLETE_CACHE_REFRESH_ALL,
            force_metadata=FORCE_COMPLETE_CACHE_REFRESH_METADATA,
            force_data=FORCE_COMPLETE_CACHE_REFRESH_DATA,
        )

    # Step 1: Sync cache from S3 on cold start (only on first invocation)
    sync_from_s3_once(cache_mgr)

    # Step 2: Ensure cache is fresh (auto-refresh if stale)
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
        metadata_stale_hours=720,  # 30 days
        data_stale_hours=24         # 1 day
    )

    # Step 3: Load metadata from cache
    metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

    # Step 4: Fetch fresh data (uses cached metadata)
    reports_to_fetch = get_reports_to_cache()
    data = get_data_parallel(
        client,
        cache_mgr,
        reports_to_fetch,
        metadata,
        cache=True  # Caches results and syncs to S3
    )

    # Step 5: Process data
    response_data = {}
    for config, records in data.items():
        response_data[f"{config.table_name}_{config.report_name}"] = {
            "record_count": len(records),
            "sample": records[:5]  # First 5 records
        }

    return {
        "statusCode": 200,
        "body": {
            "message": "Successfully fetched Quickbase data",
            "reports": response_data
        }
    }

def lambda_handler_with_dynamic_filters(event, context):
    """Lambda handler that accepts dynamic ask_values.

    Allows callers to pass filter parameters at runtime.

    Event format:
    {
        "reports": ["customers", "orders"],
        "ask_values": {
            "customers": {"ask1": "Active"},
            "orders": {"ask1": "2024-01-01"}
        }
    }
    """
    from config.reports import get_report

    client = get_client()
    cache_mgr = get_cache_manager()

    sync_from_s3_once(cache_mgr)
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
    )

    metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

    # Get requested reports
    requested_reports = event.get("reports", [])
    report_config = [get_report(name) for name in requested_reports]

    # Map ask_values to configs
    ask_values_dict = event.get("ask_values", {})
    ask_values_map = {}
    for report_name, config in zip(requested_reports, report_config):
        if report_name in ask_values_dict:
            ask_values_map[config] = ask_values_dict[report_name]

    # Fetch with dynamic filters
    data = get_data_parallel(
        client,
        cache_mgr,
        report_config,
        metadata,
        ask_values=ask_values_map if ask_values_map else None,
        cache=True
    )

    return {
        "statusCode": 200,
        "body": {
            "reports_fetched": len(data),
            "total_records": sum(len(records) for records in data.values())
        }
    }
```

### How It Works

1. **Cold Start (first invocation)**
   - `sync_from_s3_once()` downloads cache from S3 to `/tmp`
   - Happens only once per container lifecycle
   - Subsequent invocations reuse the cached data

2. **Cache Freshness Check**
   - `ensure_cache_freshness()` checks if cache is stale
   - If fresh: proceeds (no API calls)
   - If stale: automatically refreshes from Quickbase
   - Checks metadata and data independently

3. **Data Fetching**
   - Uses cached metadata to know which fields to query
   - Makes efficient API calls only for needed data
   - Results cached locally and synced back to S3

4. **Warm Starts**
   - Client and cache manager are reused across invocations
   - No reinitializing = faster execution
   - Cache already in `/tmp` from previous invocation

### S3 Bucket Structure

```
my-quickbase-cache-bucket/
├── dev/
│   ├── report_metadata/
│   │   └── sales_tracker/
│   │       ├── customers_python.json
│   │       └── opportunities_python.json
│   └── report_data/
│       └── sales_tracker/
│           ├── customers_python_data.json
│           └── opportunities_python_data.json
├── staging/
│   ├── report_metadata/...
│   └── report_data/...
└── prod/
    ├── report_metadata/...
    └── report_data/...
```

### Permissions Required

Lambda execution role needs these S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-quickbase-cache-bucket",
        "arn:aws:s3:::my-quickbase-cache-bucket/*"
      ]
    }
  ]
}
```

### Debugging Lambda

Enable debug logging:

```python
import logging

# In lambda_handler, before cache operations
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("quickbase_extract")

# Now see detailed logs in CloudWatch
sync_from_s3_once(cache_mgr)  # Will log sync details
ensure_cache_freshness(...)   # Will log freshness checks
```

Check CloudWatch Logs for:
- Cache sync status
- Staleness checks and refresh decisions
- API call details
- Any errors encountered

### Performance Considerations

| Operation | Cold Start | Warm Start |
|-----------|-----------|-----------|
| S3 sync | ~2-5 seconds | 0 (skipped) |
| Cache freshness check | ~0.1s | ~0.1s |
| Metadata load | ~0.01s | ~0.01s |
| Data fetch (N reports) | ~5-30s | ~5-30s |
| **Total** | **~7-35s** | **~5-30s** |

**Tips to optimize:**
- Reduce `max_workers` in `get_data_parallel()` if hitting rate limits
- Use environment-specific subsets (`get_reports_to_cache()`) to cache only what's needed
- Set appropriate staleness thresholds to avoid unnecessary refreshes
- Reuse client/cache manager across warm starts (shown above)

## API Reference

### Configuration

#### `ReportConfig(app_id, app_name, table_name, report_name)`

A NamedTuple that identifies a Quickbase report.

**Parameters:**
- `app_id` (str): Quickbase app ID for API calls (e.g., "bq8xyx9z")
- `app_name` (str): Normalized app name for cache paths (e.g., "sales_tracker")
- `table_name` (str): Table name in Quickbase (e.g., "Customers")
- `report_name` (str): Report name within the table (e.g., "Active")

**Example:**
```python
from quickbase_extract.config import ReportConfig

config = ReportConfig(
    app_id="bq8xyx9z",
    app_name="sales_tracker",
    table_name="Customers",
    report_name="Active"
)

# Use as dict key
metadata = {config: {...}}
data = metadata[config]
```

### Metadata Operations

#### `load_report_metadata_batch(cache_manager, report_config)`

Load cached metadata for multiple reports.

**Parameters:**
- `cache_manager` (CacheManager): Cache manager instance
- `report_config` (list[ReportConfig]): List of ReportConfig instances to load

**Returns:** Dict mapping ReportConfig → metadata dict

**Raises:**
- `FileNotFoundError`: If any report metadata is not cached (run `get_report_metadata()` first)

**Example:**
```python
from quickbase_extract.cache_manager import CacheManager
from quickbase_extract.report_metadata import load_report_metadata_batch
from config.reports import get_all_reports

cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))
metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

# Access metadata for a specific report
config = get_all_reports()[0]
report_metadata = metadata[config]
print(report_metadata["table_id"])
```

**Metadata structure returned:**
```python
{
    ReportConfig(...): {
        "app_name": "sales_tracker",
        "table_name": "customers",
        "report_name": "active",
        "table_id": "tblXYZ123",
        "field_label": {"Record ID#": "3", "Name": "6", ...},
        "fields": [3, 6, 7, 8],
        "filter": "{8.EX.'Active'}",
        "sort_by": [{"fieldId": 6, "order": "ASC"}],
        "group_by": []
    }
}
```

#### `ensure_cache_freshness(client, cache_manager, report_configs_all, report_configs_to_cache=None, metadata_stale_hours=None, data_stale_hours=None, cache_all_data=False, force_metadata=False, force_data=False, force_all=False)`

Ensure cache is fresh; refresh metadata and/or data if empty or stale.

Checks metadata and data caches independently. Refreshes only the caches that are empty or stale, avoiding unnecessary API calls. Gracefully handles refresh failures (logs but does not re-raise).

**Parameters:**
- `client`: Quickbase API client
- `cache_manager` (CacheManager): Cache manager instance
- `report_configs_all` (list[ReportConfig]): All report configs (for metadata refresh)
- `report_configs_to_cache` (list[ReportConfig], optional): Subset of reports to cache data for. If None, data caching is disabled.
- `metadata_stale_hours` (float, optional): Threshold (hours) for metadata staleness. Defaults to 168 hours (7 days). Reads from `METADATA_STALE_HOURS` env var if not provided.
- `data_stale_hours` (float, optional): Threshold (hours) for data staleness. Defaults to 24 hours. Reads from `DATA_STALE_HOURS` env var if not provided.
- `cache_all_data` (bool): If True, caches data for ALL reports (ignores `report_configs_to_cache`). Useful for prod environments. Default: False.
- `force_all` (bool): If True, refreshes both metadata and data immediately, overriding all checks. Default: False.
- `force_metadata` (bool): If True (and `force_all` is False), refreshes metadata immediately. Default: False.
- `force_data` (bool): If True (and `force_all` is False), refreshes data immediately. Default: False.

**Environment Variables:**
- `METADATA_STALE_HOURS`: Override default metadata staleness threshold (in hours)
- `DATA_STALE_HOURS`: Override default data staleness threshold (in hours)

**Returns:** None

**Example:**
```python
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from config.reports import get_all_reports, get_reports_to_cache

# Standard usage: refresh metadata for all, data for subset
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
)

# Cache all reports in production
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    cache_all_data=True,  # Override subset, cache everything
)

# Force refresh regardless of freshness
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    force_all=True,
)

# Custom thresholds
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    metadata_stale_hours=720,   # 30 days
    data_stale_hours=6          # 6 hours
)
```

### Data Operations

#### `get_data(client, cache_manager, report_config, report_metadata, cache=False, ask_values=None)`

Query a Quickbase table for data using cached report metadata.

**Parameters:**
- `client`: Quickbase API client
- `cache_manager` (CacheManager): Cache manager instance
- `report_config` (ReportConfig): Report config identifying which report to fetch
- `report_metadata` (dict): Full metadata dict from `load_report_metadata_batch()`
- `cache` (bool): Whether to cache the retrieved data. Default: False.
- `ask_values` (dict[str, str], optional): For reports with "ask the user" placeholders in filters. Maps placeholder keys to values. Example: `{"ask1": "Active", "ask2": "2024-01-01"}`. See `_replace_ask_placeholders()` for details.

**Returns:** List of dicts with field labels as keys

**Raises:**
- `KeyError`: If report_config not found in report_metadata
- `ValueError`: If ask placeholders are missing values or unused values provided
- `Exception`: If Quickbase API query fails

**Example:**
```python
from quickbase_extract.report_data import get_data

# Simple fetch
customers = get_data(
    client,
    cache_mgr,
    report_configs[0],
    metadata,
    cache=True
)

# With dynamic filters (ask placeholders)
orders = get_data(
    client,
    cache_mgr,
    report_configs[1],
    metadata,
    ask_values={"ask1": "Pending", "ask2": "2024-01-01"}
)

# Process data
for record in customers:
    print(record["Name"], record["Email"])
```

#### `get_data_parallel(client, cache_manager, report_config, report_metadata, cache=False, max_workers=8, ask_values=None)`

Fetch data for multiple reports in parallel.

**Parameters:**
- `client`: Quickbase API client (should be thread-safe)
- `cache_manager` (CacheManager): Cache manager instance
- `report_config` (list[ReportConfig]): List of report configs to fetch
- `report_metadata` (dict): Full metadata dict from `load_report_metadata_batch()`
- `cache` (bool): Whether to cache the retrieved data. Default: False.
- `max_workers` (int): Maximum number of concurrent threads. Default: 8. Adjust based on API rate limits.
- `ask_values` (dict[ReportConfig, dict[str, str]], optional): Per-report ask values. Maps ReportConfig → ask_values dict. Example: `{config1: {"ask1": "Active"}, config2: {"ask1": "2024-01-01"}}`. See `_replace_ask_placeholders()` for details.

**Returns:** Dict mapping ReportConfig → list of record dicts

**Raises:**
- `KeyError`: If any report_config not found in report_metadata
- `ValueError`: If any ask placeholders are missing values or unused values provided
- `Exception`: First exception encountered during parallel execution (fail-fast)

**Example:**
```python
from quickbase_extract.report_data import get_data_parallel
from config.reports import get_reports_to_cache

# Simple parallel fetch
data = get_data_parallel(
    client,
    cache_mgr,
    get_reports_to_cache(),
    metadata,
    cache=True,
    max_workers=4
)

# With per-report ask values
ask_values = {
    report_config[0]: {"ask1": "Active"},
    report_config[1]: {"ask1": "2024-01-01"},
}
data = get_data_parallel(
    client,
    cache_mgr,
    [report_config[0], report_config[1]],
    metadata,
    ask_values=ask_values
)

# Process results
for config, records in data.items():
    print(f"{config.table_name}: {len(records)} records")
```

#### `load_data(cache_manager, report_config, report_metadata)`

Load cached data for a single report (no API call).

**Parameters:**
- `cache_manager` (CacheManager): Cache manager instance
- `report_config` (ReportConfig): Report config to load
- `report_metadata` (dict): Full metadata dict from `load_report_metadata_batch()`

**Returns:** List of dicts with field labels as keys

**Raises:**
- `KeyError`: If report_config not found in report_metadata
- `FileNotFoundError`: If cached data does not exist (run `get_data(..., cache=True)` first)

**Example:**
```python
from quickbase_extract.report_data import load_data

# Load from cache (instant, no API call)
cached_customers = load_data(cache_mgr, report_config[0], metadata)

print(f"Loaded {len(cached_customers)} customers from cache")
```

#### `load_data_batch(cache_manager, report_config, report_metadata)`

Load cached data for multiple reports.

**Parameters:**
- `cache_manager` (CacheManager): Cache manager instance
- `report_config` (list[ReportConfig]): List of report configs to load
- `report_metadata` (dict): Full metadata dict from `load_report_metadata_batch()`

**Returns:** Dict mapping ReportConfig → list of record dicts

**Raises:**
- `KeyError`: If any report_config not found in report_metadata
- `FileNotFoundError`: If any cached data does not exist

**Example:**
```python
from quickbase_extract.report_data import load_data_batch
from config.reports import get_reports_to_cache

# Load all cached data at once
all_data = load_data_batch(cache_mgr, get_reports_to_cache(), metadata)

for config, records in all_data.items():
    print(f"{config.table_name}: {len(records)} records")
```

### Filter Manipulation

#### `_replace_ask_placeholders(report_filter, ask_values, report_config)`

Replace "ask the user" placeholders in a Quickbase filter with actual values.

Quickbase reports can have dynamic filter placeholders like `_ask1_`, `_ask2_`, etc. This function replaces them with actual values at runtime.

**Parameters:**
- `report_filter` (str): Filter string from report metadata (e.g., `"{'15'.EX.'_ask1_'}AND{'40'.EX.'_ask2_'}"`)
- `ask_values` (dict[str, str]): Mapping of placeholder names to values. Keys are like "ask1", "ask2" (without underscores). Example: `{"ask1": "Pending", "ask2": "urgent"}`
- `report_config` (ReportConfig): Report config (used for error messages)

**Returns:** Modified filter string with placeholders replaced

**Raises:**
- `ValueError`: If placeholders in filter have no corresponding values, or if provided values are not used in filter

**Example:**
```python
from quickbase_extract.report_data import _replace_ask_placeholders

filter_str = "{'15'.EX.'_ask1_'}AND({'41'.EX.'_ask2_'}OR{'40'.EX.'_ask2_'})"
config = ReportConfig(...)

# Replace placeholders
result = _replace_ask_placeholders(
    filter_str,
    {"ask1": "Pending", "ask2": "urgent"},
    config
)
# Result: "{'15'.EX.'Pending'}AND({'41'.EX.'urgent'}OR{'40'.EX.'urgent'})"
```

**Error Handling:**
```python
# Missing ask1
try:
    _replace_ask_placeholders(filter_str, {"ask2": "value"}, config)
except ValueError as e:
    print(e)  # Missing required value for _ask1_

# Unused ask3
try:
    _replace_ask_placeholders(filter_str, {"ask1": "v1", "ask2": "v2", "ask3": "unused"}, config)
except ValueError as e:
    print(e)  # ask3 is not used in filter
```

### Cache Management

#### `CacheManager(cache_root, s3_bucket=None, s3_prefix=None)`

Manages cache reads/writes for both local and Lambda environments.

**Parameters:**
- `cache_root` (Path): Path to cache root directory (required). In Lambda, use `/tmp/path`.
- `s3_bucket` (str, optional): S3 bucket name for Lambda. If not provided, reads from `CACHE_BUCKET` environment variable.
- `s3_prefix` (str, optional): Path prefix within S3 bucket (required if using S3)

**Example:**
```python
from quickbase_extract import CacheManager
from pathlib import Path

# Local development
cache_mgr = CacheManager(cache_root=Path("my_project/dev/cache"))

# Lambda with S3
cache_mgr = CacheManager(
    cache_root=Path("/tmp/my_project/dev/cache"),
    s3_bucket="my-quickbase-cache",
    s3_prefix="my_project/dev/cache"
)
```

#### `sync_from_s3_once(cache_manager, force=False)`

Download cache from S3 to `/tmp` on Lambda cold start.

Call this once at the beginning of your Lambda handler. Subsequent calls on warm starts are no-ops (cache already synced in this container).

**Parameters:**
- `cache_manager` (CacheManager): Cache manager instance
- `force` (bool): Force sync even if already synced. Default: False.

**Returns:** None

**Example:**
```python
from quickbase_extract import sync_from_s3_once

def lambda_handler(event, context):
    cache_mgr = CacheManager(...)

    # Sync on cold start (no-op on warm starts)
    sync_from_s3_once(cache_mgr)

    # Proceed with cache operations
    metadata = load_report_metadata_batch(cache_mgr, config)
```

#### `complete_cache_refresh(cache_manager, client, report_configs, force_all=False, force_metadata=False, force_data=False)`

Completely refresh cache for development/debugging: clear /tmp, fetch fresh from Quickbase, update S3, re-sync to /tmp.

This is a development utility for forcing a complete cache refresh when report metadata or configurations change. Clears local /tmp cache, fetches fresh data from Quickbase, writes to S3, and re-syncs to /tmp.

**Parameters:**
- `cache_manager` (CacheManager): Cache manager instance
- `client`: Quickbase API client for fetching fresh data
- `report_configs` (list[ReportConfig]): List of all ReportConfig instances to refresh
- `force_all` (bool): If True, refresh both metadata and data. Defaults to False.
- `force_metadata` (bool): If True (and `force_all` is False), refresh only metadata. Defaults to False.
- `force_data` (bool): If True (and `force_all` is False), refresh only data. Defaults to False.

**Returns:** None

**Raises:**
- `Exception`: If cache clearing or refresh operations fail

**Example:**
```python
from quickbase_extract import complete_cache_refresh

# Refresh only metadata (after changing report configurations)
complete_cache_refresh(
    cache_manager=cache_mgr,
    client=qb_client,
    report_configs=get_all_reports(),
    force_metadata=True
)

# Refresh all (metadata + data)
complete_cache_refresh(
    cache_manager=cache_mgr,
    client=qb_client,
    report_configs=get_all_reports(),
    force_all=True
)
```

**Note:** This function is designed for development/debugging. To use in Lambda, add toggles to your handler (see "Development/Debug Mode" section below).

### Query Execution with Retry Logic

#### `handle_query(client, table_id, *, select=None, where=None, sort_by=None, group_by=None, options=None, description="", max_retries=3)`

Execute a Quickbase query with automatic retry logic for rate limits.

Automatically retries on 429 (rate limit) errors with exponential backoff.

**Parameters:**
- `client`: Quickbase API client
- `table_id` (str): Quickbase table ID
- `select` (list[int], optional): List of field IDs to return. If None, returns default fields.
- `where` (str, optional): Quickbase query string (e.g., `"{12.EX.'VPF'}"`)
- `sort_by` (list[dict], optional): Sort order. Example: `[{"fieldId": 6, "order": "ASC"}]`
- `group_by` (list[dict], optional): Grouping configuration
- `options` (dict, optional): Additional options (e.g., `{"skip": 0, "top": 100}`)
- `description` (str): Description for logging/error messages
- `max_retries` (int): Maximum retry attempts for rate limits. Default: 3.

**Returns:** Dict with "data" key containing list of records

**Raises:**
- `QuickbaseOperationError`: If query fails after retries

**Example:**
```python
from quickbase_extract.api_handlers import handle_query

# Simple query
result = handle_query(client, "tblABC123", description="fetch customers")

# Query with filters and fields
result = handle_query(
    client,
    "tblABC123",
    select=[3, 6, 7, 8],
    where="{8.EX.'Active'}",
    sort_by=[{"fieldId": 6, "order": "ASC"}],
    description="active customers"
)

data = result["data"]
for record in data:
    print(record)
```

### Error Handling

#### `QuickbaseOperationError`

Raised when a Quickbase API operation fails.

**Attributes:**
- `operation` (str): Operation that failed (e.g., "query", "upsert")
- `details` (str): Error details
- `original_error` (Exception): Original exception

**Example:**
```python
from quickbase_extract.api_handlers import handle_query, QuickbaseOperationError

try:
    result = handle_query(client, "tblXYZ123")
except QuickbaseOperationError as e:
    print(f"Operation {e.operation} failed: {e.details}")
```

### Utility Functions

#### `normalize_name(name)`

Normalize a name to lowercase with underscores replacing spaces.

Used internally for cache path consistency. Useful for normalizing app names, table names, etc.

**Parameters:**
- `name` (str): String to normalize

**Returns:** Normalized string

**Example:**
```python
from quickbase_extract.utils import normalize_name

normalize_name("Sales Tracker")     # "sales_tracker"
normalize_name("Active Customers")  # "active_customers"
normalize_name("already_normalized") # "already_normalized"
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
    refresh_all(client, report_config)
```

### 2. Data Caching Strategy

For Lambda, cache data during the function execution to avoid repeated API calls:

```python
# Good: Fetch once, cache, reuse
metadata = load_report_metadata_batch(report_config)
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

**Error Message:**
```
FileNotFoundError: Report metadata not found for ReportConfig(...).
Run get_report_metadata() first. Expected: /path/to/cache/...
```

**Cause:** You're trying to load metadata that hasn't been cached yet.

**Solution:** Run metadata refresh first:

```python
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from config.reports import get_all_reports, get_reports_to_cache

# Refresh metadata before using
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
)

# Now load should work
metadata = load_report_metadata_batch(cache_mgr, get_all_reports())
```

---

### Issue: "Rate limit exceeded" (429 errors)

**Error Message:**
```
HTTP 429: Too Many Requests - Rate limit exceeded
```

**Cause:** Too many concurrent requests to Quickbase API.

**Solutions:**

1. **Reduce `max_workers` in parallel operations:**
   ```python
   # Reduce concurrency
   data = get_data_parallel(
       client,
       cache_mgr,
       reports,
       metadata,
       max_workers=2  # Was 8
   )
   ```

2. **Increase retry attempts:**
   ```python
   from quickbase_extract.api_handlers import handle_query

   result = handle_query(
       client,
       table_id,
       max_retries=5,  # More attempts
       description="query"
   )
   ```

3. **Use caching to reduce API calls:**
   ```python
   # Cache data aggressively
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=get_reports_to_cache(),
       data_stale_hours=168  # Cache for 7 days
   )
   ```

4. **Contact Quickbase support** if you have legitimate high-volume needs

---

### Issue: Lambda "Cache not synced from S3"

**Error Message:**
```
FileNotFoundError: Cache files not found in /tmp
```

**Cause:** `sync_from_s3_once()` wasn't called, or S3 bucket/permissions issue.

**Solutions:**

1. **Ensure `sync_from_s3_once()` is called first:**
   ```python
   from quickbase_extract import sync_from_s3_once

   def lambda_handler(event, context):
       cache_mgr = CacheManager(...)

       # Must call this first!
       sync_from_s3_once(cache_mgr)

       # Then proceed
       metadata = load_report_metadata_batch(...)
   ```

2. **Verify environment variables:**
   ```python
   import os

   # Check these are set
   print(os.environ.get("CACHE_BUCKET"))  # Should not be None
   print(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))  # Should be set
   ```

3. **Check S3 permissions:**
   - Lambda execution role needs `s3:GetObject`, `s3:PutObject`
   - Verify S3 bucket exists and has files in it
   - Check bucket name matches exactly

4. **Verify cache structure in S3:**
   ```bash
   # List S3 contents
   aws s3 ls s3://my-quickbase-cache-bucket/ --recursive

   # Should see:
   # cache/report_metadata/app_name/...
   # cache/report_data/app_name/...
   ```

---

### Issue: "KeyError: ReportConfig not in metadata"

**Error Message:**
```
KeyError: ReportConfig(app_id='bq8xyx9z', app_name='sales', ...)
```

**Cause:** The `ReportConfig` you're using doesn't exist in the loaded metadata.

**Solutions:**

1. **Ensure metadata is loaded for this config:**
   ```python
   from config.reports import get_all_reports

   # Load metadata for ALL reports
   metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

   # Then use a config that's in get_all_reports()
   config = get_all_reports()[0]
   data = get_data(client, cache_mgr, config, metadata)
   ```

2. **Verify config matches exactly:**
   ```python
   from config.reports import get_report

   # Use function to get config (ensures consistency)
   config = get_report("customers_active")

   # Don't create a new instance
   config = ReportConfig(...)  # ❌ Wrong (different object)
   ```

3. **Check you're using the right config subset:**
   ```python
   # If you only cached a subset
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=[config1, config2],  # Only these
   )

   # Then only load those
   metadata = load_report_metadata_batch(cache_mgr, [config1, config2])

   # Don't try to use config3
   ```

---

### Issue: "Missing placeholder values" in ask_values

**Error Message:**
```
ValueError: Report ReportConfig(...) filter requires values for ['_ask1_'],
but they were not provided in ask_values.
```

**Cause:** Filter has `_ask1_` placeholder, but you didn't provide `{"ask1": "value"}`.

**Solution:** Provide all required ask values:

```python
# Report filter: {'15'.EX.'_ask1_'}AND{'40'.EX.'_ask2_'}
data = get_data(
    client,
    cache_mgr,
    config,
    metadata,
    ask_values={
        "ask1": "Pending",    # Required for _ask1_
        "ask2": "urgent"      # Required for _ask2_
    }
)
```

**Debugging:** Check the report's filter to see what placeholders exist:

```python
metadata = load_report_metadata_batch(cache_mgr, [config])
info = metadata[config]
print(info["filter"])  # See what _askN_ placeholders are in the filter
```

---

### Issue: "Unused ask_values provided"

**Error Message:**
```
ValueError: Report ReportConfig(...) received ask_values ['ask3']
that are not used in the filter.
```

**Cause:** You provided a value that the filter doesn't need.

**Solution:** Only provide ask values that are in the filter:

```python
# Filter only uses _ask1_
metadata = load_report_metadata_batch(cache_mgr, [config])
info = metadata[config]
print(info["filter"])  # {'15'.EX.'_ask1_'} - only needs ask1

# Only provide ask1
data = get_data(
    client,
    cache_mgr,
    config,
    metadata,
    ask_values={"ask1": "Pending"}  # Don't include ask2, ask3, etc.
)
```

---

### Issue: Cache directory does not exist

**Error Message:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/path/to/cache/...'
```

**Cause:** Cache root path doesn't exist or parent isn't writable.

**Solution:** Ensure cache root exists and is writable:

```python
from pathlib import Path
from quickbase_extract import CacheManager

# Explicit path
cache_path = Path("my_project/dev/cache")
cache_path.mkdir(parents=True, exist_ok=True)  # Create if needed

cache_mgr = CacheManager(cache_root=cache_path)
```

**For Lambda:** `/tmp` always exists and is writable:

```python
# Lambda always works
cache_mgr = CacheManager(cache_root=Path("/tmp/my_project/cache"))
```

---

### Issue: Metadata structure different than expected

**Error Message:**
```
KeyError: 'report'  # or other metadata key
```

**Cause:** Simplified metadata structure no longer has nested `"report"` object.

**Solution:** Use top-level fields directly:

```python
metadata = load_report_metadata_batch(cache_mgr, configs)
config = configs[0]
info = metadata[config]

# ❌ Old (no longer works)
sort_by = info["report"]["query"]["sortBy"]
fields = info["report"]["query"]["fields"]

# ✅ New (correct)
sort_by = info["sort_by"]
fields = info["fields"]
```

**Metadata structure:**
```python
{
    ReportConfig(...): {
        "app_name": "sales_tracker",
        "table_name": "customers",
        "report_name": "python",
        "table_id": "tblXYZ123",
        "field_label": {...},
        "fields": [3, 6, 7, 8],          # Top level now
        "filter": "{8.EX.'Active'}",     # Top level now
        "sort_by": [{"fieldId": 6, ...}],  # Top level now
        "group_by": []                   # Top level now
    }
}
```

---

### Issue: Data fetch returns empty or different results

**Symptom:** `get_data()` returns empty list or fewer records than expected.

**Cause:** Likely due to ask_values placeholder mismatches or stale metadata.

**Solutions:**

1. **Verify metadata is fresh:**
   ```python
   # Force refresh metadata
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=get_reports_to_cache(),
       force_metadata=True  # Force metadata refresh
   )
   ```

2. **Check ask_values match filter:**
   ```python
   metadata = load_report_metadata_batch(cache_mgr, [config])
   print(metadata[config]["filter"])  # See what placeholders are needed

   # Provide matching ask_values
   data = get_data(
       client,
       cache_mgr,
       config,
       metadata,
       ask_values={"ask1": "value_matching_filter"}
   )
   ```

3. **Test with API directly:**
   ```python
   from quickbase_extract.api_handlers import handle_query

   # Bypass cache and metadata, query directly
   result = handle_query(
       client,
       "tblXYZ123",
       select=[3, 6, 7],
       where="{8.EX.'Active'}"
   )
   print(len(result["data"]))  # See raw API result
   ```

---

### Issue: CloudWatch logs show "Data cache refresh failed"

**Symptom:** Refresh attempted but failed gracefully (logged but didn't crash).

**Cause:** API failure during refresh, or metadata load failed before data fetch.

**Solution:**

1. **Check previous log lines for root cause:**
   ```
   WARNING: Metadata cache refresh needed: [reasons]
   ERROR: Metadata cache refresh failed: [details]
   WARNING: Data cache refresh needed: [reasons]
   ERROR: Data cache refresh failed: [details]
   ```

2. **Enable debug logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   logger = logging.getLogger("quickbase_extract")
   ```

3. **Retry manually:**
   ```python
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=get_reports_to_cache(),
       force_all=True  # Force retry
   )
   ```

---

### Issue: Lambda timeout during cache sync from S3

**Symptom:** Lambda times out when syncing large cache from S3.

**Cause:** Cache is large, or network is slow.

**Solutions:**

1. **Increase Lambda timeout** (default 3 seconds):
   ```
   Lambda Configuration → Timeout: 300 seconds (5 minutes)
   ```

2. **Increase Lambda memory:**
   ```
   Lambda Configuration → Memory: 1024 MB (higher = faster)
   ```

3. **Split cache into smaller chunks:**
   ```python
   # Store dev/staging/prod in separate buckets
   # Or separate by app (sales vs hr)
   cache_mgr = CacheManager(
       cache_root=Path("/tmp/cache"),
       s3_bucket=os.environ.get("CACHE_BUCKET_CRITICAL"),  # Smaller
       s3_prefix="critical_reports/cache"
   )
   ```

---

### Issue: "ask1, ask2" not replaced in filter

**Symptom:** Filter still contains `_ask1_` after calling `get_data()` with ask_values.

**Cause:** Placeholder key doesn't match filter placeholder format.

**Solution:** Ensure key matches placeholder without underscores:

```python
# Filter: {'15'.EX.'_ask1_'}
# ❌ Wrong key name
ask_values = {"_ask1_": "value"}  # Has underscores

# ✅ Correct key name
ask_values = {"ask1": "value"}    # No underscores
```

---

### Issue: Lambda has old cached data after I changed report metadata

**Symptom:** You updated a Quickbase report's fields, filters, or configuration, but your Lambda returns stale data.

**Cause:** Cache was loaded before your changes, and it hasn't become "stale" enough to auto-refresh yet.

**Solutions:**

1. **Quick fix: Use force refresh toggle (development only)**

   In your Lambda handler, temporarily set:
   ```python
   FORCE_COMPLETE_CACHE_REFRESH_METADATA = True
   ```

   Upload new build, invoke Lambda, check logs for refresh. Then revert flag to `False`.

2. **For immediate production fix:**

   Manually delete files from S3:
   ```bash
   aws s3 rm s3://my-quickbase-cache-bucket/prod/cache/report_metadata/ --recursive
   ```

   Next Lambda cold start will re-fetch fresh metadata.

3. **To prevent in future:**

   Reduce `metadata_stale_hours` threshold:
   ```python
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=get_reports_to_cache(),
       metadata_stale_hours=24  # Check daily instead of 30 days
   )
   ```

---

### Issue: Performance degradation over time

**Symptom:** First request fast, subsequent requests slow.

**Cause:** Cache growing large, or connection pool issues.

**Solutions:**

1. **Monitor cache size:**
   ```python
   import os
   cache_size = sum(
       os.path.getsize(f) for f in Path(cache_root).rglob("*")
       if f.is_file()
   )
   print(f"Cache size: {cache_size / 1024 / 1024:.1f} MB")
   ```

2. **Implement cache cleanup:**
   ```python
   import shutil
   from pathlib import Path

   def cleanup_old_cache(cache_root, days=30):
       """Delete cache files older than N days."""
       from datetime import datetime, timedelta
       cutoff = datetime.now() - timedelta(days=days)

       for cache_file in Path(cache_root).rglob("*"):
           if cache_file.is_file():
               mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
               if mtime < cutoff:
                   cache_file.unlink()
   ```

3. **Use streaming for large results** (see Performance Optimization section)

4. **Reduce data_stale_hours** to refresh more frequently:
   ```python
   ensure_cache_freshness(
       client=client,
       cache_manager=cache_mgr,
       report_configs_all=get_all_reports(),
       report_configs_to_cache=get_reports_to_cache(),
       data_stale_hours=6  # Refresh every 6 hours
   )
   ```

## Cache Freshness Management

### Automatic Cache Refresh

Use `ensure_cache_freshness()` to automatically check and refresh cache if stale. This is the recommended approach for most applications.

```python
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from quickbase_extract.report_metadata import load_report_metadata_batch
from quickbase_extract.report_data import get_data_parallel
from config.reports import get_all_reports, get_reports_to_cache

def fetch_reports(client, cache_mgr):
    """Typical application flow with automatic cache management."""

    # Step 1: Ensure cache is fresh (auto-refresh if needed)
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
        metadata_stale_hours=720,  # 30 days
        data_stale_hours=24         # 1 day
    )

    # Step 2: Load metadata (always fresh after step 1)
    metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

    # Step 3: Fetch data (uses cached metadata, cache handles freshness)
    data = get_data_parallel(
        client,
        cache_mgr,
        get_reports_to_cache(),
        metadata,
        cache=True
    )

    return data
```

### Independent Cache Refresh

Metadata and data are refreshed **independently** based on their staleness thresholds:

| Scenario | Action |
|----------|--------|
| Metadata fresh, Data fresh | No refresh (use existing cache) |
| Metadata stale, Data fresh | Refresh only metadata |
| Metadata fresh, Data stale | Refresh only data |
| Both stale | Refresh both |

This minimizes unnecessary API calls and execution time.

```python
# Example: Only metadata is stale
# - Metadata refreshed from Quickbase (API call)
# - Data reused from cache (no API call)
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    metadata_stale_hours=168,  # 7 days
    data_stale_hours=24        # 1 day
)
```

### Separate Metadata and Data Caching

Metadata changes rarely; data changes frequently. Configure different subsets:

```python
from config.reports import get_all_reports, get_reports_to_cache

# All reports for metadata (rarely changes)
all_reports = get_all_reports()

# Only critical reports for data (expensive to refresh)
reports_to_cache = get_reports_to_cache()

ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=all_reports,           # Refresh ALL metadata
    report_configs_to_cache=reports_to_cache, # Cache only critical data
)
```

### Force Refresh

Force a cache refresh either programmatically or via environment variable:

#### Programmatic Force

```python
# Force refresh of both metadata and data
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    force_all=True  # Skip age checks, refresh immediately
)

# Force metadata refresh only
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    force_metadata=True  # Skip metadata age checks
)

# Force data refresh only
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    force_data=True  # Skip data age checks
)
```

### Cache-All-Data Mode

For production, cache data for all reports instead of a subset:

```python
# Dev: cache only critical reports
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),  # Subset
)

# Prod: cache everything
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    cache_all_data=True,  # Override subset, cache all
)
```

### Custom Staleness Thresholds

Different caches can have different thresholds based on how often they change:

```python
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
    metadata_stale_hours=720,   # 30 days (rarely changes)
    data_stale_hours=6          # 6 hours (changes frequently)
)
```

#### Recommended Thresholds

| Cache Type | Threshold | Reason |
|------------|-----------|--------|
| Metadata | 168-720 hours (7-30 days) | Table structure rarely changes |
| Data - Real-time apps | 1 hour | Data changes frequently |
| Data - Daily reports | 24 hours | Data updated once daily |
| Data - Weekly reports | 168 hours (7 days) | Data updated weekly |

#### Environment Variable Thresholds

```bash
# Set thresholds via environment variables
export METADATA_STALE_HOURS=720    # 30 days
export DATA_STALE_HOURS=6          # 6 hours

# Then call normally (will use env var values)
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache()
)
```

### Error Handling During Refresh

Refresh failures are logged but don't crash your application (graceful degradation):

```python
# If refresh fails, logged but doesn't raise exception
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_to_cache(),
)

# Check CloudWatch Logs or application logs for:
# - "Metadata cache refresh failed: ..."
# - "Data cache refresh failed: ..."

# Application continues with stale cache if refresh fails
```

### Lambda Handler with Automatic Cache Management

```python
import os
from pathlib import Path
import quickbase_api
from quickbase_extract import CacheManager, sync_from_s3_once
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from quickbase_extract.report_metadata import load_report_metadata_batch
from quickbase_extract.report_data import get_data_parallel
from config.reports import get_all_reports, get_reports_to_cache

# Global clients (reused across warm starts)
_client = None
_cache_mgr = None

def get_client():
    global _client
    if _client is None:
        _client = quickbase_api.client(
            realm=os.environ["QB_REALM"],
            user_token=os.environ["QB_USER_TOKEN"]
        )
    return _client

def get_cache_manager():
    global _cache_mgr
    if _cache_mgr is None:
        _cache_mgr = CacheManager(
            cache_root=Path("/tmp/cache"),
            s3_bucket=os.environ.get("CACHE_BUCKET"),
            s3_prefix="cache"
        )
    return _cache_mgr

def lambda_handler(event, context):
    """Lambda handler with automatic cache management."""
    client = get_client()
    cache_mgr = get_cache_manager()

    # Cold start: sync from S3
    sync_from_s3_once(cache_mgr)

    # Automatic cache freshness check and refresh
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
        metadata_stale_hours=int(os.environ.get("METADATA_STALE_HOURS", 720)),
        data_stale_hours=int(os.environ.get("DATA_STALE_HOURS", 24))
    )

    # Load metadata (always fresh)
    metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

    # Fetch data
    data = get_data_parallel(
        client,
        cache_mgr,
        get_reports_to_cache(),
        metadata,
        cache=True
    )

    return {
        "statusCode": 200,
        "reports_fetched": len(data)
    }
```

### Monitoring Cache Age

```python
from quickbase_extract import CacheManager
import logging

logger = logging.getLogger(__name__)

def monitor_cache_age(cache_mgr):
    """Check and log cache age."""
    metadata_age = cache_mgr.get_cache_age_hours("metadata")
    data_age = cache_mgr.get_cache_age_hours("data")

    logger.info(f"Cache age: metadata {metadata_age}h, data {data_age}h")

    # Check if approaching staleness
    if metadata_age > 600:  # Approaching 30 days
        logger.warning("Metadata cache approaching staleness threshold")

    if data_age > 20:  # Approaching 24 hours
        logger.warning("Data cache approaching staleness threshold")

    return {
        "metadata_age_hours": metadata_age,
        "data_age_hours": data_age
    }
```

### Scheduling Regular Refreshes

For Lambda, use CloudWatch Events to trigger periodic refreshes:

```python
# CloudWatch Rule: "cron(0 2 * * ? *)" (2 AM UTC daily)
def scheduled_cache_refresh(event, context):
    """Scheduled Lambda to refresh cache on a schedule."""
    client = get_client()
    cache_mgr = get_cache_manager()

    sync_from_s3_once(cache_mgr)

    # Force refresh (ignore age checks)
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
        force_all=True  # Always refresh on scheduled runs
    )

    logger.info("Scheduled cache refresh completed")

    return {"statusCode": 200, "message": "Cache refreshed"}
```

### Development/Debug Mode: Forcing Complete Cache Refresh

When you modify report metadata or configurations in Quickbase, your Lambda may still use stale cached data. Use the force refresh toggles to clear everything and fetch fresh data.

#### When to Use

- You changed a report's filters or fields in Quickbase
- You added/removed fields from a report
- You renamed a report or table
- You need to verify fresh data is being fetched

#### How to Use

1. Open your Lambda handler code
2. Set one of the toggle flags to `True`:

```python
# In lambda_handler, find these lines:
FORCE_COMPLETE_CACHE_REFRESH_ALL = False
FORCE_COMPLETE_CACHE_REFRESH_METADATA = False
FORCE_COMPLETE_CACHE_REFRESH_DATA = False

# Change to (example: refresh only metadata):
FORCE_COMPLETE_CACHE_REFRESH_METADATA = True
```

3. Upload new Lambda build
4. Invoke your Lambda (via API or CloudWatch event)
5. Check CloudWatch logs for cache refresh messages
6. **Revert the flag back to `False`** for normal operation

#### Flag Options

| Flag | What Gets Refreshed | Use When |
|------|---------------------|----------|
| `force_all=True` | Both metadata + data | Complete cache overhaul needed |
| `force_metadata=True` | Only metadata | Report configuration changed |
| `force_data=True` | Only data | Data needs fresh pull |

#### What Happens

When you trigger a force refresh:

1. ✓ `/tmp` cache directories are deleted
2. ✓ Fresh data fetched from Quickbase API
3. ✓ Data written to S3
4. ✓ `/tmp` re-synced from updated S3

Your Lambda now has fresh data from Quickbase.

#### Example

```python
def lambda_handler(event, context):
    client = get_client()
    cache_mgr = get_cache_manager()

    # Metadata changed in Quickbase? Force refresh it:
    FORCE_COMPLETE_CACHE_REFRESH_METADATA = True  # ← Toggle this

    if (FORCE_COMPLETE_CACHE_REFRESH_ALL or FORCE_COMPLETE_CACHE_REFRESH_METADATA
            or FORCE_COMPLETE_CACHE_REFRESH_DATA):
        from quickbase_extract import complete_cache_refresh
        complete_cache_refresh(
            cache_manager=cache_mgr,
            client=client,
            report_configs=get_all_reports(),
            force_metadata=FORCE_COMPLETE_CACHE_REFRESH_METADATA,
        )

    # Rest of handler...
```

**CloudWatch logs will show:**
```
WARNING: Starting complete cache refresh for: metadata (clearing /tmp, refreshing from Quickbase, updating S3...)
DEBUG: Reset cache sync flag
INFO: Fetching fresh data from Quickbase...
INFO: Re-syncing /tmp from S3...
WARNING: Complete cache refresh finished for metadata: /tmp and S3 now have fresh data from Quickbase
```

#### Important Notes

- **Don't leave toggles set to `True`** — revert to `False` after testing
- **Only for development** — not a production workflow
- Logs will show exactly what was refreshed
- Safe to use — doesn't affect running processes, only next Lambda invocation

## Advanced Usage

### Custom Report Configurations

#### Using Field IDs Instead of Report Names

Sometimes you want to query specific fields directly without relying on a pre-configured Quickbase report.

```python
from quickbase_extract.api_handlers import handle_query

# Query specific fields directly
result = handle_query(
    client,
    table_id="tblABC123",
    select=[3, 6, 7, 8],  # Field IDs
    where="{8.EX.'Active'}AND{12.GT.'2024-01-01'}",
    sort_by=[{"fieldId": 6, "order": "ASC"}],
    description="custom field query"
)

data = result["data"]
```

#### Dynamic Report Filtering with Ask Placeholders

Modify filters at runtime using ask placeholders instead of fetching the same report multiple times.

```python
from quickbase_extract.report_data import get_data
from config.reports import get_report

def fetch_customers_by_status(client, cache_mgr, metadata, status):
    """Fetch customers with dynamic status filter.

    The report has an ask placeholder: {'8'.EX.'_ask1_'}
    """
    config = get_report("customers_active")

    return get_data(
        client,
        cache_mgr,
        config,
        metadata,
        ask_values={"ask1": status}  # Replace _ask1_ at runtime
    )

# Usage
active = fetch_customers_by_status(client, cache_mgr, metadata, "Active")
inactive = fetch_customers_by_status(client, cache_mgr, metadata, "Inactive")
```

#### Multi-Placeholder Filters

For complex filters with multiple ask placeholders:

```python
def fetch_orders_by_date_and_status(client, cache_mgr, metadata, start_date, end_date, status):
    """Fetch orders with multiple dynamic filters.

    Report filter: {'12'.GT.'_ask1_'}AND{'12'.LT.'_ask2_'}AND{'15'.EX.'_ask3_'}
    """
    config = get_report("orders")

    return get_data(
        client,
        cache_mgr,
        config,
        metadata,
        ask_values={
            "ask1": start_date,    # Replace _ask1_
            "ask2": end_date,      # Replace _ask2_
            "ask3": status         # Replace _ask3_
        }
    )

# Usage
recent_pending = fetch_orders_by_date_and_status(
    client, cache_mgr, metadata,
    start_date="2024-01-01",
    end_date="2024-01-31",
    status="Pending"
)
```

### Batch Processing with Progress Tracking

```python
from quickbase_extract.report_data import get_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_reports_with_progress(client, cache_mgr, metadata, report_config):
    """Process multiple reports with progress tracking.

    Useful for long-running operations or monitoring large batches.
    """
    results = {}
    total = len(report_config)

    for i, config in enumerate(report_config, 1):
        logger.info(f"Processing {i}/{total}: {config.table_name} - {config.report_name}")

        try:
            data = get_data(client, cache_mgr, config, metadata, cache=True)
            results[config] = {
                "status": "success",
                "record_count": len(data),
                "data": data
            }
            logger.info(f"✓ {config.table_name}: {len(data)} records")
        except Exception as e:
            results[config] = {
                "status": "error",
                "error": str(e)
            }
            logger.error(f"✗ {config.table_name}: {e}")

    # Summary
    successful = sum(1 for r in results.values() if r["status"] == "success")
    logger.info(f"Completed: {successful}/{total} reports processed successfully")

    return results

# Usage
from config.reports import get_reports_to_cache

results = process_reports_with_progress(
    client,
    cache_mgr,
    metadata,
    get_reports_to_cache()
)

# Check results
for config, result in results.items():
    if result["status"] == "success":
        print(f"{config.table_name}: {result['record_count']} records")
    else:
        print(f"{config.table_name}: FAILED - {result['error']}")
```

### Incremental Data Updates

```python
from quickbase_extract.api_handlers import handle_query, handle_upsert
from datetime import datetime, timedelta

def sync_recent_changes(client, source_table_id, target_table_id, days_back=1):
    """Sync only records modified in the last N days.

    Efficient for keeping systems in sync without full refresh.
    """
    # Calculate cutoff date
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # Fetch recent changes (Quickbase field 1 is typically "Date Modified")
    result = handle_query(
        client,
        source_table_id,
        where=f"{{1.AFT.'{cutoff_date}'}}",  # Date Modified after cutoff
        description=f"recent changes from {cutoff_date}"
    )

    recent_data = result["data"]

    if not recent_data:
        print("No recent changes found")
        return 0

    # Transform and upsert to target
    transformed = [transform_record(r) for r in recent_data]

    result = handle_upsert(
        client,
        target_table_id,
        transformed,
        description="sync recent changes"
    )

    total_synced = (
        len(result.get("metadata", {}).get("createdRecordIds", [])) +
        len(result.get("metadata", {}).get("updatedRecordIds", []))
    )

    print(f"Synced {total_synced} records")
    return total_synced

def transform_record(raw_record):
    """Transform Quickbase record format for upsert."""
    # Quickbase API uses nested format: {fieldId: {value: actual_value}}
    # Transform to upsert format
    transformed = {}
    for field_id, field_data in raw_record.items():
        transformed[field_id] = {"value": field_data.get("value")}
    return transformed

# Usage
synced = sync_recent_changes(
    client,
    source_table_id="tblXYZ123",
    target_table_id="tblABC456",
    days_back=7  # Sync last 7 days
)
```

### Multi-Environment Configuration

```python
import os
from pathlib import Path
from quickbase_extract import get_qb_client, CacheManager
from config.reports import get_all_reports, get_reports_to_cache

class QuickbaseConfig:
    """Environment-aware Quickbase configuration.

    Provides different settings based on deployment environment.
    """

    def __init__(self, env=None):
        self.env = env or os.environ.get("ENV", "dev")
        self._validate_env()
        self.config = self._load_config()

    def _validate_env(self):
        """Validate environment is recognized."""
        if self.env not in ["dev", "staging", "prod"]:
            raise ValueError(f"Unknown environment: {self.env}")

    def _load_config(self):
        """Load environment-specific configuration."""
        config = {
            "dev": {
                "realm": os.environ.get("QB_REALM", "dev-realm.quickbase.com"),
                "token": os.environ.get("QB_USER_TOKEN"),
                "cache_root": Path("./.quickbase-cache/dev"),
                "s3_bucket": None,  # Local only
                "metadata_stale_hours": 24,   # Refresh often in dev
                "data_stale_hours": 1,        # Refresh often in dev
            },
            "staging": {
                "realm": os.environ.get("QB_REALM", "staging-realm.quickbase.com"),
                "token": os.environ.get("QB_USER_TOKEN"),
                "cache_root": Path("./.quickbase-cache/staging"),
                "s3_bucket": None,  # Local
                "metadata_stale_hours": 168,  # 7 days
                "data_stale_hours": 24,       # 1 day
            },
            "prod": {
                "realm": os.environ.get("QB_REALM", "prod-realm.quickbase.com"),
                "token": os.environ.get("QB_USER_TOKEN"),
                "cache_root": Path("/tmp/quickbase-cache"),
                "s3_bucket": os.environ.get("CACHE_BUCKET"),
                "s3_prefix": "prod/cache",
                "metadata_stale_hours": 720,  # 30 days (rarely changes)
                "data_stale_hours": 24,       # 1 day
            }
        }
        return config[self.env]

    def get_client(self):
        """Get Quickbase API client."""
        if not self.config["token"]:
            raise ValueError(f"QB_USER_TOKEN not set for {self.env}")

        return get_qb_client(
            realm=self.config["realm"],
            user_token=self.config["token"]
        )

    def get_cache_manager(self):
        """Get cache manager."""
        return CacheManager(
            cache_root=self.config["cache_root"],
            s3_bucket=self.config.get("s3_bucket"),
            s3_prefix=self.config.get("s3_prefix")
        )

    def get_reports_config(self):
        """Get reports configuration for this environment."""
        return {
            "all": get_all_reports(),
            "to_cache": get_reports_to_cache(),
            "metadata_stale_hours": self.config["metadata_stale_hours"],
            "data_stale_hours": self.config["data_stale_hours"],
        }

    def display_config(self):
        """Display current configuration (useful for debugging)."""
        print(f"Environment: {self.env}")
        print(f"Realm: {self.config['realm']}")
        print(f"Cache Root: {self.config['cache_root']}")
        print(f"S3 Bucket: {self.config.get('s3_bucket', 'None (local)')}")

# Usage
config = QuickbaseConfig(env="prod")
config.display_config()

client = config.get_client()
cache_mgr = config.get_cache_manager()
reports_cfg = config.get_reports_config()

ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=reports_cfg["all"],
    report_configs_to_cache=reports_cfg["to_cache"],
    metadata_stale_hours=reports_cfg["metadata_stale_hours"],
    data_stale_hours=reports_cfg["data_stale_hours"],
)
```

### Data Transformation Pipeline

```python
from quickbase_extract.report_data import get_data
from typing import List, Dict, Callable, Any

class DataPipeline:
    """Reusable pipeline for transforming Quickbase data.

    Chains multiple transformation functions together.
    """

    def __init__(self, client, cache_mgr, metadata):
        self.client = client
        self.cache_mgr = cache_mgr
        self.metadata = metadata
        self.transformers: List[Callable] = []

    def add_transformer(self, func: Callable) -> "DataPipeline":
        """Add a transformation function to the pipeline.

        Transformation functions should accept and return a list of dicts.
        """
        self.transformers.append(func)
        return self  # Enable chaining

    def execute(self, report_config) -> List[Dict[str, Any]]:
        """Execute pipeline for a report.

        Fetches data and applies all transformations in sequence.
        """
        # Fetch data
        data = get_data(self.client, self.cache_mgr, report_config, self.metadata)

        # Apply transformations
        for transformer in self.transformers:
            data = transformer(data)

        return data


# Example transformation functions
def filter_by_status(status_value):
    """Factory function: returns a filter for specific status."""
    def filter_fn(data):
        return [r for r in data if r.get("Status") == status_value]
    return filter_fn

def add_full_name(data):
    """Add computed full name field."""
    for record in data:
        first = record.get("First Name", "").strip()
        last = record.get("Last Name", "").strip()
        record["Full Name"] = f"{first} {last}".strip()
    return data

def add_age(data):
    """Add computed age from birth date."""
    from datetime import date
    for record in data:
        birth_date_str = record.get("Birth Date")
        if birth_date_str:
            try:
                from dateutil import parser
                birth_date = parser.parse(birth_date_str).date()
                today = date.today()
                age = today.year - birth_date.year - (
                    (today.month, today.day) < (birth_date.month, birth_date.day)
                )
                record["Age"] = age
            except (ValueError, AttributeError):
                record["Age"] = None
    return data

def convert_currency_to_float(fields):
    """Factory: convert currency fields to float."""
    def converter(data):
        for record in data:
            for field in fields:
                if field in record and record[field]:
                    try:
                        # Remove $ and commas, convert to float
                        value_str = str(record[field]).replace("$", "").replace(",", "")
                        record[field] = float(value_str)
                    except (ValueError, AttributeError):
                        record[field] = None
        return data
    return converter

def log_summary(data):
    """Log summary statistics."""
    print(f"Pipeline processed {len(data)} records")
    return data


# Usage examples

# Example 1: Simple pipeline
from config.reports import get_report

pipeline = (
    DataPipeline(client, cache_mgr, metadata)
    .add_transformer(filter_by_status("Active"))
    .add_transformer(add_full_name)
    .add_transformer(log_summary)
)

active_customers = pipeline.execute(get_report("customers"))
# Output: Pipeline processed 1250 records

# Example 2: Complex pipeline with computed fields
pipeline = (
    DataPipeline(client, cache_mgr, metadata)
    .add_transformer(filter_by_status("Active"))
    .add_transformer(add_full_name)
    .add_transformer(add_age)
    .add_transformer(convert_currency_to_float(["Invoice Total", "Payment Amount"]))
    .add_transformer(log_summary)
)

processed_customers = pipeline.execute(get_report("customers"))

# Example 3: Reuse same pipeline on different reports
pipeline = (
    DataPipeline(client, cache_mgr, metadata)
    .add_transformer(add_full_name)
    .add_transformer(log_summary)
)

# Use on multiple reports
customers = pipeline.execute(get_report("customers"))
contacts = pipeline.execute(get_report("contacts"))
leads = pipeline.execute(get_report("leads"))
```

### Advanced Filtering with Complex Conditions

```python
def build_query_filter(*conditions):
    """Build a Quickbase query filter from multiple conditions.

    Makes it easier to construct complex filters programmatically.
    """
    if not conditions:
        return ""
    if len(conditions) == 1:
        return conditions[0]
    return "({})".format("AND".join(f"({c})" for c in conditions))

def fetch_orders_with_complex_filter(client, cache_mgr, metadata, **kwargs):
    """Fetch orders with complex dynamic filters.

    Example kwargs:
    - status="Pending"
    - min_amount=100
    - max_amount=10000
    - date_after="2024-01-01"
    """
    conditions = []

    # Build conditions based on kwargs
    if "status" in kwargs:
        conditions.append(f"{{15.EX.'{kwargs['status_value']}'}}")

    if "min_amount" in kwargs:
        conditions.append(f"{{8.GTE.{kwargs['min_amount']}}}")

    if "max_amount" in kwargs:
        conditions.append(f"{{8.LTE.{kwargs['max_amount']}}}")

    if "date_after" in kwargs:
        conditions.append(f"{{12.GT.'{kwargs['date_after']}'}}")

    where_clause = build_query_filter(*conditions) if conditions else ""

    # Use ask_values if asking for user input
    ask_values = kwargs.get("ask_values")

    config = get_report("orders")
    return get_data(
        client,
        cache_mgr,
        config,
        metadata,
        ask_values=ask_values
    )

# Usage
orders = fetch_orders_with_complex_filter(
    client,
    cache_mgr,
    metadata,
    status="Pending",
    min_amount=100,
    max_amount=10000,
    date_after="2024-01-01"
)
```

## Performance Optimization

### Benchmarking Your Setup

Test different configurations to find optimal settings for your environment.

```python
import time
import logging
from quickbase_extract.report_data import get_data_parallel
from config.reports import get_reports_to_cache

logger = logging.getLogger(__name__)

def benchmark_parallel_workers(client, cache_mgr, metadata, worker_counts):
    """Test different worker counts to find optimal parallel performance.

    Helps you find the sweet spot between concurrency and rate limits.
    """
    results = {}
    reports = get_reports_to_cache()

    for workers in worker_counts:
        logger.info(f"Testing with {workers} workers...")

        start = time.time()
        data = get_data_parallel(
            client,
            cache_mgr,
            reports,
            metadata,
            max_workers=workers,
            cache=False  # Don't cache for consistent timing
        )
        elapsed = time.time() - start

        total_records = sum(len(d) for d in data.values())
        throughput = total_records / elapsed if elapsed > 0 else 0

        results[workers] = {
            "time_seconds": elapsed,
            "total_records": total_records,
            "records_per_second": throughput
        }

        logger.info(
            f"  Workers={workers}: {elapsed:.2f}s, "
            f"{total_records} records, {throughput:.0f} rec/sec"
        )

    # Find optimal
    optimal_workers = max(results.items(), key=lambda x: x[1]["records_per_second"])
    logger.info(
        f"Optimal configuration: {optimal_workers[0]} workers "
        f"({optimal_workers[1]['records_per_second']:.0f} records/sec)"
    )

    return results

# Usage
results = benchmark_parallel_workers(
    client,
    cache_mgr,
    metadata,
    worker_counts=[2, 4, 8, 16, 32]
)

# Output:
# Testing with 2 workers... Workers=2: 45.23s, 10000 records, 221 rec/sec
# Testing with 4 workers... Workers=4: 24.15s, 10000 records, 414 rec/sec
# Testing with 8 workers... Workers=8: 15.67s, 10000 records, 638 rec/sec
# Testing with 16 workers... Workers=16: 14.32s, 10000 records, 698 rec/sec
# Testing with 32 workers... Workers=32: 45.21s, 10000 records, 221 rec/sec (rate limited!)
# Optimal configuration: 16 workers (698 records/sec)
```

### Optimizing Worker Count

**Guidelines for choosing `max_workers`:**

| Environment | Recommended | Reason |
|-------------|-------------|--------|
| Local development | 4-8 | Balance speed with resource usage |
| Lambda (512 MB) | 4 | Limited concurrent connections |
| Lambda (3008 MB) | 8-16 | More resources available |
| Server (8+ cores) | 16-32 | Can handle high concurrency |

**Signs you're using too many workers:**
- 429 (rate limit) errors in logs
- High latency/timeouts
- Increased CPU usage on client

**Solution:** Reduce `max_workers` or increase `max_retries`:

```python
# Conservative (safe for rate limits)
data = get_data_parallel(
    client,
    cache_mgr,
    reports,
    metadata,
    max_workers=4
)

# Aggressive (faster but may hit rate limits)
data = get_data_parallel(
    client,
    cache_mgr,
    reports,
    metadata,
    max_workers=16
)

# With retry buffer for rate limits
from quickbase_extract.api_handlers import handle_query

result = handle_query(
    client,
    table_id,
    max_retries=5  # More attempts for rate limit resilience
)
```

### Memory-Efficient Processing for Large Datasets

For datasets with thousands of records, stream to file instead of loading all in memory.

```python
import json
from quickbase_extract.api_handlers import handle_query

def stream_large_dataset_to_file(client, table_id, output_file, chunk_size=1000):
    """Stream large Quickbase dataset directly to file.

    Memory usage stays constant regardless of dataset size.
    """
    skip = 0
    total_records = 0

    with open(output_file, 'w') as f:
        f.write('[\n')
        first = True

        while True:
            # Fetch chunk
            result = handle_query(
                client,
                table_id,
                options={
                    "skip": skip,
                    "top": chunk_size
                },
                description=f"chunk at offset {skip}"
            )

            records = result.get("data", [])
            if not records:
                break

            # Write chunk
            if not first:
                f.write(',\n')

            # Write records without outer brackets
            f.write(json.dumps(records)[1:-1])
            first = False

            total_records += len(records)
            print(f"Streamed {total_records} records...")

            if len(records) < chunk_size:
                break  # Last chunk

            skip += chunk_size

        f.write('\n]')

    print(f"Complete: {total_records} records streamed to {output_file}")
    return total_records

# Usage
stream_large_dataset_to_file(
    client,
    "tblXYZ123",
    "large_dataset.json",
    chunk_size=5000  # Larger chunks = fewer API calls
)
```

### Caching Strategies

#### Strategy 1: Cache-First with TTL

Check cache first, only fetch fresh data if stale.

```python
from quickbase_extract.report_data import get_data, load_data
from datetime import datetime, timedelta

def get_data_with_ttl(
    client,
    cache_mgr,
    report_config,
    metadata,
    ttl_hours=24
):
    """Get data from cache if fresh, otherwise fetch new.

    Combines cache efficiency with data freshness control.
    """
    info = metadata[report_config]

    try:
        # Try to load from cache
        cached_data = load_data(cache_mgr, report_config, metadata)

        # Check age
        cache_path = cache_mgr.get_data_path(
            info["app_name"],
            info["table_name"],
            info["report_name"]
        )

        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime

        if age < timedelta(hours=ttl_hours):
            logger.info(f"Using cached data ({age.seconds / 3600:.1f}h old)")
            return cached_data

        logger.info(f"Cache stale ({age.seconds / 3600:.1f}h > {ttl_hours}h), fetching fresh")

    except FileNotFoundError:
        logger.info("Cache miss, fetching fresh data")

    # Cache miss or stale, fetch new
    return get_data(
        client,
        cache_mgr,
        report_config,
        metadata,
        cache=True
    )

# Usage
customers = get_data_with_ttl(
    client,
    cache_mgr,
    report_config,
    metadata,
    ttl_hours=6  # Refresh every 6 hours
)
```

#### Strategy 2: Batch Caching with Smart Subset

Cache frequently-used reports aggressively, rarely-used reports only on demand.

```python
def get_reports_smart_cache():
    """Return reports to cache based on usage patterns.

    Cache critical/frequently-used reports.
    Load rarely-used reports on demand.
    """
    from config.reports import _TAGGED_REPORTS

    # Cache reports tagged as "critical"
    cached = []
    for tagged_cfg in _TAGGED_REPORTS.values():
        if "critical" in tagged_cfg.tags:
            cached.append(tagged_cfg.config)

    return cached

# Usage
ensure_cache_freshness(
    client=client,
    cache_manager=cache_mgr,
    report_configs_all=get_all_reports(),
    report_configs_to_cache=get_reports_smart_cache(),  # Only critical
)

# For rarely-used reports, fetch on demand (not cached)
rare_data = get_data(
    client,
    cache_mgr,
    rare_config,
    metadata,
    cache=False  # Don't cache rarely-used reports
)
```

### Lambda Cold Start Optimization

Minimize cold start time by:

1. **Reuse clients across warm starts** (shown above with global `_client`)
2. **Lazy-load dependencies** where possible
3. **Use Lambda layers** for common code
4. **Increase Lambda memory** (more CPU = faster execution)

```python
# Good: Global clients (reused on warm starts)
_client = None
_cache_mgr = None

def get_client():
    global _client
    if _client is None:
        _client = quickbase_api.client(...)  # Only created on cold start
    return _client

# Lambda performance by memory allocation
# 128 MB: ~40 seconds cold start
# 512 MB: ~8 seconds cold start
# 1024 MB: ~4 seconds cold start (recommended minimum)
# 3008 MB: ~2 seconds cold start
```

### Query Optimization

#### Limit Fields to What You Need

```python
from quickbase_extract.api_handlers import handle_query

# Bad: Returns all fields (larger payload, slower)
result = handle_query(client, table_id)

# Good: Only fetch needed fields
result = handle_query(
    client,
    table_id,
    select=[3, 6, 7, 8],  # Only these fields
)
```

#### Use Filters to Reduce Rows

```python
# Bad: Fetch all records, filter in code
all_data = handle_query(client, table_id)
active = [r for r in all_data if r.get("Status") == "Active"]

# Good: Filter at source (smaller payload, faster)
result = handle_query(
    client,
    table_id,
    where="{8.EX.'Active'}",  # Filter at Quickbase
)
active = result["data"]
```

#### Use Pagination for Large Results

```python
# Process large datasets in chunks
def fetch_all_records_paginated(client, table_id, chunk_size=1000):
    """Fetch all records with pagination."""
    skip = 0
    all_records = []

    while True:
        result = handle_query(
            client,
            table_id,
            options={
                "skip": skip,
                "top": chunk_size
            }
        )

        records = result.get("data", [])
        if not records:
            break

        all_records.extend(records)
        skip += chunk_size

        if len(records) < chunk_size:
            break

    return all_records
```

### Performance Monitoring

```python
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def time_operation(operation_name):
    """Decorator to log operation timing."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start

            logger.info(f"{operation_name}: {elapsed:.2f}s")
            return result
        return wrapper
    return decorator

# Usage
@time_operation("Metadata Load")
def load_all_metadata(cache_mgr, config):
    from quickbase_extract.report_metadata import load_report_metadata_batch
    return load_report_metadata_batch(cache_mgr, config)

@time_operation("Data Fetch")
def fetch_all_data(client, cache_mgr, config, metadata):
    from quickbase_extract.report_data import get_data_parallel
    return get_data_parallel(client, cache_mgr, config, metadata)

# Logged output:
# Metadata Load: 0.23s
# Data Fetch: 8.45s
```

### Estimated Performance

**Typical timings (rough estimates):**

| Operation | Time |
|-----------|------|
| Metadata refresh (10 reports) | 5-15 seconds |
| Metadata load from cache | 0.1-0.5 seconds |
| Data fetch (100 records) | 2-5 seconds per report |
| Data fetch parallel (4 reports) | 3-8 seconds total |
| Data load from cache | 0.5-2 seconds |

**Factors affecting performance:**
- Network latency to Quickbase
- Number of fields/records
- Number of concurrent requests
- Quickbase API rate limits
- Lambda cold start vs warm start

## Real-World Use Cases

### Use Case 1: Daily Sales Report

Generate and email a daily sales report with key metrics.

```python
from quickbase_extract.report_data import get_data
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
import logging

logger = logging.getLogger(__name__)

def generate_daily_sales_report(client, cache_mgr, metadata):
    """Generate and email daily sales report.

    Fetches today's orders, calculates metrics, and emails to sales team.
    """
    from config.reports import get_report

    # Ensure cache is fresh
    ensure_cache_freshness(
        client=client,
        cache_manager=cache_mgr,
        report_configs_all=get_all_reports(),
        report_configs_to_cache=get_reports_to_cache(),
    )

    # Fetch today's data
    today = datetime.now().strftime("%Y-%m-%d")
    orders_config = get_report("orders")

    # Use ask placeholder for date filtering
    orders = get_data(
        client,
        cache_mgr,
        orders_config,
        metadata,
        ask_values={"ask1": today}  # Filter to today only
    )

    # Calculate metrics
    total_orders = len(orders)
    total_revenue = sum(
        float(o.get("Order Total", 0)) for o in orders if o.get("Order Total")
    )
    average_order = total_revenue / total_orders if total_orders > 0 else 0

    # Find top orders
    top_orders = sorted(
        orders,
        key=lambda x: float(x.get("Order Total", 0)),
        reverse=True
    )[:5]

    # Generate email body
    report_body = f"""
Daily Sales Report - {today}
================================

Total Orders: {total_orders}
Total Revenue: ${total_revenue:,.2f}
Average Order Value: ${average_order:,.2f}

Top 5 Orders:
"""

    for i, order in enumerate(top_orders, 1):
        customer = order.get("Customer Name", "Unknown")
        amount = order.get("Order Total", "N/A")
        report_body += f"\n{i}. {customer}: ${amount}"

    # Send email
    send_email(
        to_email="sales-team@company.com",
        subject=f"Daily Sales Report - {today}",
        body=report_body
    )

    logger.info(f"Daily report sent: {total_orders} orders, ${total_revenue:,.2f} revenue")

    return {
        "date": today,
        "orders": total_orders,
        "revenue": total_revenue
    }

def send_email(to_email, subject, body):
    """Send email report (configure SMTP settings)."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "quickbase-bot@company.com"
    msg['To'] = to_email

    # Configure SMTP server
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login("your-email@gmail.com", "your-app-password")
        server.send_message(msg)

# Lambda handler
def lambda_handler(event, context):
    """Daily report Lambda trigger."""
    from quickbase_extract import get_qb_client, CacheManager, sync_from_s3_once
    from config.reports import get_all_reports, get_reports_to_cache
    import os
    from pathlib import Path

    client = get_qb_client(
        realm=os.environ["QB_REALM"],
        user_token=os.environ["QB_USER_TOKEN"]
    )

    cache_mgr = CacheManager(
        cache_root=Path("/tmp/cache"),
        s3_bucket=os.environ.get("CACHE_BUCKET"),
        s3_prefix="cache"
    )

    sync_from_s3_once(cache_mgr)

    from quickbase_extract.report_metadata import load_report_metadata_batch
    metadata = load_report_metadata_batch(cache_mgr, get_all_reports())

    result = generate_daily_sales_report(client, cache_mgr, metadata)

    return {
        "statusCode": 200,
        "body": f"Report generated: {result['orders']} orders"
    }
```

### Use Case 2: Data Warehouse ETL

Sync Quickbase data to PostgreSQL data warehouse with transformation.

```python
from quickbase_extract.report_data import get_data_parallel
from quickbase_extract.cache_orchestration import ensure_cache_freshness
from quickbase_extract.report_metadata import load_report_metadata_batch
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import logging

logger = logging.getLogger(__name__)

class QuickbaseToPostgresETL:
    """ETL pipeline: Quickbase → PostgreSQL data warehouse.

    Extracts data from Quickbase, transforms it, and loads into warehouse.
    """

    def __init__(self, qb_client, cache_mgr, metadata, pg_conn_string):
        self.qb_client = qb_client
        self.cache_mgr = cache_mgr
        self.metadata = metadata
        self.pg_conn = psycopg2.connect(pg_conn_string)
        self.warehouse_schema = "quickbase_warehouse"

    def extract(self, report_config):
        """Extract data from Quickbase.

        Always fetches fresh data (no caching) for ETL.
        """
        logger.info(f"Extracting {len(report_config)} reports from Quickbase...")

        return get_data_parallel(
            self.qb_client,
            self.cache_mgr,
            report_config,
            self.metadata,
            cache=False  # Always fresh for ETL
        )

    def transform(self, data):
        """Transform data for warehouse schema.

        - Normalize field names
        - Convert data types
        - Add metadata (source, timestamp)
        """
        logger.info("Transforming data...")

        transformed = {}

        for config, records in data.items():
            table_name = config.table_name.lower().replace(" ", "_")
            cleaned = []

            for record in records:
                # Normalize field names: "First Name" -> "first_name"
                cleaned_record = {
                    k.lower().replace(" ", "_"): v
                    for k, v in record.items()
                }

                # Add metadata columns
                cleaned_record["_source"] = f"{config.app_name}_{config.table_name}"
                cleaned_record["_extracted_at"] = datetime.now().isoformat()
                cleaned_record["_loaded_at"] = None  # Filled on load

                cleaned.append(cleaned_record)

            transformed[table_name] = cleaned
            logger.info(f"  {table_name}: {len(cleaned)} records")

        return transformed

    def load(self, data):
        """Load data into PostgreSQL warehouse.

        - Creates schema if needed
        - Truncates existing tables
        - Bulk inserts data
        - Updates load timestamp
        """
        logger.info("Loading data to warehouse...")
        cursor = self.pg_conn.cursor()

        try:
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.warehouse_schema};")

            for table_name, records in data.items():
                if not records:
                    logger.info(f"  {table_name}: no records, skipping")
                    continue

                # Drop and recreate table
                full_table = f"{self.warehouse_schema}.{table_name}"
                cursor.execute(f"DROP TABLE IF EXISTS {full_table};")

                # Create table from first record's schema
                columns = list(records[0].keys())
                col_defs = ", ".join([f'"{col}" TEXT' for col in columns])
                cursor.execute(f"CREATE TABLE {full_table} ({col_defs});")

                # Bulk insert
                placeholders = ", ".join(["%s" for _ in columns])
                insert_sql = f"""
                    INSERT INTO {full_table} ({", ".join([f'"{c}"' for c in columns])})
                    VALUES ({placeholders})
                """

                rows = [
                    [record.get(col) for col in columns]
                    for record in records
                ]

                execute_batch(cursor, insert_sql, rows, page_size=1000)

                self.pg_conn.commit()
                logger.info(f"  {table_name}: loaded {len(records)} records")

        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"ETL load failed: {e}")
            raise
        finally:
            cursor.close()

    def run(self, report_config):
        """Run full ETL pipeline: Extract → Transform → Load."""
        logger.info("Starting ETL pipeline...")

        # Extract
        data = self.extract(report_config)

        # Transform
        transformed = self.transform(data)

        # Load
        self.load(transformed)

        logger.info("ETL pipeline complete!")

        return {
            "tables_loaded": len(transformed),
            "total_records": sum(len(records) for records in transformed.values()),
            "timestamp": datetime.now().isoformat()
        }

# Usage
def run_etl(client, cache_mgr, metadata, pg_conn_string):
    """Execute full ETL pipeline."""
    from config.reports import get_reports_to_cache

    etl = QuickbaseToPostgresETL(
        qb_client=client,
        cache_mgr=cache_mgr,
        metadata=metadata,
        pg_conn_string="postgresql://user:pass@localhost/warehouse"
    )

    result = etl.run(get_reports_to_cache())

    print(f"Loaded {result['tables_loaded']} tables, {result['total_records']} records")
    return result
```

### Use Case 3: Automated Data Quality Checks

Validate data quality and flag issues for investigation.

```python
from quickbase_extract.report_data import get_data
from datetime import datetime
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Run data quality checks on Quickbase data.

    Validates required fields, duplicates, value ranges, and data types.
    """

    def __init__(self, client, cache_mgr, metadata):
        self.client = client
        self.cache_mgr = cache_mgr
        self.metadata = metadata
        self.issues: List[Dict[str, Any]] = []

    def check_required_fields(self, report_config, required_fields: List[str]):
        """Check that required fields are not empty."""
        logger.info(f"Checking required fields: {required_fields}")

        data = get_data(self.client, self.cache_mgr, report_config, self.metadata)

        for i, record in enumerate(data):
            for field in required_fields:
                value = record.get(field)
                if not value or (isinstance(value, str) and not value.strip()):
                    self.issues.append({
                        "type": "missing_required_field",
                        "report": report_config.table_name,
                        "record_index": i,
                        "record_id": record.get("Record ID#"),
                        "field": field,
                        "severity": "ERROR"
                    })

    def check_duplicates(self, report_config, unique_field: str):
        """Check for duplicate values in unique fields."""
        logger.info(f"Checking duplicates in: {unique_field}")

        data = get_data(self.client, self.cache_mgr, report_config, self.metadata)

        seen = {}
        for i, record in enumerate(data):
            value = record.get(unique_field)
            if value:
                if value in seen:
                    self.issues.append({
                        "type": "duplicate_value",
                        "report": report_config.table_name,
                        "record_index": i,
                        "record_id": record.get("Record ID#"),
                        "field": unique_field,
                        "value": value,
                        "first_occurrence": seen[value],
                        "severity": "WARNING"
                    })
                else:
                    seen[value] = i

    def check_value_range(self, report_config, field: str, min_val=None, max_val=None):
        """Check that numeric values are within expected range."""
        logger.info(f"Checking value range for {field}: [{min_val}, {max_val}]")

        data = get_data(self.client, self.cache_mgr, report_config, self.metadata)

        for i, record in enumerate(data):
            value = record.get(field)
            if value is not None and value != "":
                try:
                    num_value = float(str(value).replace("$", "").replace(",", ""))

                    if min_val is not None and num_value < min_val:
                        self.issues.append({
                            "type": "value_below_minimum",
                            "report": report_config.table_name,
                            "record_index": i,
                            "record_id": record.get("Record ID#"),
                            "field": field,
                            "value": num_value,
                            "minimum": min_val,
                            "severity": "WARNING"
                        })

                    if max_val is not None and num_value > max_val:
                        self.issues.append({
                            "type": "value_above_maximum",
                            "report": report_config.table_name,
                            "record_index": i,
                            "record_id": record.get("Record ID#"),
                            "field": field,
                            "value": num_value,
                            "maximum": max_val,
                            "severity": "WARNING"
                        })

                except (ValueError, AttributeError):
                    self.issues.append({
                        "type": "invalid_data_type",
                        "report": report_config.table_name,
                        "record_index": i,
                        "record_id": record.get("Record ID#"),
                        "field": field,
                        "value": value,
                        "expected_type": "numeric",
                        "severity": "ERROR"
                    })

    def check_date_format(self, report_config, date_fields: List[str]):
        """Check that date fields are valid dates."""
        from dateutil import parser

        logger.info(f"Checking date format: {date_fields}")

        data = get_data(self.client, self.cache_mgr, report_config, self.metadata)

        for i, record in enumerate(data):
            for field in date_fields:
                value = record.get(field)
                if value and value != "":
                    try:
                        parser.parse(value)  # Validate date
                    except (ValueError, AttributeError):
                        self.issues.append({
                            "type": "invalid_date_format",
                            "report": report_config.table_name,
                            "record_index": i,
                            "record_id": record.get("Record ID#"),
                            "field": field,
                            "value": value,
                            "severity": "ERROR"
                        })

    def generate_report(self) -> str:
        """Generate data quality report."""
        if not self.issues:
            return "✓ All data quality checks passed! No issues found."

        # Group by severity
        errors = [i for i in self.issues if i["severity"] == "ERROR"]
        warnings = [i for i in self.issues if i["severity"] == "WARNING"]

        report = f"Data Quality Report - {datetime.now().isoformat()}\n"
        report += f"{'='*60}\n\n"

        report += f"Total Issues: {len(self.issues)}\n"
        report += f"  - Errors: {len(errors)}\n"
        report += f"  - Warnings: {len(warnings)}\n\n"

        if errors:
            report += "ERRORS (must fix):\n"
            report += "-" * 60 + "\n"
            for issue in errors:
                report += self._format_issue(issue) + "\n"
            report += "\n"

        if warnings:
            report += "WARNINGS (review):\n"
            report += "-" * 60 + "\n"
            for issue in warnings:
                report += self._format_issue(issue) + "\n"

        return report

    def _format_issue(self, issue: Dict[str, Any]) -> str:
        """Format single issue for display."""
        parts = [
            f"  Record {issue['record_id']} ({issue['report']})",
            f"  Type: {issue['type']}",
        ]

        if "field" in issue:
            parts.append(f"  Field: {issue['field']}")

        if "value" in issue:
            parts.append(f"  Value: {issue['value']}")

        if "minimum" in issue:
            parts.append(f"  Minimum allowed: {issue['minimum']}")

        if "maximum" in issue:
            parts.append(f"  Maximum allowed: {issue['maximum']}")

        return "\n".join(parts)

# Usage
def run_quality_checks(client, cache_mgr, metadata):
    """Execute data quality checks."""
    from config.reports import get_report

    checker = DataQualityChecker(client, cache_mgr, metadata)

    # Run checks on customers report
    customer_config = get_report("customers_active")
    checker.check_required_fields(
        customer_config,
        ["Name", "Email", "Phone"]
    )
    checker.check_duplicates(customer_config, "Email")
    checker.check_value_range(
        customer_config,
        "Annual Revenue",
        min_val=0,
        max_val=100000000
    )
    checker.check_date_format(customer_config, ["Created Date", "Last Modified"])

    # Generate report
    report = checker.generate_report()
    print(report)

    # Save report to file
    with open("quality_report.txt", "w") as f:
        f.write(report)

    # Email if there are errors
    if any(i["severity"] == "ERROR" for i in checker.issues):
        send_quality_alert(report)

    return checker.issues

def send_quality_alert(report: str):
    """Send alert email if data quality issues found."""
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(report)
    msg['Subject'] = "DATA QUALITY ALERT: Critical Issues Found"
    msg['From'] = "quickbase-bot@company.com"
    msg['To'] = "data-team@company.com"

    # Send email (configure SMTP)
    # ...
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
