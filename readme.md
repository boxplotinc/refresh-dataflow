# Incrementally Refresh Dataflow

**Advanced incremental refresh framework for Microsoft Fabric dataflows with intelligent bucketing, automatic retry mechanisms, and CI/CD support.**

![Python](https://img.shields.io/badge/Python-3.8%2B-blue) ![License](https://img.shields.io/badge/License-MIT-green) ![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-orange) ![Dataflow](https://img.shields.io/badge/Dataflow-Gen2-brightgreen)

---

## Table of Contents

- [Why This Tool?](#why-this-tool)
- [Use Cases](#use-cases)
- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Quick Start Example](#quick-start-example)
- [How It Works](#how-it-works)
- [Pipeline Parameters](#pipeline-parameters)
- [Architecture](#architecture)
- [Integration with Dataflow](#integration-with-dataflow-power-query)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Data Safety (Backup & Restore)](#7-data-safety-backup--restore)
- [Ad-hoc Extract Mode](#8-ad-hoc-extract-mode)
- [Version History](#version-history)

---

## Why This Tool?

Microsoft Fabric's standard incremental refresh capabilities have limitations that prevent effective data loading in certain scenarios:

**Common Problems:**
- **Query Folding Issues**: Many data sources don't support query folding, causing full table scans instead of filtered queries
- **Bucket Size Constraints**: Large historical datasets exceed memory or timeout limits when loaded in single operations
- **Limited Error Handling**: Transient failures (network issues, temporary service unavailability) cause entire refresh operations to fail
- **Manual Date Range Management**: No built-in mechanism to automatically track and manage incremental date ranges

**How This Framework Solves These Issues:**
- ✅ **Custom Bucketing Logic**: Splits large date ranges into configurable buckets to manage memory and timeout constraints
- ✅ **Intelligent Retry Mechanism**: Automatically retries failed buckets with exponential backoff before marking operations as failed
- ✅ **Automated Date Range Tracking**: Maintains metadata in warehouse tracking tables to manage incremental loads without manual intervention
- ✅ **CI/CD & Regular Dataflow Support**: Auto-detects dataflow type and uses appropriate Microsoft Fabric REST API endpoints
- ✅ **Pipeline Integration**: Proper failure codes ensure seamless integration with Fabric pipeline error handling and alerting

This solution is ideal for data engineers working with Microsoft Fabric who need robust, production-ready incremental refresh capabilities beyond what's available out of the box.

---

## Use Cases

This framework excels in the following scenarios:

- **Historical Data Loading**: Loading years of historical data from APIs or databases with date-range filtering
- **Non-Folding Data Sources**: Refreshing data from sources that don't support Power Query query folding (REST APIs, custom connectors, certain databases)
- **Large Dataset Management**: Managing datasets that exceed single-operation memory or timeout limits by processing in smaller buckets
- **Resilient Data Pipelines**: Building production pipelines that can recover from transient failures without manual intervention
- **Retroactive Updates**: Handling data sources where historical records can be updated by using configurable overlap windows
- **Multi-Environment Deployments**: Supporting both development (CI/CD dataflows) and production (regular dataflows) with automatic detection
- **Automated Scheduling**: Integrating with Fabric pipelines for scheduled, hands-off incremental refresh operations

---

## Prerequisites

Before using this notebook, ensure you have the following:

### Required Access & Permissions
- **Microsoft Fabric Workspace**: Contributor or Admin role in the target workspace
- **Warehouse Permissions**: Read/Write access to the warehouse where tracking tables will be created
- **Dataflow Permissions**: Permission to trigger and monitor dataflow refreshes

### Technical Requirements
- **Microsoft Fabric Capacity**: Active Fabric capacity (F2 or higher recommended)
- **Python Environment**: Fabric notebook environment with the following packages:
  - `pandas` - Data manipulation
  - `sempy.fabric` - Microsoft Fabric API client
  - `notebookutils.data` - Fabric data connection utilities
- **Dataflow Gen2**: Existing dataflow configured in Microsoft Fabric

### Knowledge Prerequisites
- Basic understanding of Microsoft Fabric dataflows
- Familiarity with incremental refresh concepts
- SQL query knowledge for troubleshooting
- Understanding of Power Query M language for dataflow integration

## Overview

This notebook implements an advanced framework for incremental data refresh in Microsoft Fabric dataflows. It is designed to handle scenarios where standard incremental refresh does not work due to limitations in the data source, such as query folding issues or bucket size constraints.

The notebook supports both **regular Dataflow Gen2** and **CI/CD Dataflow Gen2** objects, automatically detecting the dataflow type and using the appropriate Microsoft Fabric REST API endpoints.

## Architecture

The notebook orchestrates a coordinated refresh process across multiple Fabric components:

```
┌─────────────┐
│   Pipeline  │ (Triggers notebook with parameters)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              Notebook (DataflowRefresher)               │
│  ┌────────────────────────────────────────────────────┐ │
│  │ 1. Read tracking table from Warehouse              │ │
│  │ 2. Recover from interrupted runs (if needed)       │ │
│  │ 3. Calculate date ranges & buckets                 │ │
│  │ 4. Backup data in affected range                   │ │
│  │ 5. Delete overlapping data from destination table  │ │
│  │ 6. Update tracking table (Running status)          │ │
│  │ 7. Call Fabric REST API to trigger dataflow        │ │
│  │    (passes RangeStart/RangeEnd if supported)      │ │
│  │ 8. Poll for completion status                      │ │
│  │ 9. On success: drop backup, update tracking table  │ │
│  │    On failure: restore from backup, retry or fail  │ │
│  └────────────────────────────────────────────────────┘ │
└─────────┬───────────────────────────────────┬───────────┘
          │                                   │
          ▼                                   ▼
┌─────────────────────┐           ┌─────────────────────┐
│   Warehouse         │           │  Fabric REST API    │
│                     │           │  - Regular DF:      │
│  [Incremental       │           │    /dataflows/...   │
│   Update] Table     │           │  - CI/CD DF:        │
│  - range_start      │           │    /items/.../jobs  │
│  - range_end        │           └──────────┬──────────┘
│  - status           │                      │
│                     │                      ▼
│  [temporary_backup] │           ┌───────────────────┐
│  (temporary backup  │◄─────────>│   Dataflow Gen2   │
│   during refresh)   │           │  (Power Query M)  │
└─────────┬───────────┘           └─────────┬─────────┘
          │                                 │
          │                                 ▼
          │                       ┌───────────────────┐
          └──────────────────────>│  Data Source      │
            (Dataflow reads range)│  (Filtered by     │
                                  │   date range)     │
                                  └─────────┬─────────┘
                                            │
                                            ▼
                                  ┌───────────────────┐
                                  │   Warehouse       │
                                  │   Destination     │
                                  │   Table           │
                                  └───────────────────┘
```

**Key Flow:**
1. **Pipeline** passes parameters to notebook
2. **Notebook** recovers from any interrupted previous run, then manages the tracking table and orchestrates refresh
3. **Tracking table** stores date ranges (also read by dataflows without parameter support)
4. **Notebook** backs up affected data before deleting, restores on failure
5. **Dataflow** executes with filtered date range — passed directly as parameters (CI/CD with public parameters) or read from tracking table (regular dataflows)
6. **Data** flows from source to warehouse destination table

## Key Features

- **Intelligent Bucket Processing**: Splits large date ranges into configurable buckets to manage incremental loads efficiently
- **Automatic Retry Mechanism**: Retries failed bucket refreshes with exponential backoff before failing the entire process
- **Dataflow Type Auto-Detection**: Automatically detects and supports both regular and CI/CD dataflows
- **Direct Parameter Passing**: Automatically discovers and passes `RangeStart`/`RangeEnd` as public parameters to CI/CD dataflows that support them, eliminating the need for the dataflow to query the tracking table
- **Connection Management**: Handles database connections with automatic retry and reconnection logic to prevent timeout issues
- **Comprehensive Status Tracking**: Maintains detailed metadata about refresh operations in a warehouse tracking table
- **Pipeline Failure Integration**: Exits with proper failure codes to integrate with Fabric pipeline error handling
- **Data Safety (Backup & Restore)**: Automatically backs up data before deleting, restores on failure, and recovers from interrupted runs
- **Ad-hoc Extract Mode**: Load data for an arbitrary date range without disrupting incremental state

## Pipeline Parameters

Configure the following parameters when setting up the notebook activity in your Fabric pipeline:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_name` | String | No | Name of the Fabric workspace containing the dataflow. Defaults to the current workspace if omitted. The notebook resolves this to a workspace ID automatically. |
| `dataflow_name` | String | Yes | Name of the dataflow to refresh. The notebook resolves this to a dataflow ID automatically. |
| `initial_load_from_date` | String | Yes* | Start date for initial historical load (format: 'YYYY-MM-DD'). *Required only for first load |
| `bucket_size` | Integer or String | No | Size of each refresh bucket. Accepts: integer for days (e.g., `7`), or string with suffix: `"1M"` for months, `"1Y"` for years (default: 1) |
| `bucket_retry_attempts` | Integer | No | Total number of attempts per bucket, including the initial attempt (e.g., 3 = 1 initial + 2 retries; default: 3) |
| `incrementally_update_last` | Integer or String | No | Period to overlap/refresh in incremental updates. Accepts: integer for days (e.g., `1`), or string with suffix: `"3M"` for months, `"1Y"` for years (default: 1) |
| `reinitialize_dataflow` | Boolean | No | Set to True to delete tracking data and restart from scratch (default: False) |
| `destination_table` | String | Yes | Name of the destination table in the warehouse where data is written. Can be just table name (uses dbo schema) or `schema.table` format for tables in other schemas |
| `incremental_update_column` | String | Yes | DateTime column used for incremental filtering |
| `is_cicd_dataflow` | Boolean | No | Explicitly specify if this is a CI/CD dataflow (auto-detected if not provided) |
| `load_from_date` | String | No | Ad-hoc extract start date (format: 'YYYY-MM-DD'). Both `load_from_date` and `load_to_date` must be set. |
| `load_to_date` | String | No | Ad-hoc extract end date (format: 'YYYY-MM-DD'). Both `load_from_date` and `load_to_date` must be set. |
| `backup_schema` | String | No | Schema name for backup tables, created automatically if it doesn't exist (default: `temporary_backup`) |
| `skip_backup` | Boolean | No | Skip backup/restore before delete operations for faster runs. **Warning:** data is not recoverable if a refresh fails (default: False) |
| `force_run` | Boolean | No | Override the concurrency guard if a previous run appears active. Use with caution — only set if you are certain the active-looking run is stale (default: False) |
| `validate_only` | Boolean | No | Resolve IDs and print the execution plan (bucket count, date ranges, parameters) without modifying any data. Useful for verifying configuration before a live run (default: False) |
| `adhoc_fail_if_unsupported` | Boolean | No | When ad-hoc mode is requested on a dataflow that cannot consume the date range parameters, raise an error instead of silently proceeding with a best-effort reload (default: True) |
| `cicd_execute_route_mode` | String | No | CI/CD Execute route compatibility mode. `'auto'` (default) tries the documented endpoint first and falls back to the legacy Background Jobs route on `400 InvalidJobType` when no job was created. `'documented'` always uses the documented Dataflow Execute path. `'legacy_query'` always uses the legacy `?jobType=Execute` path. See [CI/CD Execute — InvalidJobType](#cicd-execute--invalidjobtype-http-400) for details. |

## Notebook Constants

The following constants must be configured inside the notebook:

| Constant | Example | Description |
|----------|---------|-------------|
| `SCHEMA` | `"[Warehouse DB].[dbo]"` | Database schema where the tracking table resides |
| `INCREMENTAL_TABLE` | `"[Incremental Update]"` | Name of the metadata tracking table |
| `CONNECTION_ARTIFACT` | `"Your Warehouse Name"` | Name of the warehouse artifact |
| `CONNECTION_ARTIFACT_ID` | `"your-warehouse-id-guid"` | Technical ID (GUID) of the **warehouse artifact** (not the workspace ID). Find it in the warehouse URL: `.../warehouses/{warehouse_id}`. The default `fabric.get_workspace_id()` is a placeholder only — this must be replaced with the actual warehouse GUID. |
| `CONNECTION_ARTIFACT_TYPE` | `"Warehouse"` | Type of artifact (typically "Warehouse") |

## Setup Instructions

Follow these steps to set up and configure the notebook:

### Step 1: Import the Notebook
1. Navigate to your Microsoft Fabric workspace
2. Click **New** → **Import notebook**
3. Upload the `Incrementally Refresh Dataflow.ipynb` file
4. Wait for the import to complete

### Step 2: Configure Notebook Constants
Open the notebook and update the following constants in the third code cell:

```python
# Update these constants with your warehouse details
SCHEMA = "[YourWarehouseName].[dbo]"
INCREMENTAL_TABLE = "[Incremental Update]"
CONNECTION_ARTIFACT = "YourWarehouseName"
CONNECTION_ARTIFACT_ID = "your-warehouse-id-guid"
CONNECTION_ARTIFACT_TYPE = "Warehouse"
```

**How to find your Warehouse ID:**
1. Open your warehouse in Fabric
2. Check the URL: `https://app.fabric.microsoft.com/groups/{workspace_id}/warehouses/{warehouse_id}`
3. Copy the `warehouse_id` GUID from the URL

### Step 3: Create a Fabric Pipeline
1. In your workspace, create a new **Data Pipeline**
2. Add a **Notebook** activity to the pipeline canvas
3. Configure the notebook activity:
   - **Notebook**: Select the imported notebook
   - **Parameters**: Add the required parameters (see Quick Start Example below)

### Step 4: Configure Your Dataflow
Ensure your dataflow Power Query includes logic to read `range_start` and `range_end` from the tracking table (see Integration section).

### Step 5: Test the Setup
1. Run the pipeline with all required parameters
2. Monitor the notebook execution in Fabric
3. Check the `[Incremental Update]` table in your warehouse to verify tracking records are created
4. Verify data appears in your destination table

### Step 6: Bootstrap Tracking Row (Recommended)

Before running the pipeline for the first time, call `bootstrap_tracking_row()` once during initial deployment to pre-seed the singleton tracking row with `status='New'`.

**Why this matters**: Fabric Warehouse constraints are `NOT ENFORCED`. If two pipeline runs start simultaneously on a fresh deployment (no prior tracking row), both will attempt to `INSERT` a new row and succeed — creating duplicate rows that violate the singleton invariant. Pre-seeding with `bootstrap_tracking_row()` avoids the first-run INSERT race when bootstrap is called before any concurrent pipeline execution begins: the runtime always finds an existing row and uses the atomic `UPDATE` acquire path, which is race-safe (only one writer wins the compare-and-swap).

**How to call it** (run once from a notebook cell or setup script):

```python
refresher.bootstrap_tracking_row(
    dataflow_id="<your-dataflow-guid>",
    workspace_id="<your-workspace-guid>",
    dataflow_name="Sales Data Incremental Refresh",
    initial_load_from_date="2024-01-01",
    bucket_size=7,
    incrementally_update_last=2,
    destination_table="FactSales",
    incremental_update_column="OrderDate",
    is_cicd_dataflow=False
)
```

This method is idempotent — serially safe to call multiple times (it is a no-op if a row already exists). Note: concurrent bootstrap calls on an empty table can still both insert; serialize deployment manually to avoid this edge case.

## Quick Start Example

Here's a complete example of how to configure the pipeline parameters for your first run:

### Example Configuration

**Pipeline Parameters:**
```json
{
  "workspace_name": "My Fabric Workspace",
  "dataflow_name": "Sales Data Incremental Refresh",
  "initial_load_from_date": "2024-01-01",
  "bucket_size": 7,
  "bucket_retry_attempts": 3,
  "incrementally_update_last": 2,
  "reinitialize_dataflow": false,
  "destination_table": "FactSales",
  "incremental_update_column": "OrderDate",
  "is_cicd_dataflow": null
}
```

**Ad-hoc mode example** -- reload June 2024 data without affecting the incremental tracking state:
```json
{
  "load_from_date": "2024-06-01",
  "load_to_date": "2024-06-30"
}
```

**Parameter Explanation:**
- `workspace_name`: Name of your Fabric workspace (omit to use the current workspace)
- `dataflow_name`: Name of the dataflow (the notebook resolves the ID internally)
- `initial_load_from_date`: Start loading data from January 1, 2024 (first run only)
- `bucket_size`: Process 7 days at a time
- `incrementally_update_last`: Overlap last 2 days on each refresh
- `destination_table`: Table name in warehouse (uses `dbo` schema by default)
- `incremental_update_column`: Date column used for filtering
- `is_cicd_dataflow`: Auto-detect dataflow type (set to `true` or `false` to override)

### Expected First Run Behavior

1. **No tracking record exists** → Initial load scenario
2. Calculates range: `2024-01-01` to `yesterday 23:59:59`
3. Splits into 7-day buckets
4. Processes each bucket sequentially
5. Creates tracking table entry
6. Dataflow reads `range_start` and `range_end` from tracking table
7. Data is loaded into `dbo.FactSales` table

### Expected Subsequent Runs

1. **Tracking record exists** → Incremental update scenario
2. Reads last successful range end date
3. Calculates new range with 2-day overlap
4. Processes new buckets
5. Updates tracking table with new range

## How It Works

### 1. Metadata Table Management

The notebook automatically creates and manages two warehouse tables.

**`[Incremental Update]`** — current orchestration state (one row per dataflow):



- `dataflow_id`, `workspace_id`, `dataflow_name`: Dataflow identifiers (resolved by name at runtime)
- `initial_load_from_date`: Historical start date
- `bucket_size`, `incrementally_update_last`: Configuration parameters
- `destination_table`, `incremental_update_column`: Target table information
- `update_time`, `status`: Current refresh status and timestamp (UTC). Recognized success statuses: `Success`, `Succeeded`, `Successful`, `Completed`. Failure/terminal statuses: `Failed`, `Cancelled`, `Error`, `Timeout`
- `range_start`, `range_end`: Date range for the current/last refresh bucket (UTC)
- `is_cicd_dataflow`: Flag indicating dataflow type (for API routing)
- `is_adhoc`: Flag indicating this is an ad-hoc extract row (not used for incremental state)
- `run_id`: UUID of the active notebook execution (used for concurrency detection)
- `job_instance_id`: CI/CD job instance ID captured from the API Location header; persisted immediately after trigger for correlation with Fabric Monitoring Hub
- `last_backup_table`: Fully-qualified name of the most recent backup table created for this dataflow; used by recovery logic to restore the correct backup without ambiguity

**`[Incremental Update History]`** — execution ledger (one row per bucket attempt):

Columns: `id` (identity), `dataflow_id`, `dataflow_name`, `run_id`, `job_instance_id`, `trigger_mode` (initial_load / incremental / adhoc / failed_bucket), `bucket_index`, `range_start`, `range_end`, `status`, `attempt_number`, `is_cicd_dataflow`, `is_adhoc`, `start_time`, `end_time`, `created_at`.

A row is inserted with `status=Running` at bucket start and then finalized in place by `id` at completion. This provides a full audit trail of every bucket attempt for RCA, compliance, and performance analysis without relying on external logs. History write failures are non-fatal and do not block orchestration.

### 2. Dataflow Type Detection

The notebook automatically detects whether the dataflow is a CI/CD or regular dataflow by probing the official Microsoft Fabric API endpoints:

- **CI/CD Dataflows**:
  - Detection: `/v1/workspaces/{workspaceId}/items/{dataflowId}`
  - Parameter Discovery: `/v1/workspaces/{workspaceId}/dataflows/{dataflowId}/parameters`
  - Trigger: `/v1/workspaces/{workspaceId}/dataflows/{dataflowId}/jobs/execute/instances` *(documented Dataflow Execute endpoint — always parameterized with `RangeStart` and `RangeEnd`; unparameterized CI/CD execution is not supported)*
  - Status (primary): `/v1/workspaces/{workspaceId}/items/{dataflowId}/jobs/instances/{jobInstanceId}` *(exact job correlation via instance ID from trigger `Location` header)*
  - Status (list fallback): `/v1/workspaces/{workspaceId}/items/{dataflowId}/jobs/instances` *(only used when no `jobInstanceId` is available from the trigger response)*
- **Regular Dataflows**:
  - Trigger: `/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/refreshes`
  - Status: `/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/transactions`

### 3. Processing Logic

#### **Scenario A: Initial Load (No Previous Refresh)**

1. Validates that `initial_load_from_date` is provided
2. Calculates date range from `initial_load_from_date` to yesterday at 23:59:59
3. Splits the date range into buckets based on `bucket_size`
4. For each bucket:
   - Backs up data in the affected range (unless `skip_backup` is True)
   - Deletes any overlapping data in the destination table
   - Updates tracking table with "Running" status
   - Triggers dataflow refresh
   - Waits for completion and monitors status
   - **If bucket fails**: Restores data from backup, then retries up to `bucket_retry_attempts` times with exponential backoff (30s, 60s, 120s, etc.)
   - **If all retries fail**: Data has been restored from the last backup. Logs error, updates tracking table, and **exits with failure code (1)** to fail the pipeline
   - **If bucket succeeds**: Drops the backup table, then moves to next bucket

#### **Scenario B: Previous Refresh Failed**

1. Detects failed status from previous run
2. Retrieves the failed bucket's date range
3. Backs up data in the affected range (unless `skip_backup` is True)
4. Retries the failed bucket using the same retry logic as above, restoring from backup on each failure
5. **If all retries fail**: Data has been restored from backup. Exits with failure code to fail the pipeline
6. **If retry succeeds**: Drops backup table, completes the run. The next pipeline run continues with normal incremental processing (Scenario C)

#### **Scenario C: Incremental Update (Previous Refresh Successful)**

1. Calculates new date range:
   - If `incrementally_update_last` is set: Uses `min(last_end_date + 1 second, yesterday - N days)` to ensure overlap without gaps
   - Otherwise: Starts from `last_end_date + 1 second`
   - End date is always yesterday at 23:59:59
2. Splits date range into buckets if needed
3. Processes each bucket with the same backup/restore, retry, and failure logic as initial load
4. **If any bucket fails after all retries**: Data has been restored from backup. Exits with failure code to fail the pipeline

### 4. Retry Mechanism with Exponential Backoff

When a bucket refresh fails, the notebook retries with exponential backoff. The `bucket_retry_attempts` parameter controls the **total** number of attempts (including the initial attempt). With the default of 3:

1. **Attempt 1**: Initial attempt (no wait)
2. **Attempt 2**: Waits 30 seconds, then retries
3. **Attempt 3**: Waits 60 seconds, then retries

Each subsequent retry doubles the wait time (30s × 2^(attempt-1)) plus a small random jitter (±5–10 s) to reduce thundering-herd pressure when multiple buckets fail simultaneously. Setting `bucket_retry_attempts` to 4 would add a fourth attempt after approximately a 120-second wait.

4. **If all attempts fail**:
   - Updates tracking table with failed status
   - Logs detailed error message
   - Raises `RuntimeError` with failure details
   - **Exits with `sys.exit(1)`** to mark the notebook as failed in the pipeline
   - **No further buckets are processed**

This ensures transient issues (network glitches, temporary service unavailability) are handled gracefully, while persistent failures properly fail the pipeline.

### 5. Date Range Logic

**Inclusive end-of-day contract:** `RangeEnd` is always the inclusive end of the bucket — `23:59:59` of the last day. Downstream Power Query filters should use `<= RangeEnd` (not `< RangeEnd`). This contract assumes source data does not contain meaningful sub-second values.

- **End Date**: Always defaults to yesterday at 23:59:59 (never includes today's partial data)
- **Bucket End Times**: All buckets end at 23:59:59 (the final bucket is capped at yesterday's 23:59:59)
- **Next Bucket Start**: Previous bucket end + 1 second (ensures no gaps or overlaps)
- **Overlap Handling**: When `incrementally_update_last` is set, the start date ensures overlap while avoiding gaps

### 6. Connection Management

The notebook proactively manages database connections to prevent timeout issues:

- Closes connections before long-running dataflow operations
- Validates and recreates connections as needed
- Implements retry logic for all database operations (max 3 retries with 1-second delays)

### 7. Data Safety (Backup & Restore)

Before deleting data from the destination table, the notebook backs up the affected rows into a separate schema:

- **Backup table location**: `[Database].[temporary_backup].[_backup_<dataflow_id_no_hyphens>]` (schema configurable via `backup_schema`; hyphens are stripped from the dataflow ID)
- **Backup schema**: Auto-created on first run if it doesn't exist
- **On success**: Backup table is dropped
- **On failure**: Data is restored from backup, then backup is dropped
- **On interrupted run**: Next execution detects leftover backup and automatically recovers

Set `skip_backup = True` to disable backups for faster execution. **Warning:** with backups disabled, data deleted before a failed refresh cannot be recovered.

### 8. Ad-hoc Extract Mode

Set both `load_from_date` and `load_to_date` to load data for an arbitrary date range without modifying the incremental tracking state.

**Compatibility:**

| Dataflow type | Ad-hoc behaviour |
|---|---|
| CI/CD with public `RangeStart`/`RangeEnd` parameters | **Full support** — range is passed directly via the Execute API at refresh time |
| CI/CD without public parameters | Range is written to the tracking table ad-hoc row; dataflow must read it explicitly |
| Regular dataflow (standard incremental filter) | Dataflow re-runs the last incremental range — **ad-hoc range is ignored** |
| Regular dataflow with custom ad-hoc query logic | Works if Power Query explicitly reads the `is_adhoc=1` row |

By default (`adhoc_fail_if_unsupported = True`) the notebook raises an error when ad-hoc mode is used on a dataflow that does not support direct parameter passing, rather than silently running with unintended data. Set `adhoc_fail_if_unsupported = False` to downgrade this to a warning and proceed with a best-effort reload.

- Creates a separate ad-hoc tracking row (with `is_adhoc` flag) so the main incremental row is untouched
- Only one ad-hoc row exists per dataflow — subsequent ad-hoc runs reuse the existing row instead of creating duplicates
- Splits the ad-hoc range into buckets using the same `bucket_size` setting
- Uses the same backup/restore, retry, and failure logic as incremental mode
- On completion, the ad-hoc tracking row is marked as "Completed"
- On failure, the ad-hoc tracking row is deleted (data already restored from backup)
- If an ad-hoc run is interrupted, the next execution automatically cleans up the orphaned row

## Execution Results

Upon completion or failure, the notebook prints:

```
Dataflow refresh execution completed:
Status: Completed / Completed with N failures / Failed: [error message]
Total buckets processed: N
Successful refreshes: N
Failed refreshes: N
Total retry attempts: N
Duration: X.XX seconds
Dataflow type: CI/CD / Regular
```

If a bucket fails after all retries:

```
================================================================================
DATAFLOW REFRESH FAILED
================================================================================
Error: Bucket N/M failed after 3 attempts with status Failed. Range: YYYY-MM-DD to YYYY-MM-DD

The dataflow refresh has been terminated due to bucket failure after all retry attempts.
Please check the logs above for detailed error information.
================================================================================
```

For unexpected (non-retry) errors, the output uses a distinct header:

```
================================================================================
DATAFLOW REFRESH ERROR
================================================================================
Error: [error message]

An unexpected error occurred during the dataflow refresh process.
Please check the logs above for detailed error information.
================================================================================
```

In both cases, the notebook exits with code 1, causing the Fabric pipeline to mark the notebook activity as **Failed**.

## Integration with Dataflow Power Query

### Option 1: Public Parameters (Recommended for CI/CD Dataflows)

If your dataflow is a CI/CD Dataflow Gen2, you can define `RangeStart` and `RangeEnd` as **public parameters** (type: DateTime). The notebook will automatically discover these parameters and pass the date range values directly when triggering the refresh — no need for the dataflow to query the tracking table.

1. In your dataflow, create two parameters:
   - **Name**: `RangeStart`, **Type**: DateTime
   - **Name**: `RangeEnd`, **Type**: DateTime
2. Use them in your Power Query filter:

```powerquery
let
    FilteredData = Table.SelectRows(YourDataSource,
        each [YourDateColumn] >= RangeStart and [YourDateColumn] <= RangeEnd)
in
    FilteredData
```

The notebook uses the Dataflow Execute endpoint to pass these values at refresh time. All CI/CD dataflows **must** expose public `RangeStart` and `RangeEnd` parameters — there is no unparameterized CI/CD path. Parameter discovery is performed as a pre-flight check before any backup or delete; if discovery fails transiently (429, 5xx, network error) or if the parameters are absent (confirmed non-support), execution fails closed with a descriptive error before any data is modified. The operator must resolve the configuration or retry when the control plane is available — no automatic fallback to a different endpoint or API family occurs.

> **Note:** The tracking table is still updated with `range_start`/`range_end` regardless of whether parameters are passed directly. This maintains backward compatibility, status tracking, and retry logic.

> **`executeOption` policy:** The Execute call uses `executeOption: SkipApplyChanges` by default, which runs only the last successfully published dataflow definition. This avoids a CI/CD publish or git-sync failure causing a refresh failure. The alternative, `ApplyChangesIfNeeded`, publishes the latest saved CI/CD state before executing — but since February 2026, if that publish step fails, the refresh also fails (instead of silently running the older published version). To change this policy, update the `_CICD_EXECUTE_OPTION` class constant in `DataflowRefresher`.

### Option 2: Read from Tracking Table (Regular Dataflows)

Your dataflow Power Query reads the `range_start` and `range_end` values from the `[Incremental Update]` table:

```powerquery
let
    Source = Sql.Database("[server]", "[database]"),
    TrackingTable = Source{[Schema="dbo", Item="Incremental Update"]}[Data],
    FilteredRows = Table.SelectRows(TrackingTable, each
        [dataflow_id] = "your-dataflow-id"
        and ([is_adhoc] = false or [is_adhoc] = null)),
    RangeStart = FilteredRows{0}[range_start],
    RangeEnd = FilteredRows{0}[range_end],

    // Use RangeStart and RangeEnd to filter your data source
    FilteredData = Table.SelectRows(YourDataSource,
        each [YourDateColumn] >= RangeStart and [YourDateColumn] <= RangeEnd)
in
    FilteredData
```

> **Note:** The filter on `is_adhoc` ensures the dataflow reads the incremental tracking row, not a temporary ad-hoc row. If you are using ad-hoc extract mode, the ad-hoc row's `is_adhoc` flag is set to `true` so both rows can coexist without conflict.

## Platform Constraints and Known Limitations

> **CI/CD Background Jobs API — Public Preview**
>
> The Fabric REST endpoints used for CI/CD dataflow triggering and status polling are currently in **Public Preview** and are explicitly **not recommended for production use** by Microsoft. They may change or be removed without notice. The Dataflow Execute trigger endpoint (`/v1/workspaces/.../dataflows/.../jobs/execute/instances`) requires a delegated user token with at least Member workspace permissions — it does not support Service Principals or Managed Identities, making it incompatible with standard enterprise service accounts (Azure DevOps, GitHub Actions, etc.). Parameter discovery and job-status polling endpoints may have different authentication requirements; verify per endpoint in your tenancy.
>
> Mitigation: Parameter discovery runs as a pre-flight check before any data modification and fails closed if `RangeStart`/`RangeEnd` parameters are absent or if discovery itself fails transiently. There is no automatic fallback to a different endpoint or API family. Once a CI/CD Execute is triggered, polling remains on the exact job-instance contract (`/items/{id}/jobs/instances/{jobInstanceId}`) and never crosses to the regular transactions API. The internal defaults `wait_for_completion=True` and `timeout_minutes=120` are not exposed as notebook parameters in the current configuration — to change them, pass them programmatically when calling `execute_incremental_refresh()` directly, or add them to the parameters cell and top-level call. Monitor [Microsoft's changelog](https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-refresh) for GA availability. Until then, consider this path Tier-2/Tier-3 suitable and not Tier-1 mission-critical.

> **Regular Dataflow Polling — Probabilistic Correlation**
>
> The regular (non-CI/CD) dataflow refresh path correlates with the correct transaction by comparing the trigger timestamp within a 60-second skew window. In shared workspaces where another operator could trigger a manual refresh close in time, a false correlation is theoretically possible. Mitigate by running notebook-managed regular dataflows in a dedicated workspace with no manual refreshes.

> **Concurrency — Advisory, Not Atomic**
>
> The concurrency guard is a check-then-act pattern, not a database-level lock. Two pipeline runs starting within milliseconds of each other could both pass the guard. The `run_id` suffix on backup tables isolates their data operations, but the risk of redundant processing exists in extreme concurrent scenarios.

## Best Practices

1. **Bucket Size**: Start with 1 day buckets. Increase if your data volume is low and performance is not a concern.
2. **Retry Attempts**: Default of 3 is recommended. Increase only if you experience frequent transient failures.
3. **Overlap Days**: Set `incrementally_update_last` to 1 or more if your source data can be updated retroactively.
4. **Destination Table Schema**:
   - If your table is in the `dbo` schema: Use just the table name (e.g., `"SalesData"`)
   - If your table is in another schema: Use `schema.table` format (e.g., `"staging.SalesData"` or `"analytics.SalesData"`)
5. **Monitoring**: Monitor the `[Incremental Update]` table in your warehouse to track refresh history and troubleshoot issues.
6. **Pipeline Design**: Use the notebook activity failure to trigger alerts or retry logic at the pipeline level.
7. **Backup Strategy**: Leave `skip_backup` as False (default) for production pipelines. Only use `skip_backup = True` for development/testing or when the source data can be easily reloaded.

## Troubleshooting

### Notebook fails immediately

**Symptoms:**
- Notebook activity fails within seconds
- Error: "Missing required parameter" or "Connection failed"

**Solutions:**
1. **Verify all required parameters are provided:**
   - `dataflow_name` (required), `workspace_name` (optional — defaults to current workspace)
   - `destination_table`, `incremental_update_column`
   - `initial_load_from_date` (required for first run only)

2. **Check warehouse connection:**
   ```sql
   -- Test warehouse connection by running this in your warehouse
   SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES
   ```

3. **Verify notebook constants are configured:**
   - Open the notebook
   - Check the third code cell for `SCHEMA`, `CONNECTION_ARTIFACT_ID`, etc.
   - Ensure the warehouse ID is correct (copy from warehouse URL)

**Common Errors:**
- `"initial_load_from_date is required for the first load"` → Add this parameter for first execution
- `"Connection timeout"` → Verify warehouse is running and accessible
- `"Table does not exist"` → Notebook will auto-create tracking table on first run

### Bucket keeps failing after retries

**Symptoms:**
- Individual buckets fail repeatedly
- Error: "Bucket N/M failed after 3 attempts with status Failed"
- Pipeline marked as failed

**Solutions:**
1. **Check dataflow execution logs:**
   - Open the dataflow in Fabric
   - Navigate to **Refresh history**
   - Review error messages from failed refreshes

2. **Verify Power Query configuration:**
   - Ensure dataflow reads `range_start` and `range_end` correctly
   - Test with a small date range manually
   - Check for data type mismatches

3. **Inspect the tracking table:**
   ```sql
   -- Check current tracking state
   SELECT TOP 5
       dataflow_id,
       dataflow_name,
       status,
       range_start,
       range_end,
       update_time
   FROM [dbo].[Incremental Update]
   WHERE dataflow_id = 'your-dataflow-id'
   ORDER BY update_time DESC
   ```

4. **Reduce bucket size:**
   - Try smaller `bucket_size` (e.g., 1 day instead of 7)
   - Large date ranges may timeout or exceed memory limits

5. **Check data source:**
   - Verify source system is accessible
   - Check for connectivity issues during refresh window
   - Confirm source data exists for the date range

### Wrong dataflow type detected

**Symptoms:**
- Error: "Dataflow refresh failed with status 404" or "Endpoint not found"
- Notebook detects wrong dataflow type (CI/CD vs Regular)

**Solutions:**
1. **Explicitly set the dataflow type:**
   ```json
   {
     "is_cicd_dataflow": true   // or false for regular dataflows
   }
   ```

2. **Verify workspace and dataflow IDs:**
   - Check the URL when viewing your dataflow
   - Ensure no typos in the GUID values
   - Confirm the dataflow exists in the specified workspace

3. **Check dataflow type in Fabric:**
   - CI/CD dataflows are typically created through Git integration
   - Regular dataflows are created directly in the workspace

### Database connection timeouts

**Symptoms:**
- Error: "Connection lost" or "Idle timeout"
- Random failures during long-running refreshes

**Solutions:**
- The notebook handles this automatically with retry logic and connection management
- Connections are closed before long dataflow operations
- If persistent, check:
  - Warehouse availability and health status
  - Network connectivity between notebook and warehouse
  - Fabric capacity resource limits

### Tracking table issues

**Problem: Tracking table shows "Running" but notebook completed**

**Solution:**
```sql
-- Manually check for stuck records
SELECT * FROM [dbo].[Incremental Update]
WHERE status = 'Running'
  AND update_time < DATEADD(HOUR, -2, GETDATE())

-- If needed, manually update stuck records
UPDATE [dbo].[Incremental Update]
SET status = 'Failed'
WHERE dataflow_id = 'your-dataflow-id'
  AND status = 'Running'
  AND update_time < DATEADD(HOUR, -2, GETDATE())
```

**Problem: Need to restart from scratch**

**Solution:**
```sql
-- Option 1: Delete tracking record (will trigger initial load on next run)
DELETE FROM [dbo].[Incremental Update]
WHERE dataflow_id = 'your-dataflow-id'

-- Option 2: Use reinitialize_dataflow parameter
-- Set reinitialize_dataflow = true in pipeline parameters
```

### Backup table recovery

**Problem: Leftover backup table exists**

**Solution:**
The notebook automatically detects and recovers from leftover backup tables on the next run. If you need to manually intervene:
```sql
-- Check for backup tables
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('temporary_backup')

-- Manually restore from a backup if needed (note: hyphens are stripped from dataflow ID)
INSERT INTO [dbo].[YourTable] SELECT * FROM [temporary_backup].[_backup_<dataflow_id_no_hyphens>]

-- Drop the backup table
DROP TABLE IF EXISTS [temporary_backup].[_backup_<dataflow_id_no_hyphens>]
```

### Monitoring and debugging

**Check tracking table history:**
```sql
-- View refresh history
SELECT
    dataflow_name,
    status,
    range_start,
    range_end,
    update_time,
    DATEDIFF(SECOND,
        LAG(update_time) OVER (PARTITION BY dataflow_id ORDER BY update_time),
        update_time
    ) as seconds_since_last_refresh
FROM [dbo].[Incremental Update]
WHERE dataflow_id = 'your-dataflow-id'
ORDER BY update_time DESC
```

**Check for data gaps:**
```sql
-- Identify gaps in refreshed date ranges
WITH RangedData AS (
    SELECT
        range_start,
        range_end,
        LEAD(range_start) OVER (ORDER BY range_start) as next_start
    FROM [dbo].[Incremental Update]
    WHERE dataflow_id = 'your-dataflow-id'
      AND status = 'Success'
)
SELECT
    range_end as gap_start,
    next_start as gap_end,
    DATEDIFF(DAY, range_end, next_start) as gap_days
FROM RangedData
WHERE DATEADD(SECOND, 1, range_end) < next_start
```

### CI/CD Execute — `InvalidJobType` (HTTP 400)

**Symptoms:**
- CI/CD run succeeds through all pre-flight checks (`RangeStart`/`RangeEnd` confirmed) but fails at the Execute trigger
- Fabric error code `InvalidJobType`, HTTP 400
- Log message: `"Failed to trigger CI/CD dataflow … HTTP 400, errorCode='InvalidJobType'"`

**Root cause:**
The documented Dataflow Execute endpoint (`/v1/workspaces/{wid}/dataflows/{dfid}/jobs/execute/instances`) is a preview API. Some tenant clusters have not yet adopted this route shape and return `400 InvalidJobType` even though Microsoft Learn documents the path. The older Background Jobs API route (`?jobType=Execute`) is still accepted by these clusters.

**Default behaviour (`cicd_execute_route_mode = 'auto'`):**
The notebook tries the documented route first. If it receives `400 InvalidJobType` with no `Location` header (meaning no job was created), it logs a warning and retries once on the legacy Background Jobs route using the identical parameter payload. No data risk: the retry only fires when the first request was rejected before any job instance was created.

**Permanently switch to the legacy route:**
If the documented route consistently fails in your tenant, set:
```
cicd_execute_route_mode = 'legacy_query'
```

**Permanently lock to the documented route:**
Once a smoke test confirms the documented route works consistently in your tenant, set:
```
cicd_execute_route_mode = 'documented'
```
This avoids the auto-retry overhead on every run.

**Same-run restore on trigger rejection:**
If the trigger is rejected after the production range has already been deleted, the notebook restores from the pinned backup immediately in the same run — the production range is not left hollow until the next notebook run. The log will confirm: `"Same-run restore completed — production data is intact."` If same-run restore fails, the backup is preserved and the log will say: `"Backup preserved for manual recovery."` Next-run recovery via `_recover_from_backup_if_needed()` remains active as a safety net for crash/OOM scenarios.

### Getting help

If you continue experiencing issues:
1. Check the notebook execution logs in Fabric
2. Review the dataflow refresh history
3. Verify all configuration values match your environment
4. Test with a minimal date range (1-2 days) first

## Version History

- **v5.15**: Addressed seven critique findings from March 2026 review: (High) `cicd_execute_route_mode` parameter added (`'auto'`/`'documented'`/`'legacy_query'`); in `'auto'` mode the documented Execute endpoint is tried first and a `400 InvalidJobType` response with no job created triggers a single retry on the legacy Background Jobs route (`?jobType=Execute`) using the identical parameter payload — safe because the first request was rejected before any job instance was created. (High) All four trigger call sites now wrap the delete→trigger sequence in a per-attempt try/except; a trigger rejection after delete performs an immediate same-run restore from the pinned backup — the log explicitly states whether restore succeeded or the backup was preserved for manual recovery; next-run recovery via `_recover_from_backup_if_needed()` remains active for crash/OOM scenarios. (Medium) Diagnostic helpers added to capture `requestId`, `x-ms-public-api-error-code`, `home-cluster-uri` headers and the route label on every trigger attempt and error path. (Low) `sys.exit(0/1)` replaced with normal completion / `raise` so Fabric surfaces the real error message rather than a misleading "Kernel died" symptom. README updated with `cicd_execute_route_mode` parameter documentation and CI/CD Execute `InvalidJobType` troubleshooting guide.
- **v5.13**: Addressed five critique findings from March 2026 review: (High) CI/CD parameter discovery now distinguishes transient failure from confirmed non-support — returns a 3-state result; only confirmed results are cached; a pre-flight check in `execute_incremental_refresh()` fails closed before any backup or delete when `RangeStart`/`RangeEnd` are absent or discovery is inconclusive; unparameterized CI/CD Execute path removed — all CI/CD dataflows must expose public `RangeStart` and `RangeEnd` parameters. (Medium) CI/CD polling now stays on the exact `job_instance_id` contract throughout a run — the direct-instance poll returns `None` on transient errors instead of falling through to the list endpoint; the list endpoint no longer flips `is_cicd_dataflow` to `False` on failure; the regular transactions API is never reached as a CI/CD fallback. (Low-Medium) `_detect_dataflow_type()` now returns `None` (inconclusive) instead of `False` for 401/403/429/5xx probe responses and total detection failure; `execute_incremental_refresh()`, `refresh_dataflow()`, and `get_latest_refresh_status()` all raise with a clear operator message when detection is inconclusive rather than silently defaulting to the regular dataflow path. (Low-Medium) README corrected: trigger endpoint updated to documented Dataflow Execute path, fallback language removed, SP/MI statement qualified per-endpoint, `wait_for_completion`/`timeout_minutes` documented as internal defaults rather than operator-facing parameters, version history label corrected. (Low-Medium) `executeOption` extracted from hardcoded literal to `DataflowRefresher._CICD_EXECUTE_OPTION` class constant with inline documentation of the `SkipApplyChanges` vs `ApplyChangesIfNeeded` trade-off.
- **v5.12**: Addressed four critique findings: (Medium) Stale `is_cicd_dataflow` stored value demoted from routing authority to diagnostic warning — caller's explicit value and live auto-detection now always take precedence in the continuation path; warning fires if stored value differs from resolved value. (Medium) CI/CD trigger endpoints migrated from legacy item-level paths (`/items/{id}/jobs/instances?jobType=`) to documented Dataflow-specific path (`/dataflows/{id}/jobs/execute/instances`); undocumented `Refresh` job type replaced with documented `Execute`. (Low-Medium) `executeOption: SkipApplyChanges` added explicitly inside `executionData` on both CI/CD Execute payloads; `DataflowName` removed from no-parameter payload (not in Execute schema). (Low) Bootstrap wording corrected from "eliminates" to "avoids … when deployment is serialized" in log message and README.
- **v5.11**: Addressed five critique findings: (High) `bootstrap_tracking_row()` deployment helper avoids the first-run INSERT race when bootstrap is serialized before first execution; runtime WARNING added for unbootstrapped first-run paths. (Medium) `Deduped` added as a terminal non-success polling status with an explanatory warning. (Medium) `_normalize_adhoc_dates()` now validates after capping — raises `ValueError` if the effective range is empty, preventing silent false-success `Completed` status. (Low) All four `wait_for_completion=False` backup-drop sites emit a data-safety warning documenting the unrecoverability risk. (Low) README updated: endpoint fallback note clarified, v5.2 history corrected, `bootstrap_tracking_row()` deployment step added.
- **v5.4**: Production hardening release addressing all findings from dual-engineer architecture review (critique.md + critique2.md):
  - **High fix**: `force_run` is now a notebook parameter wired through to the entrypoint — operators can override stale concurrency guards from the pipeline without manual warehouse edits
  - **High fix**: SQL identifier validation (`_validate_sql_identifier`) guards `destination_table` and `incremental_update_column` against characters that would break DDL/DML at runtime; `_bq()` helper enforces consistent bracket-quoting
  - **High fix**: `job_instance_id` is now persisted to the tracking table immediately after every refresh trigger (in all four execution paths), enabling correlation between warehouse state and Fabric Monitoring Hub
  - **High fix**: Per-column VARCHAR widening — each of the five varchar columns is migrated independently so partial schema drift cannot remain silently unaddressed
  - **Medium-High fix**: `[Incremental Update History]` table added — one row per bucket attempt with `trigger_mode`, `bucket_index`, `attempt_number`, `start_time`, `end_time`, and all correlation IDs; history writes are non-fatal
  - **Medium-High fix**: `last_backup_table` column added to `[Incremental Update]`; set when backup is created and consumed by recovery logic to restore the exact backup rather than picking by prefix search — ambiguous multi-orphan recovery is now refused rather than guessed
  - **Medium fix**: `_restore_from_backup` derives its column list from the backup table schema rather than the current destination table, remaining correct across deployment-time schema changes
  - **Medium fix**: Exponential backoff now includes ±5–10 s random jitter on all four retry sites to reduce thundering-herd pressure under capacity throttle
  - **Medium fix**: `validate_only` mode added — resolves IDs and prints execution plan without touching data
  - **Medium fix**: `adhoc_fail_if_unsupported` parameter (default True) — ad-hoc mode now fails closed when the dataflow cannot consume the date range, instead of silently proceeding
  - **Medium fix**: Fabric refresh limit pre-run estimation — logs estimated refresh count and warns when it may approach the 150/300 per-24h service limit
  - **Medium fix**: `FabricRestClient` used for Fabric-native `/v1/` endpoints; `PowerBIRestClient` retained for `/v1.0/myorg/` transaction polling
  - **Docs**: `CONNECTION_ARTIFACT_ID` clarified as warehouse artifact GUID (not workspace ID); `force_run` entry correct in parameters table; CI/CD preview API constraints and Service Principal limitation documented; history ledger described; platform constraints section added
- **v5.3**: Hardening release addressing all critical and high findings from dual-engineer architecture review:
  - **Critical fix**: Restore logic now pre-deletes the target range before inserting from backup, eliminating silent duplication from partial Fabric dataflow writes
  - **Critical fix**: Backup/restore uses explicit column lists (via `sys.columns`) instead of `SELECT *`, preventing failures under schema evolution
  - **High fix**: Job-correlated polling — CI/CD refresh captures the `job_instance_id` from the `Location` header and polls that exact instance; regular dataflow polling correlates by trigger timestamp
  - **High fix**: Regular dataflow API payload corrected to `{"notifyOption": "NoNotification"}` per documented contract
  - **High fix**: Concurrency guard — detects active runs and refuses to start unless the existing run is stale or `force_run=True`
  - **High fix**: Backup tables now include a per-run suffix (`_backup_<id>_<run_id_short>`) to isolate concurrent runs
  - **High fix**: Ad-hoc mode now logs a clear warning when the dataflow cannot receive the range via direct parameters
  - **Medium fix**: `Retry-After` header from CI/CD 202 responses is honoured as the initial poll delay
  - **Medium fix**: CI/CD detection upgraded to probe the parameters endpoint (capability check, not type inference)
  - **Medium fix**: All timestamps use `datetime.utcnow()` (was `datetime.now()`, implicitly UTC in Fabric but now explicit)
  - **Medium fix**: Tracking table schema — varchar columns widened to 255, `run_id` and `job_instance_id` columns added, `dbo` schema hardcode replaced with dynamic detection
  - **Medium fix**: README updated to reflect name-based parameter resolution, ad-hoc mode compatibility table, and new `force_run` parameter
- **v5.2**: Added automatic discovery and passing of `RangeStart`/`RangeEnd` as public parameters to CI/CD dataflows via the Execute endpoint. The runtime intentionally fails closed after destructive work begins — it does not fall back to the Refresh endpoint mid-operation (the v5.2 graceful-fallback wording was superseded by the fail-closed hardening introduced in later versions).
- **v5.1**: Fixed ad-hoc mode creating duplicate tracking rows — subsequent ad-hoc runs now reuse the existing row instead of inserting a new one. Updated documentation: corrected API endpoints, retry mechanism description, architecture diagram, Power Query example, backup table naming, and other discrepancies
- **v5.0**: Added data safety with backup/restore before deletes, ad-hoc extract mode, configurable backup schema, and skip-backup option
- **v3.0**: Added bucket retry mechanism with exponential backoff and pipeline failure integration
- **v2.0**: Added CI/CD dataflow support with automatic type detection
- **v1.0**: Initial implementation with basic incremental refresh framework
