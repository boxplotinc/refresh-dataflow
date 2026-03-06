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
│  [_backup] Schema   │           ┌───────────────────┐
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
| `workspace_id` | String | Yes | ID of the Fabric workspace containing the dataflow |
| `dataflow_id` | String | Yes | ID of the dataflow to refresh |
| `dataflow_name` | String | Yes | Name or description of the dataflow |
| `initial_load_from_date` | String | Yes* | Start date for initial historical load (format: 'YYYY-MM-DD'). *Required only for first load |
| `bucket_size` | Integer or String | No | Size of each refresh bucket. Accepts: integer for days (e.g., `7`), or string with suffix: `"1M"` for months, `"1Y"` for years (default: 1) |
| `bucket_retry_attempts` | Integer | No | Number of retry attempts for failed buckets (default: 3) |
| `incrementally_update_last` | Integer or String | No | Period to overlap/refresh in incremental updates. Accepts: integer for days (e.g., `1`), or string with suffix: `"3M"` for months, `"1Y"` for years (default: 1) |
| `reinitialize_dataflow` | Boolean | No | Set to True to delete tracking data and restart from scratch (default: False) |
| `destination_table` | String | Yes | Name of the destination table in the warehouse where data is written. Can be just table name (uses dbo schema) or `schema.table` format for tables in other schemas |
| `incremental_update_column` | String | Yes | DateTime column used for incremental filtering |
| `is_cicd_dataflow` | Boolean | No | Explicitly specify if this is a CI/CD dataflow (auto-detected if not provided) |
| `load_from_date` | String | No | Ad-hoc extract start date (format: 'YYYY-MM-DD'). Both `load_from_date` and `load_to_date` must be set. |
| `load_to_date` | String | No | Ad-hoc extract end date (format: 'YYYY-MM-DD'). Both `load_from_date` and `load_to_date` must be set. |
| `backup_schema` | String | No | Schema name for backup tables, created automatically if it doesn't exist (default: `_backup`) |
| `skip_backup` | Boolean | No | Skip backup/restore before delete operations for faster runs. **Warning:** data is not recoverable if a refresh fails (default: False) |

## Notebook Constants

The following constants must be configured inside the notebook:

| Constant | Example | Description |
|----------|---------|-------------|
| `SCHEMA` | `"[Warehouse DB].[dbo]"` | Database schema where the tracking table resides |
| `INCREMENTAL_TABLE` | `"[Incremental Update]"` | Name of the metadata tracking table |
| `CONNECTION_ARTIFACT` | `"Your Warehouse Name"` | Name of the warehouse artifact |
| `CONNECTION_ARTIFACT_ID` | `"your-warehouse-id-guid"` | Technical ID (GUID) of the warehouse |
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

## Quick Start Example

Here's a complete example of how to configure the pipeline parameters for your first run:

### Example Configuration

**Pipeline Parameters:**
```json
{
  "workspace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "dataflow_id": "x9y8z7w6-v5u4-3210-zyxw-vu9876543210",
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
- `workspace_id`: Your Fabric workspace GUID (found in workspace URL)
- `dataflow_id`: Your dataflow GUID (found in dataflow URL)
- `dataflow_name`: Descriptive name for logging/tracking
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

The notebook automatically creates and manages an `[Incremental Update]` tracking table in the warehouse with the following schema:

- `dataflow_id`, `workspace_id`, `dataflow_name`: Dataflow identifiers
- `initial_load_from_date`: Historical start date
- `bucket_size`, `incrementally_update_last`: Configuration parameters
- `destination_table`, `incremental_update_column`: Target table information
- `update_time`, `status`: Current refresh status and timestamp. Recognized success statuses: `Success`, `Succeeded`, `Successful`, `Completed`. Failure/terminal statuses: `Failed`, `Cancelled`, `Error`, `Timeout`
- `range_start`, `range_end`: Date range for the current/last refresh bucket
- `is_cicd_dataflow`: Flag indicating dataflow type (for API routing)
- `is_adhoc`: Flag indicating this is an ad-hoc extract row (not used for incremental state)

### 2. Dataflow Type Detection

The notebook automatically detects whether the dataflow is a CI/CD or regular dataflow by probing the official Microsoft Fabric API endpoints:

- **CI/CD Dataflows**:
  - Detection: `/v1/workspaces/{workspace_id}/items/{dataflow_id}`
  - Parameter Discovery: `/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/parameters`
  - Trigger (with parameters): `/v1/workspaces/{workspace_id}/items/{dataflow_id}/jobs/instances?jobType=Execute`
  - Trigger (without parameters): `/v1/workspaces/{workspace_id}/items/{dataflow_id}/jobs/instances?jobType=Refresh`
  - Status: `/v1/workspaces/{workspace_id}/items/{dataflow_id}/jobs/instances`
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

Each subsequent retry doubles the wait time (30s × 2^(attempt-1)). Setting `bucket_retry_attempts` to 4 would add a fourth attempt after a 120-second wait.

4. **If all attempts fail**:
   - Updates tracking table with failed status
   - Logs detailed error message
   - Raises `RuntimeError` with failure details
   - **Exits with `sys.exit(1)`** to mark the notebook as failed in the pipeline
   - **No further buckets are processed**

This ensures transient issues (network glitches, temporary service unavailability) are handled gracefully, while persistent failures properly fail the pipeline.

### 5. Date Range Logic

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

- **Backup table location**: `[Database].[_backup].[_backup_<dataflow_id_no_hyphens>]` (schema configurable via `backup_schema`; hyphens are stripped from the dataflow ID)
- **Backup schema**: Auto-created on first run if it doesn't exist
- **On success**: Backup table is dropped
- **On failure**: Data is restored from backup, then backup is dropped
- **On interrupted run**: Next execution detects leftover backup and automatically recovers

Set `skip_backup = True` to disable backups for faster execution. **Warning:** with backups disabled, data deleted before a failed refresh cannot be recovered.

### 8. Ad-hoc Extract Mode

Set both `load_from_date` and `load_to_date` to load data for an arbitrary date range without modifying the incremental tracking state:

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

The notebook uses the Execute endpoint to pass these values at refresh time. If parameter discovery or the Execute call fails, it falls back to the standard Refresh endpoint automatically.

> **Note:** The tracking table is still updated with `range_start`/`range_end` regardless of whether parameters are passed directly. This maintains backward compatibility, status tracking, and retry logic.

### Option 2: Read from Tracking Table (Regular Dataflows or Fallback)

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
   - `workspace_id`, `dataflow_id`, `dataflow_name`
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
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('_backup')

-- Manually restore from a backup if needed (note: hyphens are stripped from dataflow ID)
INSERT INTO [dbo].[YourTable] SELECT * FROM [_backup].[_backup_<dataflow_id_no_hyphens>]

-- Drop the backup table
DROP TABLE IF EXISTS [_backup].[_backup_<dataflow_id_no_hyphens>]
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

### Getting help

If you continue experiencing issues:
1. Check the notebook execution logs in Fabric
2. Review the dataflow refresh history
3. Verify all configuration values match your environment
4. Test with a minimal date range (1-2 days) first

## Version History

- **v5.2**: Added automatic discovery and passing of `RangeStart`/`RangeEnd` as public parameters to CI/CD dataflows via the Execute endpoint, with graceful fallback to the Refresh endpoint
- **v5.1**: Fixed ad-hoc mode creating duplicate tracking rows — subsequent ad-hoc runs now reuse the existing row instead of inserting a new one. Updated documentation: corrected API endpoints, retry mechanism description, architecture diagram, Power Query example, backup table naming, and other discrepancies
- **v5.0**: Added data safety with backup/restore before deletes, ad-hoc extract mode, configurable backup schema, and skip-backup option
- **v3.0**: Added bucket retry mechanism with exponential backoff and pipeline failure integration
- **v2.0**: Added CI/CD dataflow support with automatic type detection
- **v1.0**: Initial implementation with basic incremental refresh framework
