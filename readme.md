# Microsoft Fabric Dataflow Incremental Refresh Notebook

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
│  │ 2. Calculate date ranges & buckets                 │ │
│  │ 3. Update tracking table (Running status)          │ │
│  │ 4. Call Fabric REST API to trigger dataflow        │ │
│  │ 5. Poll for completion status                      │ │
│  │ 6. Update tracking table (Success/Failed status)   │ │
│  └────────────────────────────────────────────────────┘ │
└─────────┬───────────────────────────────────┬───────────┘
          │                                   │
          ▼                                   ▼
┌─────────────────────┐           ┌─────────────────────┐
│   Warehouse         │           │  Fabric REST API    │
│  [Incremental       │           │  - Regular DF:      │
│   Update] Table     │           │    /dataflows/...   │
│  - range_start      │           │  - CI/CD DF:        │
│  - range_end        │           │    /items/.../jobs  │
│  - status           │           └──────────┬──────────┘
└─────────┬───────────┘                      │
          │                                  ▼
          │                        ┌───────────────────┐
          └───────────────────────>│   Dataflow Gen2   │
            (Dataflow reads range) │  (Power Query M)  │
                                   └─────────┬─────────┘
                                             │
                                             ▼
                                   ┌───────────────────┐
                                   │  Data Source      │
                                   │  (Filtered by     │
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
2. **Notebook** manages the tracking table and orchestrates refresh
3. **Tracking table** stores date ranges that the dataflow reads
4. **Dataflow** executes with filtered date range from tracking table
5. **Data** flows from source to warehouse destination table

## Key Features

- **Intelligent Bucket Processing**: Splits large date ranges into configurable buckets to manage incremental loads efficiently
- **Automatic Retry Mechanism**: Retries failed bucket refreshes with exponential backoff before failing the entire process
- **Dataflow Type Auto-Detection**: Automatically detects and supports both regular and CI/CD dataflows
- **Connection Management**: Handles database connections with automatic retry and reconnection logic to prevent timeout issues
- **Comprehensive Status Tracking**: Maintains detailed metadata about refresh operations in a warehouse tracking table
- **Pipeline Failure Integration**: Exits with proper failure codes to integrate with Fabric pipeline error handling

## Pipeline Parameters

Configure the following parameters when setting up the notebook activity in your Fabric pipeline:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_id` | String | Yes | ID of the Fabric workspace containing the dataflow |
| `dataflow_id` | String | Yes | ID of the dataflow to refresh |
| `dataflow_name` | String | Yes | Name or description of the dataflow |
| `initial_load_from_date` | String | Yes* | Start date for initial historical load (format: 'YYYY-MM-DD'). *Required only for first load |
| `bucket_size_in_days` | Integer | No | Size of each refresh bucket in days (default: 1) |
| `bucket_retry_attempts` | Integer | No | Number of retry attempts for failed buckets (default: 3) |
| `incrementally_update_last_n_days` | Integer | No | Number of days to overlap/refresh in incremental updates (default: 1) |
| `reinitialize_dataflow` | Boolean | No | Set to True to delete tracking data and restart from scratch (default: False) |
| `destination_table` | String | Yes | Name of the destination table in the warehouse where data is written. Can be just table name (uses dbo schema) or `schema.table` format for tables in other schemas |
| `incremental_update_column` | String | Yes | DateTime column used for incremental filtering |
| `is_cicd_dataflow` | Boolean | No | Explicitly specify if this is a CI/CD dataflow (auto-detected if not provided) |

## Notebook Constants

The following constants must be configured inside the notebook:

| Constant | Example | Description |
|----------|---------|-------------|
| `SCHEMA` | `"[Warehouse DB].[dbo]"` | Database schema where the tracking table resides |
| `INCREMENTAL_TABLE` | `"[Incremental Update]"` | Name of the metadata tracking table |
| `CONNECTION_ARTIFACT` | `"Warehouse name or id"` | Name of the warehouse artifact |
| `CONNECTION_ARTIFACT_ID` | `"Workspace name or id"` | Technical ID of the warehouse |
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
  "bucket_size_in_days": 7,
  "bucket_retry_attempts": 3,
  "incrementally_update_last_n_days": 2,
  "reinitialize_dataflow": false,
  "destination_table": "FactSales",
  "incremental_update_column": "OrderDate",
  "is_cicd_dataflow": null
}
```

**Parameter Explanation:**
- `workspace_id`: Your Fabric workspace GUID (found in workspace URL)
- `dataflow_id`: Your dataflow GUID (found in dataflow URL)
- `dataflow_name`: Descriptive name for logging/tracking
- `initial_load_from_date`: Start loading data from January 1, 2024 (first run only)
- `bucket_size_in_days`: Process 7 days at a time
- `incrementally_update_last_n_days`: Overlap last 2 days on each refresh
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
- `bucket_size_in_days`, `incrementally_update_last_n_days`: Configuration parameters
- `destination_table`, `incremental_update_column`: Target table information
- `update_time`, `status`: Current refresh status and timestamp
- `range_start`, `range_end`: Date range for the current/last refresh bucket
- `is_cicd_dataflow`: Flag indicating dataflow type (for API routing)

### 2. Dataflow Type Detection

The notebook automatically detects whether the dataflow is a CI/CD or regular dataflow by probing the official Microsoft Fabric API endpoints:

- **CI/CD Dataflows**: Uses `/v1/workspaces/{workspace_id}/items/{dataflow_id}/jobs/instances` endpoint
- **Regular Dataflows**: Uses `/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/refreshes` endpoint

### 3. Processing Logic

#### **Scenario A: Initial Load (No Previous Refresh)**

1. Validates that `initial_load_from_date` is provided
2. Calculates date range from `initial_load_from_date` to yesterday at 23:59:59
3. Splits the date range into buckets based on `bucket_size_in_days`
4. For each bucket:
   - Deletes any overlapping data in the destination table
   - Updates tracking table with "Running" status
   - Triggers dataflow refresh
   - Waits for completion and monitors status
   - **If bucket fails**: Retries up to `bucket_retry_attempts` times with exponential backoff (30s, 60s, 120s, etc.)
   - **If all retries fail**: Logs error, updates tracking table, and **exits with failure code (1)** to fail the pipeline
   - **If bucket succeeds**: Moves to next bucket

#### **Scenario B: Previous Refresh Failed**

1. Detects failed status from previous run
2. Retrieves the failed bucket's date range
3. Retries the failed bucket using the same retry logic as above
4. **If all retries fail**: Exits with failure code to fail the pipeline
5. **If retry succeeds**: Continues with normal incremental processing

#### **Scenario C: Incremental Update (Previous Refresh Successful)**

1. Calculates new date range:
   - If `incrementally_update_last_n_days` is set: Uses `min(last_end_date + 1 second, yesterday - N days)` to ensure overlap without gaps
   - Otherwise: Starts from `last_end_date + 1 second`
   - End date is always yesterday at 23:59:59
2. Splits date range into buckets if needed
3. Processes each bucket with the same retry logic as initial load
4. **If any bucket fails after all retries**: Exits with failure code to fail the pipeline

### 4. Retry Mechanism with Exponential Backoff

When a bucket refresh fails, the notebook:

1. **Retry 1**: Waits 30 seconds, then retries
2. **Retry 2**: Waits 60 seconds, then retries
3. **Retry 3**: Waits 120 seconds, then retries
4. **If all retries fail**:
   - Updates tracking table with failed status
   - Logs detailed error message
   - Raises `RuntimeError` with failure details
   - **Exits with `sys.exit(1)`** to mark the notebook as failed in the pipeline
   - **No further buckets are processed**

This ensures transient issues (network glitches, temporary service unavailability) are handled gracefully, while persistent failures properly fail the pipeline.

### 5. Date Range Logic

- **End Date**: Always defaults to yesterday at 23:59:59 (never includes today's partial data)
- **Bucket End Times**: Set to 23:59:59 except for the final bucket
- **Next Bucket Start**: Previous bucket end + 1 second (ensures no gaps or overlaps)
- **Overlap Handling**: When `incrementally_update_last_n_days` is set, the start date ensures overlap while avoiding gaps

### 6. Connection Management

The notebook proactively manages database connections to prevent timeout issues:

- Closes connections before long-running dataflow operations
- Validates and recreates connections as needed
- Implements retry logic for all database operations (max 3 retries with 1-second delays)

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

The notebook then exits with code 1, causing the Fabric pipeline to mark the notebook activity as **Failed**.

## Integration with Dataflow Power Query

Your dataflow Power Query should read the `range_start` and `range_end` parameters from the `[Incremental Update]` table:

```powerquery
let
    Source = Sql.Database("[server]", "[database]"),
    TrackingTable = Source{[Schema="dbo", Item="Incremental Update"]}[Data],
    FilteredRows = Table.SelectRows(TrackingTable, each [dataflow_id] = "your-dataflow-id"),
    RangeStart = FilteredRows{0}[range_start],
    RangeEnd = FilteredRows{0}[range_end],

    // Use RangeStart and RangeEnd to filter your data source
    FilteredData = Table.SelectRows(YourDataSource,
        each [YourDateColumn] >= RangeStart and [YourDateColumn] <= RangeEnd)
in
    FilteredData
```

## Best Practices

1. **Bucket Size**: Start with 1 day buckets. Increase if your data volume is low and performance is not a concern.
2. **Retry Attempts**: Default of 3 is recommended. Increase only if you experience frequent transient failures.
3. **Overlap Days**: Set `incrementally_update_last_n_days` to 1 or more if your source data can be updated retroactively.
4. **Destination Table Schema**:
   - If your table is in the `dbo` schema: Use just the table name (e.g., `"SalesData"`)
   - If your table is in another schema: Use `schema.table` format (e.g., `"staging.SalesData"` or `"analytics.SalesData"`)
5. **Monitoring**: Monitor the `[Incremental Update]` table in your warehouse to track refresh history and troubleshoot issues.
6. **Pipeline Design**: Use the notebook activity failure to trigger alerts or retry logic at the pipeline level.

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
   - Try smaller `bucket_size_in_days` (e.g., 1 day instead of 7)
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

- **v3.0**: Added bucket retry mechanism with exponential backoff and pipeline failure integration
- **v2.0**: Added CI/CD dataflow support with automatic type detection
- **v1.0**: Initial implementation with basic incremental refresh framework
