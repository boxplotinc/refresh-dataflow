# Custom Incremental Update Framework Using a Notebook in Microsoft Fabric

## Introduction
When configuring incremental updates for extracting data from SAP Business Warehouse (BW) into Microsoft Fabric, we encountered several challenges due to limitations in query folding and bucket sizes for historical loads. Our goal was to extract three years of data, but the source system could only process data one day at a time due to volume and memory constraints. The default maximum bucket size of 50 days was not suitable for our needs.

To overcome these challenges, we developed a custom incremental update framework tailored for scenarios where standard approaches fall short. This framework leverages Microsoft Fabric notebooks to provide flexibility and precise control over the incremental update process.

## Why Use Notebooks?
Notebooks offer the most flexibility in managing incremental updates, allowing for precise control over each aspect of the data extraction and refresh process. By utilizing notebook parameters, we created a generalized approach applicable to various data sources.

## Notebook Parameters
The notebook receives the following parameters from the pipeline:

- **dataflow_id**: The technical ID of the dataflow, used to refresh and check the status of the dataflow refresh.
- **workspace_id**: The technical ID of the workspace, required for dataflow refresh operations.
- **dataflow_name**: A descriptive name for easy identification in the incremental table.
- **initial_load_from_date**: Defines the starting date for the historical data load (format: YYYY-MM-DD).
- **bucket_size_in_days**: Specifies the size of each data extraction bucket (in days). The first refresh loads data in these bucket sizes, ensuring manageable data chunks.
- **reinitialize_dataflow**: A flag indicating whether to refresh the complete dataflow, including historical data.
- **incrementally_update_last_n_days**: Specifies how many past days should be refreshed daily after the initial load.
- **destination_table**: The warehouse table where the extracted data is stored. Used to remove overlapping records before each refresh.
- **incremental_update_column**: The column used to identify overlapping records and ensure data consistency.

### Additional constants configured inside the notebook:
```python
SCHEMA = "[<your Warehouse or Lakehouse name>].[dbo]"
INCREMENTAL_TABLE = "[Incremental Update]"
CONNECTION_ARTIFACT = "<your Warehouse or Lakehouse name>"
CONNECTION_ARTIFACT_ID = "<your Warehouse or Lakehouse ID>"
CONNECTION_ARTIFACT_TYPE = "Warehouse" or "Lakehouse"
```

## How the Framework Works
Once the pipeline is configured with this notebook and the necessary parameters, the notebook executes the following steps:

### 1. Check for Existing Incremental Update Records
- The notebook queries the incremental update table in the warehouse.
- If no entry exists for the given dataflow, it creates the table and inserts a new record.

### 2. Process the Initial Data Load
- The historical date range (from `initial_load_from_date` to today) is divided into buckets of `bucket_size_in_days`.
- The notebook updates the incremental update table with `range_start` and `range_end` for each bucket.
- The dataflow is triggered using the Power BI REST API, and its refresh status is monitored.

### 3. Handle Incremental Refreshes
- If an existing entry is found in the incremental update table, the notebook checks the status of the last refresh.
- If the last refresh failed or did not complete successfully, it retries the previous refresh.
- If the last refresh was successful, the next bucket is processed until all historical data is loaded.

### 4. Monitor Dataflow Refresh Status
- The notebook polls the Power BI REST API every 30 seconds to check the dataflow refresh status.
- If the refresh is successful, it moves on to the next bucket.

### 5. Prevent Data Duplication
- Before each refresh, the notebook checks the destination table for overlapping records using `incremental_update_column`.
- Any duplicate records within the refresh range are deleted before inserting new data.

## Implementation: Dataflow Refresher
The following code initializes and executes the `DataflowRefresher` object, which handles the incremental refresh process.

```python
# Create the Power BI REST client
client = fabric.PowerBIRestClient()

# Create the dataflow refresher object
dataflow_refresher = DataflowRefresher(
    client,
    CONNECTION_ARTIFACT,
    CONNECTION_ARTIFACT_ID,
    CONNECTION_ARTIFACT_TYPE,
    SCHEMA,
    INCREMENTAL_TABLE,
    logging.INFO
)

# Execute incremental refresh
dataflow_refresher.execute_incremental_refresh(
    workspace_id,
    dataflow_id,
    dataflow_name,
    destination_table,
    incremental_update_column,
    initial_load_from_date,
    bucket_size_in_days,
    reinitialize_dataflow,
    incrementally_update_last_n_days
)
```

## Conclusion
This custom incremental update framework provides a robust solution for handling large-volume data extractions where standard approaches are insufficient. By leveraging Microsoft Fabric notebooks and Power BI REST APIs, we achieved greater flexibility and reliability in our data refresh process.

You can download the notebook from GitHub.
