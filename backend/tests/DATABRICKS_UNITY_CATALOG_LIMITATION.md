# Databricks Unity Catalog Limitation

## Issue

When testing Airtable → Databricks integration with Unity Catalog enabled, the pipeline fails with:

```
[TABLE_OR_VIEW_NOT_FOUND] The table or view `INFORMATION_SCHEMA`.`SCHEMATA` cannot be found.
```

## Root Cause

This is a known compatibility issue between DLT (data load tool library) and Databricks Unity Catalog. DLT's Databricks destination queries `INFORMATION_SCHEMA.SCHEMATA` which has a different structure in Unity Catalog compared to legacy Hive Metastore.

## What Works

✅ Databricks connection and authentication
✅ Airtable data extraction (tested with 11 customers, 16 products, 13 orders)
✅ Direct SQL table creation and queries via databricks-sql-connector
✅ Credentials stored securely in Vault
✅ Platform code supports Databricks destination

## What Doesn't Work

❌ DLT pipeline loading data to Unity Catalog
❌ Automatic schema detection in Unity Catalog

## Workarounds

### Option 1: Use Legacy Hive Metastore (if available)
If your Databricks workspace still has Hive Metastore enabled:
1. Update catalog in credentials to `hive_metastore`
2. Run pipeline as normal

### Option 2: Wait for DLT Library Update
The DLT library maintainers are aware of Unity Catalog compatibility issues. Monitor:
- https://github.com/dlt-hub/dlt/issues
- DLT release notes for Unity Catalog support

### Option 3: Use Alternative Destination
For immediate needs, use:
- BigQuery (tested and working ✅)
- SQL databases (MySQL, PostgreSQL - tested and working ✅)
- DuckDB (tested and working ✅)

## Testing Evidence

### Successful Components:
1. **Databricks Setup**: Account created, SQL Warehouse configured
2. **Authentication**: Access token working, connection verified
3. **Direct SQL Access**: Can create/query tables via databricks-sql-connector
4. **Data Extraction**: Airtable source successfully fetches all records
5. **Platform Integration**: Vault storage, UI configuration all working

### Failed Component:
- **DLT Loading**: Library-level incompatibility with Unity Catalog INFORMATION_SCHEMA

## Recommendation

For production use with Databricks:
1. Check if your workspace has legacy Hive Metastore access
2. If not, consider using BigQuery or other tested destinations
3. Monitor DLT updates for Unity Catalog support
4. Contact DLT maintainers if Unity Catalog support is critical

## Platform Status

The DLT Client platform correctly:
- ✅ Handles Databricks credentials
- ✅ Configures Databricks destination
- ✅ Passes credentials to DLT
- ✅ Supports all DLT destination parameters

The limitation is in the DLT library itself, not our platform code.

## Date Tested

February 15, 2026

## DLT Version

dlt 0.x (check requirements.txt for exact version)

## Databricks Configuration

- Workspace: dbc-497cc943-0972.cloud.databricks.com
- Catalog: main (Unity Catalog)
- SQL Warehouse: /sql/1.0/warehouses/0e8c07acee44dec1
