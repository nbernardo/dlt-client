# Airtable Integration Summary

## Overview
This PR adds Airtable integration to the DLT Client platform using the DLT-Code node pattern (Universal Connector approach).

## What's Included

### Core Integration
- **Airtable Template**: `ui/app/assets/dlt-code-template/source/airtable_tmpl`
  - Implements pagination and batch processing
  - Handles multiple tables from a single Airtable base
  - Supports all DLT destinations (MySQL, DuckDB, BigQuery, Databricks, etc.)

- **UI Integration**: `ui/app/assets/dlt-code-template/source/known-sources.js`
  - Adds Airtable to the list of available data sources in the DLT-Code node

### Cloud Destination Support
- **BigQuery**: Added imports and credential handling in `code_node_util.py`
- **Databricks**: Added imports and credential handling in `code_node_util.py`
- **Note**: Databricks has a known DLT library limitation with Unity Catalog (see `backend/tests/DATABRICKS_UNITY_CATALOG_LIMITATION.md`)

### Bug Fixes
- **Windows Compatibility**: Changed pipe characters (`|`) to double underscores (`__`) in pipeline filenames
  - `|withmetadata|` → `__withmetadata__`
  - `|toschedule|` → `__toschedule__`
  - Files affected: `DltPipeline.py`, `Workspace.py`
  
- **Secret Manager**: Fixed `get_from_references()` to filter out empty strings from references list

## Files Changed

### New Files (2)
- `ui/app/assets/dlt-code-template/source/airtable_tmpl` - Airtable template for DLT-Code node
- `backend/tests/DATABRICKS_UNITY_CATALOG_LIMITATION.md` - Documentation of Databricks limitation

### Modified Files (5)
- `ui/app/assets/dlt-code-template/source/known-sources.js` - Added Airtable to source list
- `backend/src/utils/code_node_util.py` - Added BigQuery and Databricks imports
- `backend/src/services/pipeline/DltPipeline.py` - Fixed Windows filename compatibility
- `backend/src/services/workspace/Workspace.py` - Fixed Windows filename compatibility
- `backend/src/services/workspace/SecretManager.py` - Fixed empty string handling

## Testing Completed

All integration scenarios tested successfully:
- ✅ Airtable → MySQL (11 customers, 16 products, 13 orders)
- ✅ Airtable → DuckDB (data loaded successfully)
- ✅ Airtable → BigQuery (client confirmed data received)
- ⚠️ Airtable → Databricks (connection works, DLT library has Unity Catalog limitation)

## Implementation Approach

This integration follows the **DLT-Code node pattern** (like Kafka), which means:
- Minimal code changes to core platform
- No regression testing required for existing functionality
- Template-based approach using `_tmpl` files
- No NodeMapper needed (unlike traditional node types)

## Windows Compatibility Note

Pipeline filenames now use double underscores (`__`) instead of pipe characters (`|`) for Windows compatibility:
- Metadata pipelines: `pipeline_name__withmetadata__.py`
- Scheduled pipelines: `pipeline_name__toschedule__.py`

**Important**: Double underscores (`__`) should not be used in user-defined pipeline names.

## Known Limitations

1. **Databricks Unity Catalog**: DLT library has compatibility issues with Unity Catalog's INFORMATION_SCHEMA structure. Direct SQL operations work, but DLT pipeline loading does not. This is a DLT library limitation, not a platform issue.

## Next Steps

- Add validation to prevent users from using double underscores (`__`) in pipeline names
- Update user documentation to reference the double underscore restriction
